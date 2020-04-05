// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
)

// Background is responsible for managing the background-task processing.
//
// Background manages task queues to process tasks.
// If the processing of a task is unsuccessful, background will
// schedule it for a retry until either the task gets processed successfully
// or it exhausts its max retry count.
//
// Once a task exhausts its retries, it will be moved to the "dead" queue and
// will be kept in the queue for some time until a certain condition is met
// (e.g., queue size reaches a certain limit, or the task has been in the
// queue for a certain amount of time).
type Background struct {
	mu      sync.Mutex
	running bool

	ps *base.ProcessState

	// wait group to wait for all goroutines to finish.
	wg sync.WaitGroup

	logger Logger

	rdb         *rdb.RDB
	scheduler   *scheduler
	processor   *processor
	syncer      *syncer
	heartbeater *heartbeater
	subscriber  *subscriber
}

// Config specifies the background-task processing behavior.
type Config struct {
	// Maximum number of concurrent processing of tasks.
	//
	// If set to a zero or negative value, NewBackground will overwrite the value to one.
	Concurrency int

	// Function to calculate retry delay for a failed task.
	//
	// By default, it uses exponential backoff algorithm to calculate the delay.
	//
	// n is the number of times the task has been retried.
	// e is the error returned by the task handler.
	// t is the task in question.
	RetryDelayFunc func(n int, e error, t *Task) time.Duration

	// List of queues to process with given priority value. Keys are the names of the
	// queues and values are associated priority value.
	//
	// If set to nil or not specified, the background will process only the "default" queue.
	//
	// Priority is treated as follows to avoid starving low priority queues.
	//
	// Example:
	// Queues: map[string]int{
	//     "critical": 6,
	//     "default":  3,
	//     "low":      1,
	// }
	// With the above config and given that all queues are not empty, the tasks
	// in "critical", "default", "low" should be processed 60%, 30%, 10% of
	// the time respectively.
	//
	// If a queue has a zero or negative priority value, the queue will be ignored.
	Queues map[string]int

	// StrictPriority indicates whether the queue priority should be treated strictly.
	//
	// If set to true, tasks in the queue with the highest priority is processed first.
	// The tasks in lower priority queues are processed only when those queues with
	// higher priorities are empty.
	StrictPriority bool

	// ErrorHandler handles errors returned by the task handler.
	//
	// HandleError is invoked only if the task handler returns a non-nil error.
	//
	// Example:
	// func reportError(task *asynq.Task, err error, retried, maxRetry int) {
	// 	   if retried >= maxRetry {
	//         err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	// 	   }
	//     errorReportingService.Notify(err)
	// })
	//
	// ErrorHandler: asynq.ErrorHandlerFunc(reportError)
	ErrorHandler ErrorHandler

	// Logger specifies the logger used by the background instance.
	//
	// If unset, default logger is used.
	Logger Logger
}

// An ErrorHandler handles errors returned by the task handler.
type ErrorHandler interface {
	HandleError(task *Task, err error, retried, maxRetry int)
}

// The ErrorHandlerFunc type is an adapter to allow the use of  ordinary functions as a ErrorHandler.
// If f is a function with the appropriate signature, ErrorHandlerFunc(f) is a ErrorHandler that calls f.
type ErrorHandlerFunc func(task *Task, err error, retried, maxRetry int)

// HandleError calls fn(task, err, retried, maxRetry)
func (fn ErrorHandlerFunc) HandleError(task *Task, err error, retried, maxRetry int) {
	fn(task, err, retried, maxRetry)
}

// Logger implements logging with various log levels.
type Logger interface {
	// Debug logs a message at Debug level.
	Debug(format string, args ...interface{})

	// Info logs a message at Info level.
	Info(format string, args ...interface{})

	// Warn logs a message at Warning level.
	Warn(format string, args ...interface{})

	// Error logs a message at Error level.
	Error(format string, args ...interface{})

	// Fatal logs a message at Fatal level
	// and process will exit with status set to 1.
	Fatal(format string, args ...interface{})
}

// Formula taken from https://github.com/mperham/sidekiq.
func defaultDelayFunc(n int, e error, t *Task) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	s := int(math.Pow(float64(n), 4)) + 15 + (r.Intn(30) * (n + 1))
	return time.Duration(s) * time.Second
}

var defaultQueueConfig = map[string]int{
	base.DefaultQueueName: 1,
}

// NewBackground returns a new Background given a redis connection option
// and background processing configuration.
func NewBackground(r RedisConnOpt, cfg *Config) *Background {
	n := cfg.Concurrency
	if n < 1 {
		n = 1
	}
	delayFunc := cfg.RetryDelayFunc
	if delayFunc == nil {
		delayFunc = defaultDelayFunc
	}
	queues := make(map[string]int)
	for qname, p := range cfg.Queues {
		if p > 0 {
			queues[qname] = p
		}
	}
	if len(queues) == 0 {
		queues = defaultQueueConfig
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.NewLogger(os.Stderr)
	}

	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	pid := os.Getpid()

	rdb := rdb.NewRDB(createRedisClient(r))
	ps := base.NewProcessState(host, pid, n, queues, cfg.StrictPriority)
	syncCh := make(chan *syncRequest)
	cancels := base.NewCancelations()
	syncer := newSyncer(logger, syncCh, 5*time.Second)
	heartbeater := newHeartbeater(logger, rdb, ps, 5*time.Second)
	scheduler := newScheduler(logger, rdb, 5*time.Second, queues)
	processor := newProcessor(logger, rdb, ps, delayFunc, syncCh, cancels, cfg.ErrorHandler)
	subscriber := newSubscriber(logger, rdb, cancels)
	return &Background{
		logger:      logger,
		rdb:         rdb,
		ps:          ps,
		scheduler:   scheduler,
		processor:   processor,
		syncer:      syncer,
		heartbeater: heartbeater,
		subscriber:  subscriber,
	}
}

// A Handler processes tasks.
//
// ProcessTask should return nil if the processing of a task
// is successful.
//
// If ProcessTask return a non-nil error or panics, the task
// will be retried after delay.
type Handler interface {
	ProcessTask(context.Context, *Task) error
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(context.Context, *Task) error

// ProcessTask calls fn(ctx, task)
func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
	return fn(ctx, task)
}

// Run starts the background-task processing and blocks until
// an os signal to exit the program is received. Once it receives
// a signal, it gracefully shuts down all pending workers and other
// goroutines to process the tasks.
func (bg *Background) Run(handler Handler) {
	type prefixLogger interface {
		SetPrefix(prefix string)
	}
	// If logger supports setting prefix, then set prefix for log output.
	if l, ok := bg.logger.(prefixLogger); ok {
		l.SetPrefix(fmt.Sprintf("asynq: pid=%d ", os.Getpid()))
	}
	bg.logger.Info("Starting processing")

	bg.start(handler)
	defer bg.stop()

	bg.logger.Info("Send signal TSTP to stop processing new tasks")
	bg.logger.Info("Send signal TERM or INT to terminate the process")

	bg.waitForSignals()
	fmt.Println()
	bg.logger.Info("Starting graceful shutdown")
}

// starts the background-task processing.
func (bg *Background) start(handler Handler) {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if bg.running {
		return
	}

	bg.running = true
	bg.processor.handler = handler

	bg.heartbeater.start(&bg.wg)
	bg.subscriber.start(&bg.wg)
	bg.syncer.start(&bg.wg)
	bg.scheduler.start(&bg.wg)
	bg.processor.start(&bg.wg)
}

// stops the background-task processing.
func (bg *Background) stop() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if !bg.running {
		return
	}

	// Note: The order of termination is important.
	// Sender goroutines should be terminated before the receiver goroutines.
	//
	// processor -> syncer (via syncCh)
	bg.scheduler.terminate()
	bg.processor.terminate()
	bg.syncer.terminate()
	bg.subscriber.terminate()
	bg.heartbeater.terminate()

	bg.wg.Wait()

	bg.rdb.Close()
	bg.running = false

	bg.logger.Info("Bye!")
}
