// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
)

// Server is responsible for managing the background-task processing.
//
// Server pulls tasks off queues and processes them.
// If the processing of a task is unsuccessful, server will
// schedule it for a retry.
// A task will be retried until either the task gets processed successfully
// or until it reaches its max retry count.
//
// If a task exhausts its retries, it will be moved to the "dead" queue and
// will be kept in the queue for some time until a certain condition is met
// (e.g., queue size reaches a certain limit, or the task has been in the
// queue for a certain amount of time).
type Server struct {
	ss *base.ServerState

	logger *log.Logger

	broker base.Broker

	// wait group to wait for all goroutines to finish.
	wg          sync.WaitGroup
	scheduler   *scheduler
	processor   *processor
	syncer      *syncer
	heartbeater *heartbeater
	subscriber  *subscriber
}

// Config specifies the server's background-task processing behavior.
type Config struct {
	// Maximum number of concurrent processing of tasks.
	//
	// If set to a zero or negative value, NewServer will overwrite the value
	// to the number of CPUs usable by the currennt process.
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
	// If set to nil or not specified, the server will process only the "default" queue.
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

	// Logger specifies the logger used by the server instance.
	//
	// If unset, default logger is used.
	Logger Logger

	// ShutdownTimeout specifies the duration to wait to let workers finish their tasks
	// before forcing them to abort when stopping the server.
	//
	// If unset or zero, default timeout of 8 seconds is used.
	ShutdownTimeout time.Duration
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

// Logger supports logging with various log levels.
type Logger interface {
	// Debug logs a message at Debug level.
	Debug(args ...interface{})

	// Info logs a message at Info level.
	Info(args ...interface{})

	// Warn logs a message at Warning level.
	Warn(args ...interface{})

	// Error logs a message at Error level.
	Error(args ...interface{})

	// Fatal logs a message at Fatal level
	// and process will exit with status set to 1.
	Fatal(args ...interface{})
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

const defaultShutdownTimeout = 8 * time.Second

// NewServer returns a new Server given a redis connection option
// and background processing configuration.
func NewServer(r RedisConnOpt, cfg Config) *Server {
	n := cfg.Concurrency
	if n < 1 {
		n = runtime.NumCPU()
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
	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}

	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}
	pid := os.Getpid()

	rdb := rdb.NewRDB(createRedisClient(r))
	logger := log.NewLogger(cfg.Logger)
	ss := base.NewServerState(host, pid, n, queues, cfg.StrictPriority)
	syncCh := make(chan *syncRequest)
	cancels := base.NewCancelations()
	syncer := newSyncer(logger, syncCh, 5*time.Second)
	heartbeater := newHeartbeater(logger, rdb, ss, 5*time.Second)
	scheduler := newScheduler(logger, rdb, 5*time.Second, queues)
	subscriber := newSubscriber(logger, rdb, cancels)
	processor := newProcessor(newProcessorParams{
		logger:          logger,
		broker:          rdb,
		ss:              ss,
		retryDelayFunc:  delayFunc,
		syncCh:          syncCh,
		cancelations:    cancels,
		errHandler:      cfg.ErrorHandler,
		shutdownTimeout: shutdownTimeout,
	})
	return &Server{
		ss:          ss,
		logger:      logger,
		broker:      rdb,
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

// ErrServerStopped indicates that the operation is now illegal because of the server being stopped.
var ErrServerStopped = errors.New("asynq: the server has been stopped")

// Run starts the background-task processing and blocks until
// an os signal to exit the program is received. Once it receives
// a signal, it gracefully shuts down all active workers and other
// goroutines to process the tasks.
//
// Run returns any error encountered during server startup time.
// If the server has already been stopped, ErrServerStopped is returned.
func (srv *Server) Run(handler Handler) error {
	if err := srv.Start(handler); err != nil {
		return err
	}
	srv.waitForSignals()
	srv.Stop()
	return nil
}

// Start starts the worker server. Once the server has started,
// it pulls tasks off queues and starts a worker goroutine for each task.
// Tasks are processed concurrently by the workers  up to the number of
// concurrency specified at the initialization time.
//
// Start returns any error encountered during server startup time.
// If the server has already been stopped, ErrServerStopped is returned.
func (srv *Server) Start(handler Handler) error {
	if handler == nil {
		return fmt.Errorf("asynq: server cannot run with nil handler")
	}
	switch srv.ss.Status() {
	case base.StatusRunning:
		return fmt.Errorf("asynq: the server is already running")
	case base.StatusStopped:
		return ErrServerStopped
	}
	srv.ss.SetStatus(base.StatusRunning)
	srv.processor.handler = handler

	srv.logger.Info("Starting processing")

	srv.heartbeater.start(&srv.wg)
	srv.subscriber.start(&srv.wg)
	srv.syncer.start(&srv.wg)
	srv.scheduler.start(&srv.wg)
	srv.processor.start(&srv.wg)
	return nil
}

// Stop stops the worker server.
// It gracefully closes all active workers. The server will wait for
// active workers to finish processing tasks for duration specified in Config.ShutdownTimeout.
// If worker didn't finish processing a task during the timeout, the task will be pushed back to Redis.
func (srv *Server) Stop() {
	switch srv.ss.Status() {
	case base.StatusIdle, base.StatusStopped:
		// server is not running, do nothing and return.
		return
	}

	fmt.Println() // print newline for prettier log.
	srv.logger.Info("Starting graceful shutdown")
	// Note: The order of termination is important.
	// Sender goroutines should be terminated before the receiver goroutines.
	// processor -> syncer (via syncCh)
	srv.scheduler.terminate()
	srv.processor.terminate()
	srv.syncer.terminate()
	srv.subscriber.terminate()
	srv.heartbeater.terminate()

	srv.wg.Wait()

	srv.broker.Close()
	srv.ss.SetStatus(base.StatusStopped)

	srv.logger.Info("Bye!")
}

// Quiet signals the server to stop pulling new tasks off queues.
// Quiet should be used before stopping the server.
func (srv *Server) Quiet() {
	srv.processor.stop()
	srv.ss.SetStatus(base.StatusQuiet)
}
