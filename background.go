// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

// Background is responsible for managing the background-task processing.
//
// Background manages background queues to process tasks and retry if
// necessary. If the processing of a task is unsuccessful, background will
// schedule it for a retry with an exponential backoff until either the task
// gets processed successfully or it exhausts its max retry count.
//
// Once a task exhausts its retries, it will be moved to the "dead" queue and
// will be kept in the queue for some time until a certain condition is met
// (e.g., queue size reaches a certain limit, or the task has been in the
// queue for a certain amount of time).
type Background struct {
	mu      sync.Mutex
	running bool

	rdb       *rdb.RDB
	scheduler *scheduler
	processor *processor
}

// Config specifies the background-task processing behavior.
type Config struct {
	// Maximum number of concurrent workers to process tasks.
	//
	// If set to zero or negative value, NewBackground will overwrite the value to one.
	Concurrency int

	// Function to calculate retry delay for a failed task.
	//
	// By default, it uses exponential backoff algorithm to calculate the delay.
	//
	// n is the number of times the task has been retried.
	// e is the error returned by the task handler.
	// t is the task in question.
	RetryDelayFunc func(n int, e error, t *Task) time.Duration

	// List of queues to process with given priority level. Keys are the names of the
	// queues and values are associated priority level.
	//
	// If set to nil or not specified, the background will process only the "default" queue.
	//
	// Priority is treated as follows to avoid starving low priority queues.
	//
	// Example:
	// Queues: map[string]uint{
	//     "critical": 6,
	//     "default":  3,
	//     "low":      1,
	// }
	// With the above config and given that all queues are not empty, the tasks
	// in "critical", "default", "low" should be processed 60%, 30%, 10% of
	// the time respectively.
	Queues map[string]uint

	// StrictPriority indicates whether the queue priority should be treated strictly.
	//
	// If set to true, tasks in the queue with the highest priority is processed first.
	// The tasks in lower priority queues are processed only when those queues with
	// higher priorities are empty.
	StrictPriority bool
}

// Formula taken from https://github.com/mperham/sidekiq.
func defaultDelayFunc(n int, e error, t *Task) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	s := int(math.Pow(float64(n), 4)) + 15 + (r.Intn(30) * (n + 1))
	return time.Duration(s) * time.Second
}

var defaultQueueConfig = map[string]uint{
	base.DefaultQueueName: 1,
}

// NewBackground returns a new Background instance given a redis client
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
	queues := cfg.Queues
	if queues == nil || len(queues) == 0 {
		queues = defaultQueueConfig
	}
	qcfg := normalizeQueueCfg(queues)

	rdb := rdb.NewRDB(createRedisClient(r))
	scheduler := newScheduler(rdb, 5*time.Second, qcfg)
	processor := newProcessor(rdb, n, qcfg, cfg.StrictPriority, delayFunc)
	return &Background{
		rdb:       rdb,
		scheduler: scheduler,
		processor: processor,
	}
}

// A Handler processes a task.
//
// ProcessTask should return nil if the processing of a task
// is successful.
//
// If ProcessTask return a non-nil error or panics, the task
// will be retried after delay.
type Handler interface {
	ProcessTask(*Task) error
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(*Task) error

// ProcessTask calls fn(task)
func (fn HandlerFunc) ProcessTask(task *Task) error {
	return fn(task)
}

// Run starts the background-task processing and blocks until
// an os signal to exit the program is received. Once it receives
// a signal, it gracefully shuts down all pending workers and other
// goroutines to process the tasks.
func (bg *Background) Run(handler Handler) {
	bg.start(handler)
	defer bg.stop()

	// Wait for a signal to terminate.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGTSTP)
	for {
		sig := <-sigs
		if sig == syscall.SIGTSTP {
			bg.processor.stop()
			continue
		}
		break
	}
	fmt.Println()
	log.Println("[INFO] Starting graceful shutdown...")
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

	bg.scheduler.start()
	bg.processor.start()
}

// stops the background-task processing.
func (bg *Background) stop() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if !bg.running {
		return
	}

	bg.scheduler.terminate()
	bg.processor.terminate()

	bg.rdb.Close()
	bg.processor.handler = nil
	bg.running = false
}

// normalizeQueueCfg divides priority numbers by their
// greatest common divisor.
func normalizeQueueCfg(queueCfg map[string]uint) map[string]uint {
	var xs []uint
	for _, x := range queueCfg {
		xs = append(xs, x)
	}
	d := gcd(xs...)
	res := make(map[string]uint)
	for q, x := range queueCfg {
		res[q] = x / d
	}
	return res
}

func gcd(xs ...uint) uint {
	fn := func(x, y uint) uint {
		for y > 0 {
			x, y = y, x%y
		}
		return x
	}
	res := xs[0]
	for i := 0; i < len(xs); i++ {
		res = fn(xs[i], res)
		if res == 1 {
			return 1
		}
	}
	return res
}
