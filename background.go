package asynq

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"
)

// Background is a top-level entity for the background-task processing.
type Background struct {
	mu      sync.Mutex
	running bool

	rdb       *rdb
	poller    *poller
	processor *processor
}

// NewBackground returns a new Background instance.
func NewBackground(numWorkers int, opt *RedisOpt) *Background {
	rdb := newRDB(opt)
	poller := newPoller(rdb, 5*time.Second, []string{scheduled, retry})
	processor := newProcessor(rdb, numWorkers, nil)
	return &Background{
		rdb:       rdb,
		poller:    poller,
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

	// Wait for a signal to exit.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)
	<-sigs
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

	bg.poller.start()
	bg.processor.start()
}

// stops the background-task processing.
func (bg *Background) stop() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if !bg.running {
		return
	}

	bg.poller.terminate()
	bg.processor.terminate()

	bg.rdb.client.Close()
	bg.processor.handler = nil
	bg.running = false
}
