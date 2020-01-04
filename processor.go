// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

type processor struct {
	rdb *rdb.RDB

	handler Handler

	retryDelayFunc retryDelayFunc

	// timeout for blocking dequeue operation.
	// dequeue needs to timeout to avoid blocking forever
	// in case of a program shutdown or additon of a new queue.
	dequeueTimeout time.Duration

	// sema is a counting semaphore to ensure the number of active workers
	// does not exceed the limit.
	sema chan struct{}

	// channel to communicate back to the long running "processor" goroutine.
	// once is used to send value to the channel only once.
	done chan struct{}
	once sync.Once

	// abort channel is closed when the shutdown of the "processor" goroutine starts.
	abort chan struct{}

	// quit channel communicates to the in-flight worker goroutines to stop.
	quit chan struct{}
}

type retryDelayFunc func(n int, err error, task *Task) time.Duration

func newProcessor(r *rdb.RDB, n int, fn retryDelayFunc) *processor {
	return &processor{
		rdb:            r,
		retryDelayFunc: fn,
		dequeueTimeout: 2 * time.Second,
		sema:           make(chan struct{}, n),
		done:           make(chan struct{}),
		abort:          make(chan struct{}),
		quit:           make(chan struct{}),
		handler:        HandlerFunc(func(t *Task) error { return fmt.Errorf("handler not set") }),
	}
}

// Note: stops only the "processor" goroutine, does not stop workers.
// It's safe to call this method multiple times.
func (p *processor) stop() {
	p.once.Do(func() {
		log.Println("[INFO] Processor shutting down...")
		// Unblock if processor is waiting for sema token.
		close(p.abort)
		// Signal the processor goroutine to stop processing tasks
		// from the queue.
		p.done <- struct{}{}
	})
}

// NOTE: once terminated, processor cannot be re-started.
func (p *processor) terminate() {
	p.stop()

	// IDEA: Allow user to customize this timeout value.
	const timeout = 8 * time.Second
	time.AfterFunc(timeout, func() { close(p.quit) })
	log.Println("[INFO] Waiting for all workers to finish...")
	// block until all workers have released the token
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
	log.Println("[INFO] All workers have finished.")
	p.restore() // move any unfinished tasks back to the queue.
}

func (p *processor) start() {
	// NOTE: The call to "restore" needs to complete before starting
	// the processor goroutine.
	p.restore()
	go func() {
		for {
			select {
			case <-p.done:
				log.Println("[INFO] Processor done.")
				return
			default:
				p.exec()
			}
		}
	}()
}

// exec pulls a task out of the queue and starts a worker goroutine to
// process the task.
func (p *processor) exec() {
	msg, err := p.rdb.Dequeue(p.dequeueTimeout)
	if err == rdb.ErrDequeueTimeout {
		// timed out, this is a normal behavior.
		return
	}
	if err != nil {
		log.Printf("[ERROR] unexpected error while pulling a task out of queue: %v\n", err)
		return
	}

	select {
	case <-p.abort:
		// shutdown is starting, return immediately after requeuing the message.
		p.requeue(msg)
		return
	case p.sema <- struct{}{}: // acquire token
		go func() {
			defer func() { <-p.sema /* release token */ }()

			resCh := make(chan error, 1)
			task := NewTask(msg.Type, msg.Payload)
			go func() {
				resCh <- perform(p.handler, task)
			}()

			select {
			case <-p.quit:
				// time is up, quit this worker goroutine.
				log.Printf("[WARN] Terminating in-progress task %+v\n", msg)
				return
			case resErr := <-resCh:
				// Note: One of three things should happen.
				// 1) Done  -> Removes the message from InProgress
				// 2) Retry -> Removes the message from InProgress & Adds the message to Retry
				// 3) Kill  -> Removes the message from InProgress & Adds the message to Dead
				if resErr != nil {
					if msg.Retried >= msg.Retry {
						p.kill(msg, resErr)
					} else {
						p.retry(msg, resErr)
					}
					return
				}
				p.markAsDone(msg)
			}
		}()
	}
}

// restore moves all tasks from "in-progress" back to queue
// to restore all unfinished tasks.
func (p *processor) restore() {
	n, err := p.rdb.RestoreUnfinished()
	if err != nil {
		log.Printf("[ERROR] Could not restore unfinished tasks: %v\n", err)
	}
	if n > 0 {
		log.Printf("[INFO] Restored %d unfinished tasks back to queue.\n", n)
	}
}

func (p *processor) requeue(msg *base.TaskMessage) {
	err := p.rdb.Requeue(msg)
	if err != nil {
		log.Printf("[ERROR] Could not move task from InProgress back to queue: %v\n", err)
	}
}

func (p *processor) markAsDone(msg *base.TaskMessage) {
	err := p.rdb.Done(msg)
	if err != nil {
		log.Printf("[ERROR] Could not remove task from InProgress queue: %v\n", err)
	}
}

func (p *processor) retry(msg *base.TaskMessage, e error) {
	d := p.retryDelayFunc(msg.Retried, e, NewTask(msg.Type, msg.Payload))
	retryAt := time.Now().Add(d)
	err := p.rdb.Retry(msg, retryAt, e.Error())
	if err != nil {
		log.Printf("[ERROR] Could not send task %+v to Retry queue: %v\n", msg, err)
	}
}

func (p *processor) kill(msg *base.TaskMessage, e error) {
	log.Printf("[WARN] Retry exhausted for task(Type: %q, ID: %v)\n", msg.Type, msg.ID)
	err := p.rdb.Kill(msg, e.Error())
	if err != nil {
		log.Printf("[ERROR] Could not send task %+v to Dead queue: %v\n", msg, err)
	}
}

// perform calls the handler with the given task.
// If the call returns without panic, it simply returns the value,
// otherwise, it recovers from panic and returns an error.
func perform(h Handler, task *Task) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("panic: %v", x)
		}
	}()
	return h.ProcessTask(task)
}
