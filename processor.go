package asynq

import (
	"fmt"
	"log"
	"time"
)

type processor struct {
	rdb *rdb

	handler TaskHandler

	// sema is a counting semaphore to ensure the number of active workers
	// does not exceed the limit
	sema chan struct{}

	// channel to communicate back to the long running "processor" goroutine.
	done chan struct{}
}

func newProcessor(rdb *rdb, numWorkers int, handler TaskHandler) *processor {
	return &processor{
		rdb:     rdb,
		handler: handler,
		sema:    make(chan struct{}, numWorkers),
		done:    make(chan struct{}),
	}
}

func (p *processor) terminate() {
	log.Println("[INFO] Processor shutting down...")
	// Signal the processor goroutine to stop processing tasks from the queue.
	p.done <- struct{}{}

	log.Println("[INFO] Waiting for all workers to finish...")
	// block until all workers have released the token
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
	log.Println("[INFO] All workers have finished.")
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
	// NOTE: dequeue needs to timeout to avoid blocking forever
	// in case of a program shutdown or additon of a new queue.
	const timeout = 5 * time.Second
	msg, err := p.rdb.dequeue(defaultQueue, timeout)
	if err == errDequeueTimeout {
		// timed out, this is a normal behavior.
		return
	}
	if err != nil {
		log.Printf("[ERROR] unexpected error while pulling a task out of queue: %v\n", err)
		return
	}

	task := &Task{Type: msg.Type, Payload: msg.Payload}
	p.sema <- struct{}{} // acquire token
	go func(task *Task) {
		// NOTE: This deferred anonymous function needs to take taskMessage as a value because
		// the message can be mutated by the time this function is called.
		defer func(msg taskMessage) {
			if err := p.rdb.remove(inProgress, &msg); err != nil {
				log.Printf("[ERROR] could not remove %+v from %q: %v\n", msg, inProgress, err)
			}
			<-p.sema // release token
		}(*msg)
		err := perform(p.handler, task)
		if err != nil {
			retryTask(p.rdb, msg, err)
		}
	}(task)
}

// restore moves all tasks from "in-progress" back to queue
// to restore all unfinished tasks.
func (p *processor) restore() {
	err := p.rdb.moveAll(inProgress, defaultQueue)
	if err != nil {
		log.Printf("[ERROR] could not move tasks from %q to %q\n", inProgress, defaultQueue)
	}
}

// perform calls the handler with the given task.
// If the call returns without panic, it simply returns the value,
// otherwise, it recovers from panic and returns an error.
func perform(handler TaskHandler, task *Task) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("panic: %v", x)
		}
	}()
	return handler(task)
}
