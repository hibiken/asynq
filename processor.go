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
	fmt.Print("Processor shutting down...")
	// Signal the processor goroutine to stop processing tasks from the queue.
	p.done <- struct{}{}

	fmt.Print("Waiting for all workers to finish...")
	for i := 0; i < cap(p.sema); i++ {
		// block until all workers have released the token
		p.sema <- struct{}{}
	}
	fmt.Println("Done")
}

func (p *processor) start() {
	// NOTE: The call to "restore" needs to complete before starting
	// the processor goroutine.
	p.restore()
	go func() {
		for {
			select {
			case <-p.done:
				fmt.Println("Done")
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
	if err != nil {
		switch err {
		case errQueuePopTimeout:
			// timed out, this is a normal behavior.
			return
		case errDeserializeTask:
			log.Println("[Error] could not parse json encoded message")
			return
		default:
			log.Printf("[Error] unexpected error while pulling message out of queues: %v\n", err)
			return
		}
	}

	task := &Task{Type: msg.Type, Payload: msg.Payload}
	p.sema <- struct{}{} // acquire token
	go func(task *Task) {
		defer func() {
			if err := p.rdb.lrem(inProgress, msg); err != nil {
				log.Printf("[ERROR] could not remove %+v from %q: %v\n", msg, inProgress, err)
			}
			<-p.sema // release token
		}()
		err := p.handler(task) // TODO(hibiken): maybe also handle panic?
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
