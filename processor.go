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
	// Signal the processor goroutine to stop processing tasks from the queue.
	p.done <- struct{}{}

	fmt.Println("--- Waiting for all workers to finish ---")
	for i := 0; i < cap(p.sema); i++ {
		// block until all workers have released the token
		p.sema <- struct{}{}
	}
	fmt.Println("--- All workers have finished! ----")
}

func (p *processor) start() {
	go func() {
		for {
			select {
			case <-p.done:
				fmt.Println("-------------[Processor]---------------")
				fmt.Println("Processor shutting down...")
				fmt.Println("-------------------------------------")
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
	// NOTE: BLPOP needs to timeout to avoid blocking forever
	// in case of a program shutdown or additon of a new queue.
	const timeout = 5 * time.Second
	// TODO(hibiken): sort the list of queues in order of priority
	msg, err := p.rdb.dequeue(timeout, p.rdb.listQueues()...)
	if err != nil {
		switch err {
		case errQueuePopTimeout:
			// timed out, this is a normal behavior.
			return
		case errDeserializeTask:
			log.Println("[Servere Error] could not parse json encoded message")
			return
		default:
			log.Printf("[Servere Error] unexpected error while pulling message out of queues: %v\n", err)
			return
		}
	}

	t := &Task{Type: msg.Type, Payload: msg.Payload}
	p.sema <- struct{}{} // acquire token
	go func(task *Task) {
		defer func() {
			if err := p.rdb.srem(inProgress, msg); err != nil {
				log.Printf("[SERVER ERROR] SREM failed: %v\n", err)
			}
			<-p.sema // release token
		}()
		err := p.handler(task)
		if err != nil {
			retryTask(p.rdb, msg, err)
		}
	}(t)
}
