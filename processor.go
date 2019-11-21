package asynq

import (
	"fmt"
	"log"
	"math"
	"math/rand"
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
	// Signal he processor goroutine to stop processing tasks from the queue.
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

func (p *processor) exec() {
	// pull message out of the queue and process it
	// TODO(hibiken): sort the list of queues in order of priority
	// NOTE: BLPOP needs to timeout in case a new queue is added.
	msg, err := p.rdb.bpop(5*time.Second, p.rdb.listQueues()...)
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
		defer func() { <-p.sema }() // release token
		err := p.handler(task)
		if err != nil {
			if msg.Retried >= msg.Retry {
				fmt.Println("Retry exhausted!!!")
				if err := p.rdb.kill(msg); err != nil {
					log.Printf("[SERVER ERROR] could not add task %+v to 'dead' set\n", err)
				}
				return
			}
			fmt.Println("RETRY!!!")
			retryAt := time.Now().Add(delaySeconds((msg.Retried)))
			fmt.Printf("[DEBUG] retying the task in %v\n", retryAt.Sub(time.Now()))
			msg.Retried++
			msg.ErrorMsg = err.Error()
			if err := p.rdb.zadd(retry, float64(retryAt.Unix()), msg); err != nil {
				// TODO(hibiken): Not sure how to handle this error
				log.Printf("[SEVERE ERROR] could not add msg %+v to 'retry' set: %v\n", msg, err)
				return
			}
		}
	}(t)
}

// delaySeconds returns a number seconds to delay before retrying.
// Formula taken from https://github.com/mperham/sidekiq.
func delaySeconds(count int) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	s := int(math.Pow(float64(count), 4)) + 15 + (r.Intn(30) * (count + 1))
	return time.Duration(s) * time.Second
}
