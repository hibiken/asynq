package asynq

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v7"
)

type manager struct {
	rdb *redis.Client

	handler TaskHandler

	// sema is a counting semaphore to ensure the number of active workers
	// does not exceed the limit
	sema chan struct{}

	done chan struct{}
}

func newManager(rdb *redis.Client, numWorkers int, handler TaskHandler) *manager {
	return &manager{
		rdb:     rdb,
		handler: handler,
		sema:    make(chan struct{}, numWorkers),
		done:    make(chan struct{}),
	}
}

func (m *manager) terminate() {
	m.done <- struct{}{}
}

func (m *manager) start() {
	go func() {
		for {
			select {
			case <-m.done:
				m.shutdown()
			default:
				m.processTasks()
			}
		}
	}()
}

func (m *manager) processTasks() {
	// pull message out of the queue and process it
	// TODO(hibiken): sort the list of queues in order of priority
	res, err := m.rdb.BLPop(5*time.Second, listQueues(m.rdb)...).Result() // NOTE: BLPOP needs to time out because if case a new queue is added.
	if err != nil {
		if err != redis.Nil {
			log.Printf("BLPOP command failed: %v\n", err)
		}
		return
	}

	q, data := res[0], res[1]
	fmt.Printf("perform task %v from %s\n", data, q)
	var msg taskMessage
	err = json.Unmarshal([]byte(data), &msg)
	if err != nil {
		log.Printf("[Servere Error] could not parse json encoded message %s: %v", data, err)
		return
	}
	t := &Task{Type: msg.Type, Payload: msg.Payload}
	m.sema <- struct{}{} // acquire token
	go func(task *Task) {
		defer func() { <-m.sema }() // release token
		err := m.handler(task)
		if err != nil {
			if msg.Retried >= msg.Retry {
				fmt.Println("Retry exhausted!!!")
				if err := kill(m.rdb, &msg); err != nil {
					log.Printf("[SERVER ERROR] could not add task %+v to 'dead' set\n", err)
				}
				return
			}
			fmt.Println("RETRY!!!")
			retryAt := time.Now().Add(delaySeconds((msg.Retried)))
			fmt.Printf("[DEBUG] retying the task in %v\n", retryAt.Sub(time.Now()))
			msg.Retried++
			msg.ErrorMsg = err.Error()
			if err := zadd(m.rdb, retry, float64(retryAt.Unix()), &msg); err != nil {
				// TODO(hibiken): Not sure how to handle this error
				log.Printf("[SEVERE ERROR] could not add msg %+v to 'retry' set: %v\n", msg, err)
				return
			}
		}
	}(t)
}

func (m *manager) shutdown() {
	// TODO(hibiken): implement this. Gracefully shutdown all active goroutines.
	fmt.Println("-------------[Manager]---------------")
	fmt.Println("Manager shutting down...")
	fmt.Println("------------------------------------")
}
