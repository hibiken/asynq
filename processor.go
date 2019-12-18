package asynq

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/rdb"
)

type processor struct {
	rdb *rdb.RDB

	handler Handler

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

	// shutdown channel is closed when the shutdown of the "processor" goroutine starts.
	shutdown chan struct{}

	// quit channel communicates to the in-flight worker goroutines to stop.
	quit chan struct{}
}

func newProcessor(r *rdb.RDB, numWorkers int, handler Handler) *processor {
	return &processor{
		rdb:            r,
		handler:        handler,
		dequeueTimeout: 2 * time.Second,
		sema:           make(chan struct{}, numWorkers),
		done:           make(chan struct{}),
		shutdown:       make(chan struct{}),
		quit:           make(chan struct{}),
	}
}

// Note: stops only the "processor" goroutine, does not stop workers.
// It's safe to call this method multiple times.
func (p *processor) stop() {
	p.once.Do(func() {
		log.Println("[INFO] Processor shutting down...")
		// Unblock if processor is waiting for sema token.
		close(p.shutdown)
		// Signal the processor goroutine to stop processing tasks
		// from the queue.
		p.done <- struct{}{}
	})
}

// NOTE: once terminated, processor cannot be re-started.
func (p *processor) terminate() {
	p.stop()

	// TODO(hibiken): Allow user to customize this timeout value.
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
	case <-p.shutdown:
		// shutdown is starting, return immediately after requeuing the message.
		p.requeue(msg)
		return
	case p.sema <- struct{}{}: // acquire token
		go func() {
			defer func() { <-p.sema /* release token */ }()

			resCh := make(chan error, 1)
			task := &Task{Type: msg.Type, Payload: msg.Payload}
			go func() {
				resCh <- perform(p.handler, task)
			}()

			select {
			case <-p.quit:
				// time is up, quit this worker goroutine.
				return
			case resErr := <-resCh:
				// Note: One of three things should happen.
				// 1) Done  -> Removes the message from InProgress
				// 2) Retry -> Removes the message from InProgress & Adds the message to Retry
				// 3) Kill  -> Removes the message from InProgress & Adds the message to Dead
				if resErr != nil {
					if msg.Retried >= msg.Retry {
						p.kill(msg, resErr.Error())
						return
					}
					p.retry(msg, resErr.Error())
					return
				}
				p.markAsDone(msg)
				return
			}
		}()
	}
}

// restore moves all tasks from "in-progress" back to queue
// to restore all unfinished tasks.
func (p *processor) restore() {
	err := p.rdb.RestoreUnfinished()
	if err != nil {
		log.Printf("[ERROR] Could not restore unfinished tasks: %v\n", err)
	}
}

func (p *processor) requeue(msg *rdb.TaskMessage) {
	err := p.rdb.Requeue(msg)
	if err != nil {
		log.Printf("[ERROR] Could not move task from InProgress back to queue: %v\n", err)
	}
}

func (p *processor) markAsDone(msg *rdb.TaskMessage) {
	err := p.rdb.Done(msg)
	if err != nil {
		log.Printf("[ERROR] Could not remove task from InProgress queue: %v\n", err)
	}
}

func (p *processor) retry(msg *rdb.TaskMessage, errMsg string) {
	retryAt := time.Now().Add(delaySeconds(msg.Retried))
	err := p.rdb.Retry(msg, retryAt, errMsg)
	if err != nil {
		log.Printf("[ERROR] Could not send task %+v to Retry queue: %v\n", msg, err)
	}
}

func (p *processor) kill(msg *rdb.TaskMessage, errMsg string) {
	log.Printf("[WARN] Retry exhausted for task(Type: %q, ID: %v)\n", msg.Type, msg.ID)
	err := p.rdb.Kill(msg, errMsg)
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

// delaySeconds returns a number seconds to delay before retrying.
// Formula taken from https://github.com/mperham/sidekiq.
func delaySeconds(count int) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	s := int(math.Pow(float64(count), 4)) + 15 + (r.Intn(30) * (count + 1))
	return time.Duration(s) * time.Second
}
