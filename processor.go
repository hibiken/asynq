// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	"golang.org/x/time/rate"
)

type processor struct {
	rdb *rdb.RDB

	pinfo *base.ProcessInfo

	handler Handler

	queueConfig map[string]uint

	// orderedQueues is set only in strict-priority mode.
	orderedQueues []string

	retryDelayFunc retryDelayFunc

	// channel via which to send sync requests to syncer.
	syncRequestCh chan<- *syncRequest

	// rate limiter to prevent spamming logs with a bunch of errors.
	errLogLimiter *rate.Limiter

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

// newProcessor constructs a new processor.
func newProcessor(r *rdb.RDB, pinfo *base.ProcessInfo, fn retryDelayFunc, syncRequestCh chan<- *syncRequest) *processor {
	qcfg := normalizeQueueCfg(pinfo.Queues)
	orderedQueues := []string(nil)
	if pinfo.StrictPriority {
		orderedQueues = sortByPriority(qcfg)
	}
	return &processor{
		rdb:            r,
		pinfo:          pinfo,
		queueConfig:    qcfg,
		orderedQueues:  orderedQueues,
		retryDelayFunc: fn,
		syncRequestCh:  syncRequestCh,
		errLogLimiter:  rate.NewLimiter(rate.Every(3*time.Second), 1),
		sema:           make(chan struct{}, pinfo.Concurrency),
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
		logger.info("Processor shutting down...")
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
	logger.info("Waiting for all workers to finish...")
	// block until all workers have released the token
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
	logger.info("All workers have finished")
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
				logger.info("Processor done")
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
	qnames := p.queues()
	msg, err := p.rdb.Dequeue(qnames...)
	if err == rdb.ErrNoProcessableTask {
		// queues are empty, this is a normal behavior.
		if len(p.queueConfig) > 1 {
			// sleep to avoid slamming redis and let scheduler move tasks into queues.
			// Note: With multiple queues, we are not using blocking pop operation and
			// polling queues instead. This adds significant load to redis.
			time.Sleep(time.Second)
		}
		return
	}
	if err != nil {
		if p.errLogLimiter.Allow() {
			logger.error("Dequeue error: %v", err)
		}
		return
	}

	select {
	case <-p.abort:
		// shutdown is starting, return immediately after requeuing the message.
		p.requeue(msg)
		return
	case p.sema <- struct{}{}: // acquire token
		p.pinfo.IncrActiveWorkerCount(1)
		go func() {
			defer func() {
				<-p.sema /* release token */
				p.pinfo.IncrActiveWorkerCount(-1)
			}()

			resCh := make(chan error, 1)
			task := NewTask(msg.Type, msg.Payload)
			go func() {
				resCh <- perform(p.handler, task)
			}()

			select {
			case <-p.quit:
				// time is up, quit this worker goroutine.
				logger.warn("Quitting worker to process task id=%s", msg.ID)
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
		logger.error("Could not restore unfinished tasks: %v", err)
	}
	if n > 0 {
		logger.info("Restored %d unfinished tasks back to queue", n)
	}
}

func (p *processor) requeue(msg *base.TaskMessage) {
	err := p.rdb.Requeue(msg)
	if err != nil {
		logger.error("Could not push task id=%s back to queue: %v", msg.ID, err)
	}
}

func (p *processor) markAsDone(msg *base.TaskMessage) {
	err := p.rdb.Done(msg)
	if err != nil {
		errMsg := fmt.Sprintf("Could not remove task id=%s from %q", msg.ID, base.InProgressQueue)
		logger.warn("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.rdb.Done(msg)
			},
			errMsg: errMsg,
		}
	}
}

func (p *processor) retry(msg *base.TaskMessage, e error) {
	d := p.retryDelayFunc(msg.Retried, e, NewTask(msg.Type, msg.Payload))
	retryAt := time.Now().Add(d)
	err := p.rdb.Retry(msg, retryAt, e.Error())
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q", msg.ID, base.InProgressQueue, base.RetryQueue)
		logger.warn("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.rdb.Retry(msg, retryAt, e.Error())
			},
			errMsg: errMsg,
		}
	}
}

func (p *processor) kill(msg *base.TaskMessage, e error) {
	logger.warn("Retry exhausted for task id=%s", msg.ID)
	err := p.rdb.Kill(msg, e.Error())
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q", msg.ID, base.InProgressQueue, base.DeadQueue)
		logger.warn("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.rdb.Kill(msg, e.Error())
			},
			errMsg: errMsg,
		}
	}
}

// queues returns a list of queues to query.
// Order of the queue names is based on the priority of each queue.
// Queue names is sorted by their priority level if strict-priority is true.
// If strict-priority is false, then the order of queue names are roughly based on
// the priority level but randomized in order to avoid starving low priority queues.
func (p *processor) queues() []string {
	// skip the overhead of generating a list of queue names
	// if we are processing one queue.
	if len(p.queueConfig) == 1 {
		for qname := range p.queueConfig {
			return []string{qname}
		}
	}
	if p.orderedQueues != nil {
		return p.orderedQueues
	}
	var names []string
	for qname, priority := range p.queueConfig {
		for i := 0; i < int(priority); i++ {
			names = append(names, qname)
		}
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	return uniq(names, len(p.queueConfig))
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

// uniq dedupes elements and returns a slice of unique names of length l.
// Order of the output slice is based on the input list.
func uniq(names []string, l int) []string {
	var res []string
	seen := make(map[string]struct{})
	for _, s := range names {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			res = append(res, s)
		}
		if len(res) == l {
			break
		}
	}
	return res
}

// sortByPriority returns a list of queue names sorted by
// their priority level in descending order.
func sortByPriority(qcfg map[string]uint) []string {
	var queues []*queue
	for qname, n := range qcfg {
		queues = append(queues, &queue{qname, n})
	}
	sort.Sort(sort.Reverse(byPriority(queues)))
	var res []string
	for _, q := range queues {
		res = append(res, q.name)
	}
	return res
}

type queue struct {
	name     string
	priority uint
}

type byPriority []*queue

func (x byPriority) Len() int           { return len(x) }
func (x byPriority) Less(i, j int) bool { return x[i].priority < x[j].priority }
func (x byPriority) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

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
