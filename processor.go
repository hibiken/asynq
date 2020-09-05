// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
	"github.com/hibiken/asynq/internal/rdb"
	"golang.org/x/time/rate"
)

type processor struct {
	logger *log.Logger
	broker base.Broker

	handler Handler

	queueConfig map[string]int

	// orderedQueues is set only in strict-priority mode.
	orderedQueues []string

	retryDelayFunc retryDelayFunc

	errHandler ErrorHandler

	shutdownTimeout time.Duration

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

	// quit channel is closed when the shutdown of the "processor" goroutine starts.
	quit chan struct{}

	// abort channel communicates to the in-flight worker goroutines to stop.
	abort chan struct{}

	// cancelations is a set of cancel functions for all active tasks.
	cancelations *base.Cancelations

	starting chan<- *base.TaskMessage
	finished chan<- *base.TaskMessage
}

type retryDelayFunc func(n int, err error, task *Task) time.Duration

type processorParams struct {
	logger          *log.Logger
	broker          base.Broker
	retryDelayFunc  retryDelayFunc
	syncCh          chan<- *syncRequest
	cancelations    *base.Cancelations
	concurrency     int
	queues          map[string]int
	strictPriority  bool
	errHandler      ErrorHandler
	shutdownTimeout time.Duration
	starting        chan<- *base.TaskMessage
	finished        chan<- *base.TaskMessage
}

// newProcessor constructs a new processor.
func newProcessor(params processorParams) *processor {
	queues := normalizeQueues(params.queues)
	orderedQueues := []string(nil)
	if params.strictPriority {
		orderedQueues = sortByPriority(queues)
	}
	return &processor{
		logger:         params.logger,
		broker:         params.broker,
		queueConfig:    queues,
		orderedQueues:  orderedQueues,
		retryDelayFunc: params.retryDelayFunc,
		syncRequestCh:  params.syncCh,
		cancelations:   params.cancelations,
		errLogLimiter:  rate.NewLimiter(rate.Every(3*time.Second), 1),
		sema:           make(chan struct{}, params.concurrency),
		done:           make(chan struct{}),
		quit:           make(chan struct{}),
		abort:          make(chan struct{}),
		errHandler:     params.errHandler,
		handler:        HandlerFunc(func(ctx context.Context, t *Task) error { return fmt.Errorf("handler not set") }),
		starting:       params.starting,
		finished:       params.finished,
	}
}

// Note: stops only the "processor" goroutine, does not stop workers.
// It's safe to call this method multiple times.
func (p *processor) stop() {
	p.once.Do(func() {
		p.logger.Debug("Processor shutting down...")
		// Unblock if processor is waiting for sema token.
		close(p.quit)
		// Signal the processor goroutine to stop processing tasks
		// from the queue.
		p.done <- struct{}{}
	})
}

// NOTE: once terminated, processor cannot be re-started.
func (p *processor) terminate() {
	p.stop()

	time.AfterFunc(p.shutdownTimeout, func() { close(p.abort) })

	p.logger.Info("Waiting for all workers to finish...")
	// block until all workers have released the token
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
	p.logger.Info("All workers have finished")
}

func (p *processor) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-p.done:
				p.logger.Debug("Processor done")
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
	select {
	case <-p.quit:
		return
	case p.sema <- struct{}{}: // acquire token
		qnames := p.queues()
		msg, deadline, err := p.broker.Dequeue(qnames...)
		switch {
		case err == rdb.ErrNoProcessableTask:
			p.logger.Debug("All queues are empty")
			// Queues are empty, this is a normal behavior.
			// Sleep to avoid slamming redis and let scheduler move tasks into queues.
			// Note: We are not using blocking pop operation and polling queues instead.
			// This adds significant load to redis.
			time.Sleep(time.Second)
			<-p.sema // release token
			return
		case err != nil:
			if p.errLogLimiter.Allow() {
				p.logger.Errorf("Dequeue error: %v", err)
			}
			<-p.sema // release token
			return
		}

		p.starting <- msg
		go func() {
			defer func() {
				p.finished <- msg
				<-p.sema // release token
			}()

			ctx, cancel := createContext(msg, deadline)
			p.cancelations.Add(msg.ID.String(), cancel)
			defer func() {
				cancel()
				p.cancelations.Delete(msg.ID.String())
			}()

			// check context before starting a worker goroutine.
			select {
			case <-ctx.Done():
				// already canceled (e.g. deadline exceeded).
				p.retryOrKill(ctx, msg, ctx.Err())
				return
			default:
			}

			resCh := make(chan error, 1)
			go func() {
				resCh <- perform(ctx, NewTask(msg.Type, msg.Payload), p.handler)
			}()

			select {
			case <-p.abort:
				// time is up, push the message back to queue and quit this worker goroutine.
				p.logger.Warnf("Quitting worker. task id=%s", msg.ID)
				p.requeue(msg)
				return
			case <-ctx.Done():
				p.retryOrKill(ctx, msg, ctx.Err())
				return
			case resErr := <-resCh:
				// Note: One of three things should happen.
				// 1) Done  -> Removes the message from Active
				// 2) Retry -> Removes the message from Active & Adds the message to Retry
				// 3) Kill  -> Removes the message from Active & Adds the message to Dead
				if resErr != nil {
					p.retryOrKill(ctx, msg, resErr)
					return
				}
				p.markAsDone(ctx, msg)
			}
		}()
	}
}

func (p *processor) requeue(msg *base.TaskMessage) {
	err := p.broker.Requeue(msg)
	if err != nil {
		p.logger.Errorf("Could not push task id=%s back to queue: %v", msg.ID, err)
	} else {
		p.logger.Infof("Pushed task id=%s back to queue", msg.ID)
	}
}

func (p *processor) markAsDone(ctx context.Context, msg *base.TaskMessage) {
	err := p.broker.Done(msg)
	if err != nil {
		errMsg := fmt.Sprintf("Could not remove task id=%s type=%q from %q err: %+v", msg.ID, msg.Type, base.ActiveKey(msg.Queue), err)
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Done(msg)
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

func (p *processor) retryOrKill(ctx context.Context, msg *base.TaskMessage, err error) {
	if p.errHandler != nil {
		p.errHandler.HandleError(ctx, NewTask(msg.Type, msg.Payload), err)
	}
	if msg.Retried >= msg.Retry {
		p.logger.Warnf("Retry exhausted for task id=%s", msg.ID)
		p.kill(ctx, msg, err)
	} else {
		p.retry(ctx, msg, err)
	}
}

func (p *processor) retry(ctx context.Context, msg *base.TaskMessage, e error) {
	d := p.retryDelayFunc(msg.Retried, e, NewTask(msg.Type, msg.Payload))
	retryAt := time.Now().Add(d)
	err := p.broker.Retry(msg, retryAt, e.Error())
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q", msg.ID, base.ActiveKey(msg.Queue), base.RetryKey(msg.Queue))
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Retry(msg, retryAt, e.Error())
			},
			errMsg:   errMsg,
			deadline: deadline,
		}
	}
}

func (p *processor) kill(ctx context.Context, msg *base.TaskMessage, e error) {
	err := p.broker.Kill(msg, e.Error())
	if err != nil {
		errMsg := fmt.Sprintf("Could not move task id=%s from %q to %q", msg.ID, base.ActiveKey(msg.Queue), base.DeadKey(msg.Queue))
		deadline, ok := ctx.Deadline()
		if !ok {
			panic("asynq: internal error: missing deadline in context")
		}
		p.logger.Warnf("%s; Will retry syncing", errMsg)
		p.syncRequestCh <- &syncRequest{
			fn: func() error {
				return p.broker.Kill(msg, e.Error())
			},
			errMsg:   errMsg,
			deadline: deadline,
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
		for i := 0; i < priority; i++ {
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
func perform(ctx context.Context, task *Task, h Handler) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("panic: %v", x)
		}
	}()
	return h.ProcessTask(ctx, task)
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
func sortByPriority(qcfg map[string]int) []string {
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
	priority int
}

type byPriority []*queue

func (x byPriority) Len() int           { return len(x) }
func (x byPriority) Less(i, j int) bool { return x[i].priority < x[j].priority }
func (x byPriority) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// normalizeQueues divides priority numbers by their greatest common divisor.
func normalizeQueues(queues map[string]int) map[string]int {
	var xs []int
	for _, x := range queues {
		xs = append(xs, x)
	}
	d := gcd(xs...)
	res := make(map[string]int)
	for q, x := range queues {
		res[q] = x / d
	}
	return res
}

func gcd(xs ...int) int {
	fn := func(x, y int) int {
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
