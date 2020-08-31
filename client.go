// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

// A Client is responsible for scheduling tasks.
//
// A Client is used to register tasks that should be processed
// immediately or some time in the future.
//
// Clients are safe for concurrent use by multiple goroutines.
type Client struct {
	mu   sync.Mutex
	opts map[string][]Option
	rdb  *rdb.RDB
}

// NewClient and returns a new Client given a redis connection option.
func NewClient(r RedisConnOpt) *Client {
	rdb := rdb.NewRDB(createRedisClient(r))
	return &Client{
		opts: make(map[string][]Option),
		rdb:  rdb,
	}
}

// Option specifies the task processing behavior.
type Option interface{}

// Internal option representations.
type (
	retryOption    int
	queueOption    string
	timeoutOption  time.Duration
	deadlineOption time.Time
	uniqueOption   time.Duration
)

// MaxRetry returns an option to specify the max number of times
// the task will be retried.
//
// Negative retry count is treated as zero retry.
func MaxRetry(n int) Option {
	if n < 0 {
		n = 0
	}
	return retryOption(n)
}

// Queue returns an option to specify the queue to enqueue the task into.
//
// Queue name is case-insensitive and the lowercased version is used.
func Queue(name string) Option {
	return queueOption(strings.ToLower(name))
}

// Timeout returns an option to specify how long a task may run.
// If the timeout elapses before the Handler returns, then the task
// will be retried.
//
// Zero duration means no limit.
//
// If there's a conflicting Deadline option, whichever comes earliest
// will be used.
func Timeout(d time.Duration) Option {
	return timeoutOption(d)
}

// Deadline returns an option to specify the deadline for the given task.
// If it reaches the deadline before the Handler returns, then the task
// will be retried.
//
// If there's a conflicting Timeout option, whichever comes earliest
// will be used.
func Deadline(t time.Time) Option {
	return deadlineOption(t)
}

// Unique returns an option to enqueue a task only if the given task is unique.
// Task enqueued with this option is guaranteed to be unique within the given ttl.
// Once the task gets processed successfully or once the TTL has expired, another task with the same uniqueness may be enqueued.
// ErrDuplicateTask error is returned when enqueueing a duplicate task.
//
// Uniqueness of a task is based on the following properties:
//     - Task Type
//     - Task Payload
//     - Queue Name
func Unique(ttl time.Duration) Option {
	return uniqueOption(ttl)
}

// ErrDuplicateTask indicates that the given task could not be enqueued since it's a duplicate of another task.
//
// ErrDuplicateTask error only applies to tasks enqueued with a Unique option.
var ErrDuplicateTask = errors.New("task already exists")

type option struct {
	retry     int
	queue     string
	timeout   time.Duration
	deadline  time.Time
	uniqueTTL time.Duration
}

// composeOptions merges user provided options into the default options
// and returns the composed option.
// It also validates the user provided options and returns an error if any of
// the user provided options fail the validations.
func composeOptions(opts ...Option) (option, error) {
	res := option{
		retry:    defaultMaxRetry,
		queue:    base.DefaultQueueName,
		timeout:  0, // do not set to deafultTimeout here
		deadline: time.Time{},
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			res.retry = int(opt)
		case queueOption:
			trimmed := strings.TrimSpace(string(opt))
			if err := validateQueueName(trimmed); err != nil {
				return option{}, err
			}
			res.queue = trimmed
		case timeoutOption:
			res.timeout = time.Duration(opt)
		case deadlineOption:
			res.deadline = time.Time(opt)
		case uniqueOption:
			res.uniqueTTL = time.Duration(opt)
		default:
			// ignore unexpected option
		}
	}
	return res, nil
}

func validateQueueName(qname string) error {
	if len(qname) == 0 {
		return fmt.Errorf("queue name must contain one or more characters")
	}
	return nil
}

const (
	// Default max retry count used if nothing is specified.
	defaultMaxRetry = 25

	// Default timeout used if both timeout and deadline are not specified.
	defaultTimeout = 30 * time.Minute
)

// Value zero indicates no timeout and no deadline.
var (
	noTimeout  time.Duration = 0
	noDeadline time.Time     = time.Unix(0, 0)
)

// SetDefaultOptions sets options to be used for a given task type.
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
//
// Default options can be overridden by options passed at enqueue time.
func (c *Client) SetDefaultOptions(taskType string, opts ...Option) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opts[taskType] = opts
}

// A Result holds enqueued task's metadata.
type Result struct {
	// ID is a unique identifier for the task.
	ID string

	// Retry is the maximum number of retry for the task.
	Retry int

	// Queue is a name of the queue the task is enqueued to.
	Queue string

	// Timeout is the timeout value for the task.
	// Counting for timeout starts when a worker starts processing the task.
	// If task processing doesn't complete within the timeout, the task will be retried.
	// The value zero means no timeout.
	//
	// If deadline is set, min(now+timeout, deadline) is used, where the now is the time when
	// a worker starts processing the task.
	Timeout time.Duration

	// Deadline is the deadline value for the task.
	// If task processing doesn't complete before the deadline, the task will be retried.
	// The value time.Unix(0, 0) means no deadline.
	//
	// If timeout is set, min(now+timeout, deadline) is used, where the now is the time when
	// a worker starts processing the task.
	Deadline time.Time
}

// EnqueueAt schedules task to be enqueued at the specified time.
//
// EnqueueAt returns nil if the task is scheduled successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// By deafult, max retry is set to 25 and timeout is set to 30 minutes.
func (c *Client) EnqueueAt(t time.Time, task *Task, opts ...Option) (*Result, error) {
	return c.enqueueAt(t, task, opts...)
}

// Enqueue enqueues task to be processed immediately.
//
// Enqueue returns nil if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// By deafult, max retry is set to 25 and timeout is set to 30 minutes.
func (c *Client) Enqueue(task *Task, opts ...Option) (*Result, error) {
	return c.enqueueAt(time.Now(), task, opts...)
}

// EnqueueIn schedules task to be enqueued after the specified delay.
//
// EnqueueIn returns nil if the task is scheduled successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
// By deafult, max retry is set to 25 and timeout is set to 30 minutes.
func (c *Client) EnqueueIn(d time.Duration, task *Task, opts ...Option) (*Result, error) {
	return c.enqueueAt(time.Now().Add(d), task, opts...)
}

// Close closes the connection with redis server.
func (c *Client) Close() error {
	return c.rdb.Close()
}

func (c *Client) enqueueAt(t time.Time, task *Task, opts ...Option) (*Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if defaults, ok := c.opts[task.Type]; ok {
		opts = append(defaults, opts...)
	}
	opt, err := composeOptions(opts...)
	if err != nil {
		return nil, err
	}
	deadline := noDeadline
	if !opt.deadline.IsZero() {
		deadline = opt.deadline
	}
	timeout := noTimeout
	if opt.timeout != 0 {
		timeout = opt.timeout
	}
	if deadline.Equal(noDeadline) && timeout == noTimeout {
		// If neither deadline nor timeout are set, use default timeout.
		timeout = defaultTimeout
	}
	var uniqueKey string
	if opt.uniqueTTL > 0 {
		uniqueKey = base.UniqueKey(opt.queue, task.Type, task.Payload.data)
	}
	msg := &base.TaskMessage{
		ID:        uuid.New(),
		Type:      task.Type,
		Payload:   task.Payload.data,
		Queue:     opt.queue,
		Retry:     opt.retry,
		Deadline:  deadline.Unix(),
		Timeout:   int64(timeout.Seconds()),
		UniqueKey: uniqueKey,
	}
	now := time.Now()
	if t.Before(now) || t.Equal(now) {
		err = c.enqueue(msg, opt.uniqueTTL)
	} else {
		err = c.schedule(msg, t, opt.uniqueTTL)
	}
	switch {
	case err == rdb.ErrDuplicateTask:
		return nil, fmt.Errorf("%w", ErrDuplicateTask)
	case err != nil:
		return nil, err
	}
	return &Result{
		ID:       msg.ID.String(),
		Queue:    msg.Queue,
		Retry:    msg.Retry,
		Timeout:  timeout,
		Deadline: deadline,
	}, nil
}

func (c *Client) enqueue(msg *base.TaskMessage, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		return c.rdb.EnqueueUnique(msg, uniqueTTL)
	}
	return c.rdb.Enqueue(msg)
}

func (c *Client) schedule(msg *base.TaskMessage, t time.Time, uniqueTTL time.Duration) error {
	if uniqueTTL > 0 {
		ttl := t.Add(uniqueTTL).Sub(time.Now())
		return c.rdb.ScheduleUnique(msg, t, ttl)
	}
	return c.rdb.Schedule(msg, t)
}
