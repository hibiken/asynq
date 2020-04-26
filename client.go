// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/rs/xid"
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
//
// Zero duration means no limit.
func Timeout(d time.Duration) Option {
	return timeoutOption(d)
}

// Deadline returns an option to specify the deadline for the given task.
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

func composeOptions(opts ...Option) option {
	res := option{
		retry:    defaultMaxRetry,
		queue:    base.DefaultQueueName,
		timeout:  0,
		deadline: time.Time{},
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			res.retry = int(opt)
		case queueOption:
			res.queue = string(opt)
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
	return res
}

// uniqueKey computes the redis key used for the given task.
// It returns an empty string if ttl is zero.
func uniqueKey(t *Task, ttl time.Duration, qname string) string {
	if ttl == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%s:%s", t.Type, serializePayload(t.Payload.data), qname)
}

func serializePayload(payload map[string]interface{}) string {
	if payload == nil {
		return "nil"
	}
	type entry struct {
		k string
		v interface{}
	}
	var es []entry
	for k, v := range payload {
		es = append(es, entry{k, v})
	}
	// sort entries by key
	sort.Slice(es, func(i, j int) bool { return es[i].k < es[j].k })
	var b strings.Builder
	for _, e := range es {
		if b.Len() > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%s=%v", e.k, e.v))
	}
	return b.String()
}

// Default max retry count used if nothing is specified.
const defaultMaxRetry = 25

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

// EnqueueAt schedules task to be enqueued at the specified time.
//
// EnqueueAt returns nil if the task is scheduled successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
func (c *Client) EnqueueAt(t time.Time, task *Task, opts ...Option) error {
	return c.enqueueAt(t, task, opts...)
}

// Enqueue enqueues task to be processed immediately.
//
// Enqueue returns nil if the task is enqueued successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
func (c *Client) Enqueue(task *Task, opts ...Option) error {
	return c.enqueueAt(time.Now(), task, opts...)
}

// EnqueueIn schedules task to be enqueued after the specified delay.
//
// EnqueueIn returns nil if the task is scheduled successfully, otherwise returns a non-nil error.
//
// The argument opts specifies the behavior of task processing.
// If there are conflicting Option values the last one overrides others.
func (c *Client) EnqueueIn(d time.Duration, task *Task, opts ...Option) error {
	return c.enqueueAt(time.Now().Add(d), task, opts...)
}

func (c *Client) enqueueAt(t time.Time, task *Task, opts ...Option) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if defaults, ok := c.opts[task.Type]; ok {
		opts = append(defaults, opts...)
	}
	opt := composeOptions(opts...)
	msg := &base.TaskMessage{
		ID:        xid.New(),
		Type:      task.Type,
		Payload:   task.Payload.data,
		Queue:     opt.queue,
		Retry:     opt.retry,
		Timeout:   opt.timeout.String(),
		Deadline:  opt.deadline.Format(time.RFC3339),
		UniqueKey: uniqueKey(task, opt.uniqueTTL, opt.queue),
	}
	var err error
	if time.Now().After(t) {
		err = c.enqueue(msg, opt.uniqueTTL)
	} else {
		err = c.schedule(msg, t, opt.uniqueTTL)
	}
	if err == rdb.ErrDuplicateTask {
		return fmt.Errorf("%w", ErrDuplicateTask)
	}
	return err
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
