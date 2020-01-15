// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"strings"
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
	rdb *rdb.RDB
}

// NewClient and returns a new Client given a redis configuration.
func NewClient(r RedisConnOpt) *Client {
	rdb := rdb.NewRDB(createRedisClient(r))
	return &Client{rdb}
}

// Option specifies the processing behavior for the associated task.
type Option interface{}

// Internal option representations.
type (
	retryOption int
	queueOption string
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

// Queue returns an option to specify which queue to enqueue the task into.
//
// Queue name is case-insensitive and the lowercased version is used.
func Queue(name string) Option {
	return queueOption(strings.ToLower(name))
}

type option struct {
	retry int
	queue string
}

func composeOptions(opts ...Option) option {
	res := option{
		retry: defaultMaxRetry,
		queue: base.DefaultQueueName,
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			res.retry = int(opt)
		case queueOption:
			res.queue = string(opt)
		default:
			// ignore unexpected option
		}
	}
	return res
}

const (
	// Max retry count by default
	defaultMaxRetry = 25
)

// Schedule registers a task to be processed at the specified time.
//
// Schedule returns nil if the task is registered successfully,
// otherwise returns non-nil error.
//
// opts specifies the behavior of task processing. If there are conflicting
// Option the last one overrides the ones before.
func (c *Client) Schedule(task *Task, processAt time.Time, opts ...Option) error {
	opt := composeOptions(opts...)
	msg := &base.TaskMessage{
		ID:      xid.New(),
		Type:    task.Type,
		Payload: task.Payload.data,
		Queue:   opt.queue,
		Retry:   opt.retry,
	}
	return c.enqueue(msg, processAt)
}

func (c *Client) enqueue(msg *base.TaskMessage, processAt time.Time) error {
	if time.Now().After(processAt) {
		return c.rdb.Enqueue(msg)
	}
	return c.rdb.Schedule(msg, processAt)
}
