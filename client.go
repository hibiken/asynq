package asynq

import (
	"time"

	"github.com/go-redis/redis/v7"
)

// Client is an interface for scheduling tasks.
type Client struct {
	rdb *rdb
}

// NewClient creates and returns a new client.
func NewClient(opt *RedisOpt) *Client {
	client := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	return &Client{rdb: newRDB(client)}
}

// Process enqueues the task to be performed at a given time.
func (c *Client) Process(task *Task, executeAt time.Time) error {
	msg := &taskMessage{
		Type:    task.Type,
		Payload: task.Payload,
		Queue:   "default",
		Retry:   defaultMaxRetry,
	}
	return c.enqueue(msg, executeAt)
}

// enqueue pushes a given task to the specified queue.
func (c *Client) enqueue(msg *taskMessage, executeAt time.Time) error {
	if time.Now().After(executeAt) {
		return c.rdb.push(msg)
	}
	return c.rdb.zadd(scheduled, float64(executeAt.Unix()), msg)
}
