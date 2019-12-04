package asynq

import (
	"time"

	"github.com/google/uuid"
)

// Client is an interface for scheduling tasks.
type Client struct {
	rdb *rdb
}

// NewClient creates and returns a new client.
func NewClient(config *RedisConfig) *Client {
	return &Client{rdb: newRDB(config)}
}

// Process enqueues the task to be performed at a given time.
func (c *Client) Process(task *Task, processAt time.Time) error {
	msg := &taskMessage{
		ID:      uuid.New(),
		Type:    task.Type,
		Payload: task.Payload,
		Queue:   "default",
		Retry:   defaultMaxRetry,
	}
	return c.enqueue(msg, processAt)
}

// enqueue pushes a given task to the specified queue.
func (c *Client) enqueue(msg *taskMessage, processAt time.Time) error {
	if time.Now().After(processAt) {
		return c.rdb.enqueue(msg)
	}
	return c.rdb.schedule(scheduled, processAt, msg)
}
