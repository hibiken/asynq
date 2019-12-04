package asynq

import (
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/rdb"
)

// Client is an interface for scheduling tasks.
type Client struct {
	rdb *rdb.RDB
}

// NewClient creates and returns a new client.
func NewClient(config *RedisConfig) *Client {
	r := rdb.NewRDB(newRedisClient(config))
	return &Client{r}
}

// Process enqueues the task to be performed at a given time.
func (c *Client) Process(task *Task, processAt time.Time) error {
	msg := &rdb.TaskMessage{
		ID:      uuid.New(),
		Type:    task.Type,
		Payload: task.Payload,
		Queue:   "default",
		Retry:   defaultMaxRetry,
	}
	return c.enqueue(msg, processAt)
}

// enqueue pushes a given task to the specified queue.
func (c *Client) enqueue(msg *rdb.TaskMessage, processAt time.Time) error {
	if time.Now().After(processAt) {
		return c.rdb.Enqueue(msg)
	}
	return c.rdb.Schedule(rdb.Scheduled, processAt, msg)
}
