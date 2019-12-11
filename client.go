package asynq

import (
	"time"

	"github.com/rs/xid"
	"github.com/hibiken/asynq/internal/rdb"
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
func NewClient(cfg *RedisConfig) *Client {
	r := rdb.NewRDB(newRedisClient(cfg))
	return &Client{r}
}

// Process registers a task to be processed at the specified time.
//
// Process returns nil if the task is registered successfully,
// otherwise returns non-nil error.
func (c *Client) Process(task *Task, processAt time.Time) error {
	msg := &rdb.TaskMessage{
		ID:      xid.New(),
		Type:    task.Type,
		Payload: task.Payload,
		Queue:   "default",
		Retry:   defaultMaxRetry,
	}
	return c.enqueue(msg, processAt)
}

func (c *Client) enqueue(msg *rdb.TaskMessage, processAt time.Time) error {
	if time.Now().After(processAt) {
		return c.rdb.Enqueue(msg)
	}
	return c.rdb.Schedule(msg, processAt)
}
