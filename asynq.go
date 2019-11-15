package asynq

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
)

// Redis keys
const (
	queuePrefix = "asynq:queues:"
	scheduled   = "asynq:scheduled"
)

// Client is an interface for scheduling tasks.
type Client struct {
	rdb *redis.Client
}

// Task represents a task to be performed.
type Task struct {
	Handler string
	Args    []interface{}
}

type delayedTask struct {
	ID    string
	Queue string
	Task  *Task
}

// RedisOpt specifies redis options.
type RedisOpt struct {
	Addr     string
	Password string
}

// NewClient creates and returns a new client.
func NewClient(opt *RedisOpt) *Client {
	rdb := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	return &Client{rdb: rdb}
}

// Enqueue pushes a given task to the specified queue.
func (c *Client) Enqueue(queue string, task *Task, delay time.Duration) error {
	if delay == 0 {
		bytes, err := json.Marshal(task)
		if err != nil {
			return err
		}
		return c.rdb.RPush(queuePrefix+queue, string(bytes)).Err()
	}
	executeAt := time.Now().Add(delay)
	dt := &delayedTask{
		ID: uuid.New().String(),
		Queue: queue,
		Task: task,
	}
	bytes, err := json.Marshal(dt)
	if err != nil {
		return err
	}
	return c.rdb.ZAdd(scheduled, &redis.Z{Member: string(bytes), Score: float64(executeAt.Unix())}).Err()
}
