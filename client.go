package asynq

import (
	"time"

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
func NewClient(cfg *RedisConfig) *Client {
	r := rdb.NewRDB(newRedisClient(cfg))
	return &Client{r}
}

// Option configures the behavior of task processing.
type Option interface{}

// max number of times a task will be retried.
type retryOption int

// MaxRetry returns an option to specify the max number of times
// a task will be retried.
//
// Negative retry count is treated as zero retry.
func MaxRetry(n int) Option {
	if n < 0 {
		n = 0
	}
	return retryOption(n)
}

type option struct {
	retry int
}

func composeOptions(opts ...Option) option {
	res := option{
		retry: defaultMaxRetry,
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case retryOption:
			res.retry = int(opt)
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

// Process registers a task to be processed at the specified time.
//
// Process returns nil if the task is registered successfully,
// otherwise returns non-nil error.
//
// opts specifies the behavior of task processing. If there are conflicting
// Option the last one overrides the ones before.
func (c *Client) Process(task *Task, processAt time.Time, opts ...Option) error {
	opt := composeOptions(opts...)
	msg := &rdb.TaskMessage{
		ID:      xid.New(),
		Type:    task.Type,
		Payload: task.Payload,
		Queue:   "default",
		Retry:   opt.retry,
	}
	return c.enqueue(msg, processAt)
}

func (c *Client) enqueue(msg *rdb.TaskMessage, processAt time.Time) error {
	if time.Now().After(processAt) {
		return c.rdb.Enqueue(msg)
	}
	return c.rdb.Schedule(msg, processAt)
}
