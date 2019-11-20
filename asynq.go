package asynq

/*
TODOs:
- [P0] Write tests
- [P1] Add Support for multiple queues
- [P1] User defined max-retry count
- [P2] Web UI
*/

import (
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

// Max retry count by default
const defaultMaxRetry = 25

// Client is an interface for scheduling tasks.
type Client struct {
	rdb *rdb
}

// Task represents a task to be performed.
type Task struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload is an arbitrary data needed for task execution.
	// The value has to be serializable.
	Payload map[string]interface{}
}

// taskMessage is an internal representation of a task with additional metadata fields.
// This data gets written in redis.
type taskMessage struct {
	// fields from type Task
	Type    string
	Payload map[string]interface{}

	//------- metadata fields ----------
	// queue name this message should be enqueued to
	Queue string

	// max number of retry for this task.
	Retry int

	// number of times we've retried so far
	Retried int

	// error message from the last failure
	ErrorMsg string
}

// RedisOpt specifies redis options.
type RedisOpt struct {
	Addr     string
	Password string
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

//-------------------- Launcher --------------------

// Launcher starts the manager and poller.
type Launcher struct {
	// running indicates whether manager and poller are both running.
	running bool
	mu      sync.Mutex

	poller *poller

	manager *manager
}

// NewLauncher creates and returns a new Launcher.
func NewLauncher(poolSize int, opt *RedisOpt) *Launcher {
	client := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	rdb := newRDB(client)
	poller := newPoller(rdb, 5*time.Second, []string{scheduled, retry})
	manager := newManager(rdb, poolSize, nil)
	return &Launcher{
		poller:  poller,
		manager: manager,
	}
}

// TaskHandler handles a given task and report any error.
type TaskHandler func(*Task) error

// Start starts the manager and poller.
func (l *Launcher) Start(handler TaskHandler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.running {
		return
	}
	l.running = true
	l.manager.handler = handler

	l.poller.start()
	l.manager.start()
}

// Stop stops both manager and poller.
func (l *Launcher) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.running {
		return
	}
	l.running = false

	l.poller.terminate()
	l.manager.terminate()
}
