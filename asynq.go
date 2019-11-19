package asynq

/*
TODOs:
- [P0] Write tests
- [P1] Add Support for multiple queues
- [P1] User defined max-retry count
- [P2] Web UI
*/

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

// Redis keys
const (
	queuePrefix = "asynq:queues:"   // LIST
	allQueues   = "asynq:queues"    // SET
	scheduled   = "asynq:scheduled" // ZSET
	retry       = "asynq:retry"     // ZSET
	dead        = "asynq:dead"      // ZSET
)

// Max retry count by default
const defaultMaxRetry = 25

// Client is an interface for scheduling tasks.
type Client struct {
	rdb *redis.Client
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
	rdb := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	return &Client{rdb: rdb}
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
		return push(c.rdb, msg)
	}
	return zadd(c.rdb, scheduled, float64(executeAt.Unix()), msg)
}

//-------------------- Launcher --------------------

// Launcher starts the manager and poller.
type Launcher struct {
	rdb *redis.Client

	// running indicates whether manager and poller are both running.
	running bool
	mu      sync.Mutex

	poller *poller

	manager *manager
}

// NewLauncher creates and returns a new Launcher.
func NewLauncher(poolSize int, opt *RedisOpt) *Launcher {
	rdb := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	poller := &poller{
		rdb:         rdb,
		done:        make(chan struct{}),
		avgInterval: 5 * time.Second,
		zsets:       []string{scheduled, retry},
	}
	manager := newManager(rdb, poolSize, nil)
	return &Launcher{
		rdb:     rdb,
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

// push pushes the task to the specified queue to get picked up by a worker.
func push(rdb *redis.Client, msg *taskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not encode task into JSON: %v", err)
	}
	qname := queuePrefix + msg.Queue
	err = rdb.SAdd(allQueues, qname).Err()
	if err != nil {
		return fmt.Errorf("could not execute command SADD %q %q: %v",
			allQueues, qname, err)
	}
	return rdb.RPush(qname, string(bytes)).Err()
}

// zadd serializes the given message and adds to the specified sorted set.
func zadd(rdb *redis.Client, zset string, zscore float64, msg *taskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not encode task into JSON: %v", err)
	}
	return rdb.ZAdd(zset, &redis.Z{Member: string(bytes), Score: zscore}).Err()
}

const maxDeadTask = 100
const deadExpirationInDays = 90

// kill sends the task to "dead" sorted set. It also trim the sorted set by
// timestamp and set size.
func kill(rdb *redis.Client, msg *taskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not encode task into JSON: %v", err)
	}
	now := time.Now()
	pipe := rdb.Pipeline()
	pipe.ZAdd(dead, &redis.Z{Member: string(bytes), Score: float64(now.Unix())})
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	pipe.ZRemRangeByScore(dead, "-inf", strconv.Itoa(int(limit)))
	pipe.ZRemRangeByRank(dead, 0, -maxDeadTask) // trim the set to 100
	_, err = pipe.Exec()
	return err
}

// listQueues returns the list of all queues.
func listQueues(rdb *redis.Client) []string {
	return rdb.SMembers(allQueues).Val()
}

// delaySeconds returns a number seconds to delay before retrying.
// Formula taken from https://github.com/mperham/sidekiq.
func delaySeconds(count int) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	s := int(math.Pow(float64(count), 4)) + 15 + (r.Intn(30) * (count + 1))
	return time.Duration(s) * time.Second
}
