package asynq

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
)

// Redis keys
const (
	queuePrefix = "asynq:queues:"
	allQueues   = "asynq:queues"
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
func (c *Client) Enqueue(queue string, task *Task, executeAt time.Time) error {
	if time.Now().After(executeAt) {
		fmt.Println("going directly to queue")
		bytes, err := json.Marshal(task)
		if err != nil {
			return err
		}
		return c.rdb.RPush(queuePrefix+queue, string(bytes)).Err()
	}
	dt := &delayedTask{
		ID:    uuid.New().String(),
		Queue: queue,
		Task:  task,
	}
	bytes, err := json.Marshal(dt)
	if err != nil {
		return err
	}
	return c.rdb.ZAdd(scheduled, &redis.Z{Member: string(bytes), Score: float64(executeAt.Unix())}).Err()
}

//-------------------- Workers --------------------

// Workers represents a pool of workers.
type Workers struct {
	rdb *redis.Client

	// poolTokens is a counting semaphore to ensure the number of active workers
	// does not exceed the limit.
	poolTokens chan struct{}

	// handlers maps queue name to a handler for that queue's messages.
	handlers map[string]func(msg string)
}

// NewWorkers creates and returns a new Workers.
func NewWorkers(poolSize int, opt *RedisOpt) *Workers {
	rdb := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	return &Workers{
		rdb:        rdb,
		poolTokens: make(chan struct{}, poolSize),
		handlers:   make(map[string]func(string)),
	}
}

// Handle registers a handler function for a given queue.
func (w *Workers) Handle(q string, fn func(msg string)) {
	w.handlers[q] = fn
}

// Run starts the workers and scheduler.
func (w *Workers) Run() {
	go w.pollScheduledTasks()

	for {
		// pull message out of the queue and process it
		// TODO(hibiken): get a list of queues in order of priority
		res, err := w.rdb.BLPop(0, "asynq:queues:test").Result() // A timeout of zero means block indefinitely.
		if err != nil {
			if err != redis.Nil {
				log.Printf("error when BLPOP from %s: %v\n", "aysnq:queues:test", err)
			}
			continue
		}

		q, msg := res[0], res[1]
		fmt.Printf("perform task %v from %s\n", msg, q)
		handler, ok := w.handlers[strings.TrimPrefix(q, queuePrefix)]
		if !ok {
			log.Printf("no handler found for queue %q\n", strings.TrimPrefix(q, queuePrefix))
			continue
		}
		w.poolTokens <- struct{}{} // acquire a token
		go func(msg string) {
			handler(msg)
			<-w.poolTokens
		}(msg)
	}
}

func (w *Workers) pollScheduledTasks() {
	for {
		// Get next items in the queue with scores (time to execute) <= now.
		now := time.Now().Unix()
		jobs, err := w.rdb.ZRangeByScore(scheduled,
			&redis.ZRangeBy{
				Min: "-inf",
				Max: strconv.Itoa(int(now))}).Result()
		fmt.Printf("len(jobs) = %d\n", len(jobs))
		if err != nil {
			log.Printf("radis command ZRANGEBYSCORE failed: %v\n", err)
			continue
		}
		if len(jobs) == 0 {
			fmt.Println("jobs empty")
			time.Sleep(5 * time.Second)
			continue
		}

		for _, j := range jobs {
			var job delayedTask
			err = json.Unmarshal([]byte(j), &job)
			if err != nil {
				fmt.Println("unmarshal failed")
				continue
			}

			// TODO(hibiken): Acquire lock for job.ID
			pipe := w.rdb.TxPipeline()
			pipe.ZRem(scheduled, j)
			// Do we need to encode this again?
			// Can we skip this entirely by defining Task field to be a string field?
			bytes, err := json.Marshal(job.Task)
			if err != nil {
				log.Printf("could not marshal job.Task %v: %v\n", job.Task, err)
				pipe.Discard()
				continue
			}
			pipe.RPush(queuePrefix+job.Queue, string(bytes))
			_, err = pipe.Exec()
			if err != nil {
				log.Printf("could not execute pipeline: %v\n", err)
				continue
			}
			// TODO(hibiken): Release lock for job.ID
		}
	}
}
