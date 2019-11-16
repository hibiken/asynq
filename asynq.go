package asynq

/*
TODOs:
- [P0] Task error handling
- [P0] Retry
- [P0] Dead task (retry exausted)
- [P0] Shutdown all workers gracefully when killed
- [P1] Add Support for multiple queues
*/

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload is an arbitrary data needed for task execution.
	// The value has to be serializable.
	Payload map[string]interface{}
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

// Process enqueues the task to be performed at a given time.
func (c *Client) Process(task *Task, executeAt time.Time) error {
	return c.enqueue("default", task, executeAt)
}

// enqueue pushes a given task to the specified queue.
func (c *Client) enqueue(queue string, task *Task, executeAt time.Time) error {
	if time.Now().After(executeAt) {
		return push(c.rdb, queue, task)
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
}

// NewWorkers creates and returns a new Workers.
func NewWorkers(poolSize int, opt *RedisOpt) *Workers {
	rdb := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	return &Workers{
		rdb:        rdb,
		poolTokens: make(chan struct{}, poolSize),
	}
}

// TaskHandler handles a given task and report any error.
type TaskHandler func(*Task) error

// Run starts the workers and scheduler with a given handler.
func (w *Workers) Run(handler TaskHandler) {
	go w.pollScheduledTasks()

	for {
		// pull message out of the queue and process it
		// TODO(hibiken): sort the list of queues in order of priority
		res, err := w.rdb.BLPop(0, listQueues(w.rdb)...).Result() // A timeout of zero means block indefinitely.
		if err != nil {
			if err != redis.Nil {
				log.Printf("BLPOP command failed: %v\n", err)
			}
			continue
		}

		q, msg := res[0], res[1]
		fmt.Printf("perform task %v from %s\n", msg, q)
		var task Task
		err = json.Unmarshal([]byte(msg), &task)
		if err != nil {
			log.Printf("[Servere Error] could not parse json encoded message %s: %v", msg, err)
			continue
		}
		w.poolTokens <- struct{}{} // acquire a token
		go func(task *Task) {
			handler(task)
			<-w.poolTokens
		}(&task)
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

			if w.rdb.ZRem(scheduled, j).Val() > 0 {
				err = push(w.rdb, job.Queue, job.Task)
				if err != nil {
					log.Printf("could not push task to queue %q: %v", job.Queue, err)
					// TODO(hibiken): Handle this error properly. Add back to scheduled ZSET?
					continue
				}
			}
		}
	}
}

// push pushes the task to the specified queue to get picked up by a worker.
func push(rdb *redis.Client, queue string, t *Task) error {
	bytes, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("could not encode task into JSON: %v", err)
	}
	qname := queuePrefix + queue
	err = rdb.SAdd(allQueues, qname).Err()
	if err != nil {
		return fmt.Errorf("could not execute command SADD %q %q: %v",
			allQueues, qname, err)
	}
	return rdb.RPush(qname, string(bytes)).Err()
}

// listQueues returns the list of all queues.
func listQueues(rdb *redis.Client) []string {
	return rdb.SMembers(allQueues).Val()
}
