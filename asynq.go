package asynq

/*
TODOs:
- [P0] Write tests
- [P0] Shutdown all workers gracefully when the process gets killed
- [P1] Add Support for multiple queues
- [P1] User defined max-retry count
- [P2] Web UI
*/

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
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

//-------------------- Workers --------------------

// Workers represents a pool of workers.
type Workers struct {
	rdb *redis.Client

	// poolTokens is a counting semaphore to ensure the number of active workers
	// does not exceed the limit.
	poolTokens chan struct{}

	// running indicates whether the workes are currently running.
	running bool
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
	if w.running {
		return
	}
	w.running = true

	go w.pollDeferred()

	for {
		// pull message out of the queue and process it
		// TODO(hibiken): sort the list of queues in order of priority
		res, err := w.rdb.BLPop(5*time.Second, listQueues(w.rdb)...).Result() // NOTE: BLPOP needs to time out because if case a new queue is added.
		if err != nil {
			if err != redis.Nil {
				log.Printf("BLPOP command failed: %v\n", err)
			}
			continue
		}

		q, data := res[0], res[1]
		fmt.Printf("perform task %v from %s\n", data, q)
		var msg taskMessage
		err = json.Unmarshal([]byte(data), &msg)
		if err != nil {
			log.Printf("[Servere Error] could not parse json encoded message %s: %v", data, err)
			continue
		}
		t := &Task{Type: msg.Type, Payload: msg.Payload}
		w.poolTokens <- struct{}{} // acquire token
		go func(task *Task) {
			defer func() { <-w.poolTokens }() // release token
			err := handler(task)
			if err != nil {
				if msg.Retried >= msg.Retry {
					fmt.Println("Retry exhausted!!!")
					if err := kill(w.rdb, &msg); err != nil {
						log.Printf("[SERVER ERROR] could not add task %+v to 'dead' set\n", err)
					}
					return
				}
				fmt.Println("RETRY!!!")
				retryAt := time.Now().Add(delaySeconds((msg.Retried)))
				fmt.Printf("[DEBUG] retying the task in %v\n", retryAt.Sub(time.Now()))
				msg.Retried++
				msg.ErrorMsg = err.Error()
				if err := zadd(w.rdb, retry, float64(retryAt.Unix()), &msg); err != nil {
					// TODO(hibiken): Not sure how to handle this error
					log.Printf("[SEVERE ERROR] could not add msg %+v to 'retry' set: %v\n", msg, err)
					return
				}

			}
		}(t)
	}
}

func (w *Workers) pollDeferred() {
	zsets := []string{scheduled, retry}
	for {
		for _, zset := range zsets {
			// Get next items in the queue with scores (time to execute) <= now.
			now := time.Now().Unix()
			fmt.Printf("[DEBUG] polling ZSET %q\n", zset)
			jobs, err := w.rdb.ZRangeByScore(zset,
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
				continue
			}

			for _, j := range jobs {
				var msg taskMessage
				err = json.Unmarshal([]byte(j), &msg)
				if err != nil {
					fmt.Println("unmarshal failed")
					continue
				}

				if w.rdb.ZRem(zset, j).Val() > 0 {
					err = push(w.rdb, &msg)
					if err != nil {
						log.Printf("could not push task to queue %q: %v", msg.Queue, err)
						// TODO(hibiken): Handle this error properly. Add back to scheduled ZSET?
						continue
					}
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
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
