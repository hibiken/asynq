// Package rdb encapsulates the interactions with redis.
package rdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/rs/xid"
)

// Redis keys
const (
	queuePrefix = "asynq:queues:"         // LIST - asynq:queues:<qname>
	defaultQ    = queuePrefix + "default" // LIST
	scheduledQ  = "asynq:scheduled"       // ZSET
	retryQ      = "asynq:retry"           // ZSET
	deadQ       = "asynq:dead"            // ZSET
	inProgressQ = "asynq:in_progress"     // LIST
)

var (
	// ErrDequeueTimeout indicates that the blocking dequeue operation timed out.
	ErrDequeueTimeout = errors.New("blocking dequeue operation timed out")

	// ErrTaskNotFound indicates that a task that matches the given identifier was not found.
	ErrTaskNotFound = errors.New("could not find a task")
)

// RDB is a client interface to query and mutate task queues.
type RDB struct {
	client *redis.Client
}

// NewRDB returns a new instance of RDB.
func NewRDB(client *redis.Client) *RDB {
	return &RDB{client}
}

// TaskMessage is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written in redis.
type TaskMessage struct {
	//-------- Task fields --------
	// Type represents the kind of task.
	Type string
	// Payload holds data needed to process the task.
	Payload map[string]interface{}

	//-------- Metadata fields --------
	// ID is a unique identifier for each task
	ID xid.ID
	// Queue is a name this message should be enqueued to
	Queue string
	// Retry is the max number of retry for this task.
	Retry int
	// Retried is the number of times we've retried this task so far
	Retried int
	// ErrorMsg holds the error message from the last failure
	ErrorMsg string
}

// Close closes the connection with redis server.
func (r *RDB) Close() error {
	return r.client.Close()
}

// Enqueue inserts the given task to the end of the queue.
// It also adds the queue name to the "all-queues" list.
func (r *RDB) Enqueue(msg *TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	qname := queuePrefix + msg.Queue
	pipe := r.client.Pipeline()
	pipe.LPush(qname, string(bytes))
	_, err = pipe.Exec()
	if err != nil {
		return fmt.Errorf("could not enqueue the task %+v to %q: %v", msg, qname, err)
	}
	return nil
}

// Dequeue blocks until there is a task available to be processed,
// once a task is available, it adds the task to "in progress" list
// and returns the task.
func (r *RDB) Dequeue(timeout time.Duration) (*TaskMessage, error) {
	data, err := r.client.BRPopLPush(defaultQ, inProgressQ, timeout).Result()
	if err == redis.Nil {
		return nil, ErrDequeueTimeout
	}
	if err != nil {
		return nil, fmt.Errorf("command `BRPOPLPUSH %q %q %v` failed: %v", defaultQ, inProgressQ, timeout, err)
	}
	var msg TaskMessage
	err = json.Unmarshal([]byte(data), &msg)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal %v to json: %v", data, err)
	}
	return &msg, nil
}

// Done removes the task from in-progress queue to mark the task as done.
func (r *RDB) Done(msg *TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	// NOTE: count ZERO means "remove all elements equal to val"
	err = r.client.LRem(inProgressQ, 0, string(bytes)).Err()
	if err != nil {
		return fmt.Errorf("command `LREM %s 0 %s` failed: %v", inProgressQ, string(bytes), err)
	}
	return nil
}

// Schedule adds the task to the backlog queue to be processed in the future.
func (r *RDB) Schedule(msg *TaskMessage, processAt time.Time) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	score := float64(processAt.Unix())
	err = r.client.ZAdd(scheduledQ, &redis.Z{Member: string(bytes), Score: score}).Err()
	if err != nil {
		return fmt.Errorf("command `ZADD %s %.1f %s` failed: %v", scheduledQ, score, string(bytes), err)
	}
	return nil
}

// Retry moves the task from in-progress to retry queue, incrementing retry count
// and assigning error message to the task message.
func (r *RDB) Retry(msg *TaskMessage, processAt time.Time, errMsg string) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	// 	KEYS[1] -> asynq:in_progress
	//  KEYS[2] -> asynq:retry
	//  ARGV[1] -> TaskMessage value
	//  ARGV[2] -> error message
	//  ARGV[3] -> retry_at UNIX timestamp
	script := redis.NewScript(`
	redis.call("LREM", KEYS[1], 0, ARGV[1])
	local msg = cjson.decode(ARGV[1])
	msg["Retried"] = msg["Retried"] + 1
	msg["ErrorMsg"] = ARGV[2]
	redis.call("ZADD", KEYS[2], ARGV[3], cjson.encode(msg))
	return redis.status_reply("OK")
	`)
	_, err = script.Run(r.client, []string{inProgressQ, retryQ}, string(bytes), errMsg, processAt.Unix()).Result()
	return err
}

// Kill sends the task to "dead" queue from in-progress queue, assigning
// the error message to the task.
// It also trims the set by timestamp and set size.
func (r *RDB) Kill(msg *TaskMessage, errMsg string) error {
	const maxDeadTask = 10
	const deadExpirationInDays = 90
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	now := time.Now()
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	// KEYS[1] -> asynq:in_progress
	// KEYS[2] -> asynq:dead
	// ARGV[1] -> TaskMessage value
	// ARGV[2] -> error message
	// ARGV[3] -> died_at UNIX timestamp
	// ARGV[4] -> cutoff timestamp (e.g., 90 days ago)
	// ARGV[5] -> max number of tasks in dead queue (e.g., 100)
	script := redis.NewScript(`
	redis.call("LREM", KEYS[1], 0, ARGV[1])
	local msg = cjson.decode(ARGV[1])
	msg["ErrorMsg"] = ARGV[2]
	redis.call("ZADD", KEYS[2], ARGV[3], cjson.encode(msg))
	redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[4])
	redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[5])
	return redis.status_reply("OK")
	`)
	_, err = script.Run(r.client, []string{inProgressQ, deadQ},
		string(bytes), errMsg, now.Unix(), limit, maxDeadTask).Result()
	return err
}

// RestoreUnfinished  moves all tasks from in-progress list to the queue.
func (r *RDB) RestoreUnfinished() error {
	script := redis.NewScript(`
	local len = redis.call("LLEN", KEYS[1])
	for i = len, 1, -1 do
		redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
	end
	return len
	`)
	_, err := script.Run(r.client, []string{inProgressQ, defaultQ}).Result()
	return err
}

// CheckAndEnqueue checks for all scheduled tasks and enqueues any tasks that
// have to be processed.
func (r *RDB) CheckAndEnqueue() error {
	delayed := []string{scheduledQ, retryQ}
	for _, zset := range delayed {
		if err := r.forward(zset); err != nil {
			return err
		}
	}
	return nil
}

// forward moves all tasks with a score less than the current unix time
// from the given zset to the default queue.
func (r *RDB) forward(from string) error {
	script := redis.NewScript(`
	local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
	for _, msg in ipairs(msgs) do
		redis.call("ZREM", KEYS[1], msg)
		redis.call("LPUSH", KEYS[2], msg)
	end
	return msgs
	`)
	now := float64(time.Now().Unix())
	_, err := script.Run(r.client, []string{from, defaultQ}, now).Result()
	return err
}
