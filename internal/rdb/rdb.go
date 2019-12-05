// Package rdb encapsulates the interactions with redis.
package rdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
)

// Redis keys
const (
	allQueues   = "asynq:queues"          // SET
	queuePrefix = "asynq:queues:"         // LIST - asynq:queues:<qname>
	defaultQ    = queuePrefix + "default" // LIST
	scheduledQ  = "asynq:scheduled"       // ZSET
	retryQ      = "asynq:retry"           // ZSET
	deadQ       = "asynq:dead"            // ZSET
	inProgressQ = "asynq:in_progress"     // LIST
)

// ErrDequeueTimeout indicates that the blocking dequeue operation timed out.
var ErrDequeueTimeout = errors.New("blocking dequeue operation timed out")

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
	ID uuid.UUID
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
	pipe.SAdd(allQueues, qname)
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
	fmt.Printf("[DEBUG] perform task %+v from %s\n", msg, defaultQ)
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
	return r.schedule(scheduledQ, processAt, msg)
}

// RetryLater adds the task to the backlog queue to be retried in the future.
func (r *RDB) RetryLater(msg *TaskMessage, processAt time.Time) error {
	return r.schedule(retryQ, processAt, msg)
}

// schedule adds the task to the zset to be processd at the specified time.
func (r *RDB) schedule(zset string, processAt time.Time, msg *TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	score := float64(processAt.Unix())
	err = r.client.ZAdd(zset, &redis.Z{Member: string(bytes), Score: score}).Err()
	if err != nil {
		return fmt.Errorf("command `ZADD %s %.1f %s` failed: %v", zset, score, string(bytes), err)
	}
	return nil
}

// Kill sends the task to "dead" set.
// It also trims the set by timestamp and set size.
func (r *RDB) Kill(msg *TaskMessage) error {
	const maxDeadTask = 10
	const deadExpirationInDays = 90
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	now := time.Now()
	pipe := r.client.Pipeline()
	pipe.ZAdd(deadQ, &redis.Z{Member: string(bytes), Score: float64(now.Unix())})
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	pipe.ZRemRangeByScore(deadQ, "-inf", strconv.Itoa(int(limit)))
	pipe.ZRemRangeByRank(deadQ, 0, -maxDeadTask) // trim the set to 100
	_, err = pipe.Exec()
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

// Forward moves all tasks with a score less than the current unix time
// from the given zset to the default queue.
func (r *RDB) forward(from string) error {
	script := redis.NewScript(`
	local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
	for _, msg in ipairs(msgs) do
		redis.call("ZREM", KEYS[1], msg)
		redis.call("SADD", KEYS[2], KEYS[3])
		redis.call("LPUSH", KEYS[3], msg)
	end
	return msgs
	`)
	now := float64(time.Now().Unix())
	res, err := script.Run(r.client, []string{from, allQueues, defaultQ}, now).Result()
	fmt.Printf("[DEBUG] got %d tasks from %q\n", len(res.([]interface{})), from)
	return err
}