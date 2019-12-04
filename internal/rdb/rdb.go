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
	allQueues    = "asynq:queues"          // SET
	queuePrefix  = "asynq:queues:"         // LIST - asynq:queues:<qname>
	DefaultQueue = queuePrefix + "default" // LIST
	Scheduled    = "asynq:scheduled"       // ZSET
	Retry        = "asynq:retry"           // ZSET
	Dead         = "asynq:dead"            // ZSET
	InProgress   = "asynq:in_progress"     // SET
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

// Stats represents a state of queues at a certain time.
type Stats struct {
	Queued     int
	InProgress int
	Scheduled  int
	Retry      int
	Dead       int
	Timestamp  time.Time
}

// EnqueuedTask is a task in a queue and is ready to be processed.
// Note: This is read only and used for monitoring purpose.
type EnqueuedTask struct {
	ID      uuid.UUID
	Type    string
	Payload map[string]interface{}
}

// InProgressTask is a task that's currently being processed.
// Note: This is read only and used for monitoring purpose.
type InProgressTask struct {
	ID      uuid.UUID
	Type    string
	Payload map[string]interface{}
}

// ScheduledTask is a task that's scheduled to be processed in the future.
// Note: This is read only and used for monitoring purpose.
type ScheduledTask struct {
	ID        uuid.UUID
	Type      string
	Payload   map[string]interface{}
	ProcessAt time.Time
}

// RetryTask is a task that's in retry queue because worker failed to process the task.
// Note: This is read only and used for monitoring purpose.
type RetryTask struct {
	ID      uuid.UUID
	Type    string
	Payload map[string]interface{}
	// TODO(hibiken): add LastFailedAt time.Time
	ProcessAt time.Time
	ErrorMsg  string
	Retried   int
	Retry     int
}

// DeadTask is a task in that has exhausted all retries.
// Note: This is read only and used for monitoring purpose.
type DeadTask struct {
	ID           uuid.UUID
	Type         string
	Payload      map[string]interface{}
	LastFailedAt time.Time
	ErrorMsg     string
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
	data, err := r.client.BRPopLPush(DefaultQueue, InProgress, timeout).Result()
	if err == redis.Nil {
		return nil, ErrDequeueTimeout
	}
	if err != nil {
		return nil, fmt.Errorf("command `BRPOPLPUSH %q %q %v` failed: %v", DefaultQueue, InProgress, timeout, err)
	}
	var msg TaskMessage
	err = json.Unmarshal([]byte(data), &msg)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal %v to json: %v", data, err)
	}
	fmt.Printf("[DEBUG] perform task %+v from %s\n", msg, DefaultQueue)
	return &msg, nil
}

// Done removes the task from in-progress queue to mark the task as done.
func (r *RDB) Done(msg *TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	// NOTE: count ZERO means "remove all elements equal to val"
	err = r.client.LRem(InProgress, 0, string(bytes)).Err()
	if err != nil {
		return fmt.Errorf("command `LREM %s 0 %s` failed: %v", InProgress, string(bytes), err)
	}
	return nil
}

// Schedule adds the task to the backlog queue to be processed in the future.
func (r *RDB) Schedule(msg *TaskMessage, processAt time.Time) error {
	return r.schedule(Scheduled, processAt, msg)
}

// RetryLater adds the task to the backlog queue to be retried in the future.
func (r *RDB) RetryLater(msg *TaskMessage, processAt time.Time) error {
	return r.schedule(Retry, processAt, msg)
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

const maxDeadTask = 100
const deadExpirationInDays = 90

// Kill sends the taskMessage to "dead" set.
// It also trims the sorted set by timestamp and set size.
func (r *RDB) Kill(msg *TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("could not marshal %+v to json: %v", msg, err)
	}
	now := time.Now()
	pipe := r.client.Pipeline()
	pipe.ZAdd(Dead, &redis.Z{Member: string(bytes), Score: float64(now.Unix())})
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	pipe.ZRemRangeByScore(Dead, "-inf", strconv.Itoa(int(limit)))
	pipe.ZRemRangeByRank(Dead, 0, -maxDeadTask) // trim the set to 100
	_, err = pipe.Exec()
	return err
}

// MoveAll moves all tasks from src list to dst list.
func (r *RDB) MoveAll(src, dst string) error {
	script := redis.NewScript(`
	local len = redis.call("LLEN", KEYS[1])
	for i = len, 1, -1 do
		redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
	end
	return len
	`)
	_, err := script.Run(r.client, []string{src, dst}).Result()
	return err
}

// Forward moves all tasks with a score less than the current unix time
// from the given zset to the default queue.
// TODO(hibiken): Find a better method name that reflects what this does.
func (r *RDB) Forward(from string) error {
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
	res, err := script.Run(r.client, []string{from, allQueues, DefaultQueue}, now).Result()
	fmt.Printf("[DEBUG] got %d tasks from %q\n", len(res.([]interface{})), from)
	return err
}

// CurrentStats returns a current state of the queues.
func (r *RDB) CurrentStats() (*Stats, error) {
	pipe := r.client.Pipeline()
	qlen := pipe.LLen(DefaultQueue)
	plen := pipe.LLen(InProgress)
	slen := pipe.ZCard(Scheduled)
	rlen := pipe.ZCard(Retry)
	dlen := pipe.ZCard(Dead)
	_, err := pipe.Exec()
	if err != nil {
		return nil, err
	}
	return &Stats{
		Queued:     int(qlen.Val()),
		InProgress: int(plen.Val()),
		Scheduled:  int(slen.Val()),
		Retry:      int(rlen.Val()),
		Dead:       int(dlen.Val()),
		Timestamp:  time.Now(),
	}, nil
}

func (r *RDB) ListEnqueued() ([]*TaskMessage, error) {
	return r.rangeList(DefaultQueue)
}

func (r *RDB) ListInProgress() ([]*TaskMessage, error) {
	return r.rangeList(InProgress)
}

func (r *RDB) ListScheduled() ([]*ScheduledTask, error) {
	data, err := r.client.ZRangeWithScores(Scheduled, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var tasks []*ScheduledTask
	for _, z := range data {
		s, ok := z.Member.(string)
		if !ok {
			continue // bad data, ignore and continue
		}
		var msg TaskMessage
		err := json.Unmarshal([]byte(s), &msg)
		if err != nil {
			continue // bad data, ignore and continue
		}
		processAt := time.Unix(int64(z.Score), 0)
		tasks = append(tasks, &ScheduledTask{
			ID:        msg.ID,
			Type:      msg.Type,
			Payload:   msg.Payload,
			ProcessAt: processAt,
		})
	}
	return tasks, nil
}

func (r *RDB) ListRetry() ([]*RetryTask, error) {
	data, err := r.client.ZRangeWithScores(Retry, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var tasks []*RetryTask
	for _, z := range data {
		s, ok := z.Member.(string)
		if !ok {
			continue // bad data, ignore and continue
		}
		var msg TaskMessage
		err := json.Unmarshal([]byte(s), &msg)
		if err != nil {
			continue // bad data, ignore and continue
		}
		processAt := time.Unix(int64(z.Score), 0)
		tasks = append(tasks, &RetryTask{
			ID:        msg.ID,
			Type:      msg.Type,
			Payload:   msg.Payload,
			ErrorMsg:  msg.ErrorMsg,
			Retry:     msg.Retry,
			Retried:   msg.Retried,
			ProcessAt: processAt,
		})
	}
	return tasks, nil
}

func (r *RDB) ListDead() ([]*DeadTask, error) {
	data, err := r.client.ZRangeWithScores(Dead, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var tasks []*DeadTask
	for _, z := range data {
		s, ok := z.Member.(string)
		if !ok {
			continue // bad data, ignore and continue
		}
		var msg TaskMessage
		err := json.Unmarshal([]byte(s), &msg)
		if err != nil {
			continue // bad data, ignore and continue
		}
		lastFailedAt := time.Unix(int64(z.Score), 0)
		tasks = append(tasks, &DeadTask{
			ID:           msg.ID,
			Type:         msg.Type,
			Payload:      msg.Payload,
			ErrorMsg:     msg.ErrorMsg,
			LastFailedAt: lastFailedAt,
		})
	}
	return tasks, nil
}

func (r *RDB) rangeList(key string) ([]*TaskMessage, error) {
	data, err := r.client.LRange(key, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return r.toMessageSlice(data), nil
}

func (r *RDB) rangeZSet(key string) ([]*TaskMessage, error) {
	data, err := r.client.ZRange(key, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return r.toMessageSlice(data), nil
}

// toMessageSlice convers json strings to a slice of task messages.
func (r *RDB) toMessageSlice(data []string) []*TaskMessage {
	var msgs []*TaskMessage
	for _, s := range data {
		var msg TaskMessage
		err := json.Unmarshal([]byte(s), &msg)
		if err != nil {
			// bad data; ignore and continue
			continue
		}
		msgs = append(msgs, &msg)
	}
	return msgs
}
