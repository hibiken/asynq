// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package rdb encapsulates the interactions with redis.
package rdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/base"
	"github.com/spf13/cast"
)

var (
	// ErrNoProcessableTask indicates that there are no tasks ready to be processed.
	ErrNoProcessableTask = errors.New("no tasks are ready for processing")

	// ErrTaskNotFound indicates that a task that matches the given identifier was not found.
	ErrTaskNotFound = errors.New("could not find a task")
)

const statsTTL = 90 * 24 * time.Hour // 90 days

// RDB is a client interface to query and mutate task queues.
type RDB struct {
	client *redis.Client
}

// NewRDB returns a new instance of RDB.
func NewRDB(client *redis.Client) *RDB {
	return &RDB{client}
}

// Close closes the connection with redis server.
func (r *RDB) Close() error {
	return r.client.Close()
}

// Enqueue inserts the given task to the tail of the queue.
func (r *RDB) Enqueue(msg *base.TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	key := base.QueueKey(msg.Queue)
	return r.client.LPush(key, string(bytes)).Err()
}

// Dequeue queries given queues in order and pops a task message if there
// is one and returns it. If all queues are empty,  ErrNoProcessableTask
// error is returned.
func (r *RDB) Dequeue(qnames ...string) (*base.TaskMessage, error) {
	var keys []string
	for _, q := range qnames {
		keys = append(keys, base.QueueKey(q))
	}
	data, err := r.dequeue(keys...)
	if err == redis.Nil {
		return nil, ErrNoProcessableTask
	}
	if err != nil {
		return nil, err
	}
	var msg base.TaskMessage
	err = json.Unmarshal([]byte(data), &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (r *RDB) dequeue(queues ...string) (data string, err error) {
	var args []interface{}
	for _, qkey := range queues {
		args = append(args, qkey)
	}
	script := redis.NewScript(`
	local res
	for _, qkey in ipairs(ARGV) do
		res = redis.call("RPOPLPUSH", qkey, KEYS[1])
		if res then
			return res
		end
	end
	return res
	`)
	res, err := script.Run(r.client, []string{base.InProgressQueue}, args...).Result()
	if err != nil {
		return "", err
	}
	return cast.ToStringE(res)
}

// Done removes the task from in-progress queue to mark the task as done.
func (r *RDB) Done(msg *base.TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	// Note: LREM count ZERO means "remove all elements equal to val"
	// KEYS[1] -> asynq:in_progress
	// KEYS[2] -> asynq:processed:<yyyy-mm-dd>
	// ARGV[1] -> base.TaskMessage value
	// ARGV[2] -> stats expiration timestamp
	script := redis.NewScript(`
	redis.call("LREM", KEYS[1], 0, ARGV[1]) 
	local n = redis.call("INCR", KEYS[2])
	if tonumber(n) == 1 then
		redis.call("EXPIREAT", KEYS[2], ARGV[2])
	end
	return redis.status_reply("OK")
	`)
	now := time.Now()
	processedKey := base.ProcessedKey(now)
	expireAt := now.Add(statsTTL)
	return script.Run(r.client,
		[]string{base.InProgressQueue, processedKey},
		string(bytes), expireAt.Unix()).Err()
}

// Requeue moves the task from in-progress queue to the default
// queue.
func (r *RDB) Requeue(msg *base.TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	// Note: Use RPUSH to push to the head of the queue.
	// KEYS[1] -> asynq:in_progress
	// KEYS[2] -> asynq:queues:default
	// ARGV[1] -> base.TaskMessage value
	script := redis.NewScript(`
	redis.call("LREM", KEYS[1], 0, ARGV[1])
	redis.call("RPUSH", KEYS[2], ARGV[1])
	return redis.status_reply("OK")
	`)
	return script.Run(r.client,
		[]string{base.InProgressQueue, base.DefaultQueue},
		string(bytes)).Err()
}

// Schedule adds the task to the backlog queue to be processed in the future.
func (r *RDB) Schedule(msg *base.TaskMessage, processAt time.Time) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	score := float64(processAt.Unix())
	return r.client.ZAdd(base.ScheduledQueue,
		&redis.Z{Member: string(bytes), Score: score}).Err()
}

// Retry moves the task from in-progress to retry queue, incrementing retry count
// and assigning error message to the task message.
func (r *RDB) Retry(msg *base.TaskMessage, processAt time.Time, errMsg string) error {
	bytesToRemove, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	modified := *msg
	modified.Retried++
	modified.ErrorMsg = errMsg
	bytesToAdd, err := json.Marshal(&modified)
	if err != nil {
		return err
	}
	// KEYS[1] -> asynq:in_progress
	// KEYS[2] -> asynq:retry
	// KEYS[3] -> asynq:processed:<yyyy-mm-dd>
	// KEYS[4] -> asynq:failure:<yyyy-mm-dd>
	// ARGV[1] -> base.TaskMessage value to remove from base.InProgressQueue queue
	// ARGV[2] -> base.TaskMessage value to add to Retry queue
	// ARGV[3] -> retry_at UNIX timestamp
	// ARGV[4] -> stats expiration timestamp
	script := redis.NewScript(`
	redis.call("LREM", KEYS[1], 0, ARGV[1])
	redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
	local n = redis.call("INCR", KEYS[3])
	if tonumber(n) == 1 then
		redis.call("EXPIREAT", KEYS[3], ARGV[4])
	end
	local m = redis.call("INCR", KEYS[4])
	if tonumber(m) == 1 then
		redis.call("EXPIREAT", KEYS[4], ARGV[4])
	end
	return redis.status_reply("OK")
	`)
	now := time.Now()
	processedKey := base.ProcessedKey(now)
	failureKey := base.FailureKey(now)
	expireAt := now.Add(statsTTL)
	return script.Run(r.client,
		[]string{base.InProgressQueue, base.RetryQueue, processedKey, failureKey},
		string(bytesToRemove), string(bytesToAdd), processAt.Unix(), expireAt.Unix()).Err()
}

const (
	maxDeadTasks         = 10000
	deadExpirationInDays = 90
)

// Kill sends the task to "dead" queue from in-progress queue, assigning
// the error message to the task.
// It also trims the set by timestamp and set size.
func (r *RDB) Kill(msg *base.TaskMessage, errMsg string) error {
	bytesToRemove, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	modified := *msg
	modified.ErrorMsg = errMsg
	bytesToAdd, err := json.Marshal(&modified)
	if err != nil {
		return err
	}
	now := time.Now()
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	processedKey := base.ProcessedKey(now)
	failureKey := base.FailureKey(now)
	expireAt := now.Add(statsTTL)
	// KEYS[1] -> asynq:in_progress
	// KEYS[2] -> asynq:dead
	// KEYS[3] -> asynq:processed:<yyyy-mm-dd>
	// KEYS[4] -> asynq.failure:<yyyy-mm-dd>
	// ARGV[1] -> base.TaskMessage value to remove from base.InProgressQueue queue
	// ARGV[2] -> base.TaskMessage value to add to Dead queue
	// ARGV[3] -> died_at UNIX timestamp
	// ARGV[4] -> cutoff timestamp (e.g., 90 days ago)
	// ARGV[5] -> max number of tasks in dead queue (e.g., 100)
	// ARGV[6] -> stats expiration timestamp
	script := redis.NewScript(`
	redis.call("LREM", KEYS[1], 0, ARGV[1])
	redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
	redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[4])
	redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[5])
	local n = redis.call("INCR", KEYS[3])
	if tonumber(n) == 1 then
		redis.call("EXPIREAT", KEYS[3], ARGV[6])
	end
	local m = redis.call("INCR", KEYS[4])
	if tonumber(m) == 1 then
		redis.call("EXPIREAT", KEYS[4], ARGV[6])
	end
	return redis.status_reply("OK")
	`)
	return script.Run(r.client,
		[]string{base.InProgressQueue, base.DeadQueue, processedKey, failureKey},
		string(bytesToRemove), string(bytesToAdd), now.Unix(), limit, maxDeadTasks, expireAt.Unix()).Err()
}

// RestoreUnfinished  moves all tasks from in-progress list to the queue
// and reports the number of tasks restored.
func (r *RDB) RestoreUnfinished() (int64, error) {
	script := redis.NewScript(`
	local len = redis.call("LLEN", KEYS[1])
	for i = len, 1, -1 do
		redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
	end
	return len
	`)
	res, err := script.Run(r.client, []string{base.InProgressQueue, base.DefaultQueue}).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	return n, nil
}

// CheckAndEnqueue checks for all scheduled tasks and enqueues any tasks that
// have to be processed.
func (r *RDB) CheckAndEnqueue() error {
	delayed := []string{base.ScheduledQueue, base.RetryQueue}
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
		local decoded = cjson.decode(msg)
		local qkey = ARGV[2] .. decoded["Queue"]
		redis.call("LPUSH", qkey, msg)
	end
	return msgs
	`)
	now := float64(time.Now().Unix())
	return script.Run(r.client,
		[]string{from, base.DefaultQueue}, now, base.QueuePrefix).Err()
}
