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

	// ErrDuplicateTask indicates that another task with the same unique key holds the uniqueness lock.
	ErrDuplicateTask = errors.New("task already exists")
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

// KEYS[1] -> asynq:queues:<qname>
// KEYS[2] -> asynq:queues
// ARGV[1] -> task message data
var enqueueCmd = redis.NewScript(`
redis.call("LPUSH", KEYS[1], ARGV[1])
redis.call("SADD", KEYS[2], KEYS[1])
return 1`)

// Enqueue inserts the given task to the tail of the queue.
func (r *RDB) Enqueue(msg *base.TaskMessage) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	key := base.QueueKey(msg.Queue)
	return enqueueCmd.Run(r.client, []string{key, base.AllQueues}, encoded).Err()
}

// KEYS[1] -> unique key in the form <type>:<payload>:<qname>
// KEYS[2] -> asynq:queues:<qname>
// KEYS[2] -> asynq:queues
// ARGV[1] -> task ID
// ARGV[2] -> uniqueness lock TTL
// ARGV[3] -> task message data
var enqueueUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return 0
end
redis.call("LPUSH", KEYS[2], ARGV[3])
redis.call("SADD", KEYS[3], KEYS[2])
return 1
`)

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) EnqueueUnique(msg *base.TaskMessage, ttl time.Duration) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	key := base.QueueKey(msg.Queue)
	res, err := enqueueUniqueCmd.Run(r.client,
		[]string{msg.UniqueKey, key, base.AllQueues},
		msg.ID.String(), int(ttl.Seconds()), encoded).Result()
	if err != nil {
		return err
	}
	n, ok := res.(int64)
	if !ok {
		return fmt.Errorf("could not cast %v to int64", res)
	}
	if n == 0 {
		return ErrDuplicateTask
	}
	return nil
}

// Dequeue queries given queues in order and pops a task message
// off a queue if one exists and returns the message and deadline in Unix time in seconds.
// Dequeue skips a queue if the queue is paused.
// If all queues are empty, ErrNoProcessableTask error is returned.
func (r *RDB) Dequeue(qnames ...string) (msg *base.TaskMessage, deadline int, err error) {
	var qkeys []interface{}
	for _, q := range qnames {
		qkeys = append(qkeys, base.QueueKey(q))
	}
	data, deadline, err := r.dequeue(qkeys...)
	if err == redis.Nil {
		return nil, 0, ErrNoProcessableTask
	}
	if err != nil {
		return nil, 0, err
	}
	if msg, err = base.DecodeMessage(data); err != nil {
		return nil, 0, err
	}
	return msg, deadline, nil
}

// KEYS[1]  -> asynq:in_progress
// KEYS[2]  -> asynq:paused
// KEYS[3]  -> asynq:deadlines
// ARGV[1]  -> current time in Unix time
// ARGV[2:] -> List of queues to query in order
//
// dequeueCmd checks whether a queue is paused first, before
// calling RPOPLPUSH to pop a task from the queue.
// It computes the task deadline by inspecting Timout and Deadline fields,
// and inserts the task with deadlines set.
var dequeueCmd = redis.NewScript(`
for i = 2, table.getn(ARGV) do
	local qkey = ARGV[i]
	if redis.call("SISMEMBER", KEYS[2], qkey) == 0 then
		local msg = redis.call("RPOPLPUSH", qkey, KEYS[1])
		if msg then
			local decoded = cjson.decode(msg)
			local timeout = decoded["Timeout"]
			local deadline = decoded["Deadline"]
			local score
			if timeout ~= 0 and deadline ~= 0 then
				score = math.min(ARGV[1]+timeout, deadline)
			elseif timeout ~= 0 then
				score = ARGV[1] + timeout
			elseif deadline ~= 0 then
			    score = deadline
			else
				return redis.error_reply("asynq internal error: both timeout and deadline are not set")
			end
			redis.call("ZADD", KEYS[3], score, msg)
			return {msg, score}
		end
	end
end
return nil`)

func (r *RDB) dequeue(qkeys ...interface{}) (msgjson string, deadline int, err error) {
	var args []interface{}
	args = append(args, time.Now().Unix())
	args = append(args, qkeys...)
	res, err := dequeueCmd.Run(r.client,
		[]string{base.InProgressQueue, base.PausedQueues, base.KeyDeadlines}, args...).Result()
	if err != nil {
		return "", 0, err
	}
	data, err := cast.ToSliceE(res)
	if err != nil {
		return "", 0, err
	}
	if len(data) != 2 {
		return "", 0, fmt.Errorf("asynq: internal error: dequeue command returned %v values", len(data))
	}
	if msgjson, err = cast.ToStringE(data[0]); err != nil {
		return "", 0, err
	}
	if deadline, err = cast.ToIntE(data[1]); err != nil {
		return "", 0, err
	}
	return msgjson, deadline, nil
}

// KEYS[1] -> asynq:in_progress
// KEYS[2] -> asynq:processed:<yyyy-mm-dd>
// KEYS[3] -> unique key in the format <type>:<payload>:<qname>
// ARGV[1] -> base.TaskMessage value
// ARGV[2] -> stats expiration timestamp
// ARGV[3] -> task ID
// Note: LREM count ZERO means "remove all elements equal to val"
var doneCmd = redis.NewScript(`
local x = redis.call("LREM", KEYS[1], 0, ARGV[1]) 
if x == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[2])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[2], ARGV[2])
end
if string.len(KEYS[3]) > 0 and redis.call("GET", KEYS[3]) == ARGV[3] then
  redis.call("DEL", KEYS[3])
end
return redis.status_reply("OK")
`)

// Done removes the task from in-progress queue to mark the task as done.
// It removes a uniqueness lock acquired by the task, if any.
func (r *RDB) Done(msg *base.TaskMessage) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	now := time.Now()
	processedKey := base.ProcessedKey(now)
	expireAt := now.Add(statsTTL)
	return doneCmd.Run(r.client,
		[]string{base.InProgressQueue, processedKey, msg.UniqueKey},
		encoded, expireAt.Unix(), msg.ID.String()).Err()
}

// KEYS[1] -> asynq:in_progress
// KEYS[2] -> asynq:queues:<qname>
// ARGV[1] -> base.TaskMessage value
// Note: Use RPUSH to push to the head of the queue.
var requeueCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("RPUSH", KEYS[2], ARGV[1])
return redis.status_reply("OK")`)

// Requeue moves the task from in-progress queue to the specified queue.
func (r *RDB) Requeue(msg *base.TaskMessage) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	return requeueCmd.Run(r.client,
		[]string{base.InProgressQueue, base.QueueKey(msg.Queue)},
		encoded).Err()
}

// KEYS[1] -> asynq:scheduled
// KEYS[2] -> asynq:queues
// ARGV[1] -> score (process_at timestamp)
// ARGV[2] -> task message
// ARGV[3] -> queue key
var scheduleCmd = redis.NewScript(`
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
redis.call("SADD", KEYS[2], ARGV[3])
return 1
`)

// Schedule adds the task to the backlog queue to be processed in the future.
func (r *RDB) Schedule(msg *base.TaskMessage, processAt time.Time) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	qkey := base.QueueKey(msg.Queue)
	score := float64(processAt.Unix())
	return scheduleCmd.Run(r.client,
		[]string{base.ScheduledQueue, base.AllQueues},
		score, encoded, qkey).Err()
}

// KEYS[1] -> unique key in the format <type>:<payload>:<qname>
// KEYS[2] -> asynq:scheduled
// KEYS[3] -> asynq:queues
// ARGV[1] -> task ID
// ARGV[2] -> uniqueness lock TTL
// ARGV[3] -> score (process_at timestamp)
// ARGV[4] -> task message
// ARGV[5] -> queue key
var scheduleUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return 0
end
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
redis.call("SADD", KEYS[3], ARGV[5])
return 1
`)

// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) ScheduleUnique(msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	qkey := base.QueueKey(msg.Queue)
	score := float64(processAt.Unix())
	res, err := scheduleUniqueCmd.Run(r.client,
		[]string{msg.UniqueKey, base.ScheduledQueue, base.AllQueues},
		msg.ID.String(), int(ttl.Seconds()), score, encoded, qkey).Result()
	if err != nil {
		return err
	}
	n, ok := res.(int64)
	if !ok {
		return fmt.Errorf("could not cast %v to int64", res)
	}
	if n == 0 {
		return ErrDuplicateTask
	}
	return nil
}

// KEYS[1] -> asynq:in_progress
// KEYS[2] -> asynq:retry
// KEYS[3] -> asynq:processed:<yyyy-mm-dd>
// KEYS[4] -> asynq:failure:<yyyy-mm-dd>
// ARGV[1] -> base.TaskMessage value to remove from base.InProgressQueue queue
// ARGV[2] -> base.TaskMessage value to add to Retry queue
// ARGV[3] -> retry_at UNIX timestamp
// ARGV[4] -> stats expiration timestamp
var retryCmd = redis.NewScript(`
local x = redis.call("LREM", KEYS[1], 0, ARGV[1])
if x == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
local n = redis.call("INCR", KEYS[3])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[3], ARGV[4])
end
local m = redis.call("INCR", KEYS[4])
if tonumber(m) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[4])
end
return redis.status_reply("OK")`)

// Retry moves the task from in-progress to retry queue, incrementing retry count
// and assigning error message to the task message.
func (r *RDB) Retry(msg *base.TaskMessage, processAt time.Time, errMsg string) error {
	msgToRemove, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	modified := *msg
	modified.Retried++
	modified.ErrorMsg = errMsg
	msgToAdd, err := base.EncodeMessage(&modified)
	if err != nil {
		return err
	}
	now := time.Now()
	processedKey := base.ProcessedKey(now)
	failureKey := base.FailureKey(now)
	expireAt := now.Add(statsTTL)
	return retryCmd.Run(r.client,
		[]string{base.InProgressQueue, base.RetryQueue, processedKey, failureKey},
		msgToRemove, msgToAdd, processAt.Unix(), expireAt.Unix()).Err()
}

const (
	maxDeadTasks         = 10000
	deadExpirationInDays = 90
)

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
var killCmd = redis.NewScript(`
local x = redis.call("LREM", KEYS[1], 0, ARGV[1])
if x == 0 then
  return redis.error_reply("NOT FOUND")
end
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
return redis.status_reply("OK")`)

// Kill sends the task to "dead" queue from in-progress queue, assigning
// the error message to the task.
// It also trims the set by timestamp and set size.
func (r *RDB) Kill(msg *base.TaskMessage, errMsg string) error {
	msgToRemove, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	modified := *msg
	modified.ErrorMsg = errMsg
	msgToAdd, err := base.EncodeMessage(&modified)
	if err != nil {
		return err
	}
	now := time.Now()
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	processedKey := base.ProcessedKey(now)
	failureKey := base.FailureKey(now)
	expireAt := now.Add(statsTTL)
	return killCmd.Run(r.client,
		[]string{base.InProgressQueue, base.DeadQueue, processedKey, failureKey},
		msgToRemove, msgToAdd, now.Unix(), limit, maxDeadTasks, expireAt.Unix()).Err()
}

// CheckAndEnqueue checks for all scheduled/retry tasks and enqueues any tasks that
// are ready to be processed.
func (r *RDB) CheckAndEnqueue() (err error) {
	delayed := []string{base.ScheduledQueue, base.RetryQueue}
	for _, zset := range delayed {
		n := 1
		for n != 0 {
			n, err = r.forward(zset)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// KEYS[1] -> source queue (e.g. scheduled or retry queue)
// ARGV[1] -> current unix time
// ARGV[2] -> queue prefix
// Note: Script moves tasks up to 100 at a time to keep the runtime of script short.
var forwardCmd = redis.NewScript(`
local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, msg in ipairs(msgs) do
	local decoded = cjson.decode(msg)
	local qkey = ARGV[2] .. decoded["Queue"]
	redis.call("LPUSH", qkey, msg)
	redis.call("ZREM", KEYS[1], msg)
end
return table.getn(msgs)`)

// forward moves tasks with a score less than the current unix time
// from the src zset. It returns the number of tasks moved.
func (r *RDB) forward(src string) (int, error) {
	now := float64(time.Now().Unix())
	res, err := forwardCmd.Run(r.client,
		[]string{src}, now, base.QueuePrefix).Result()
	if err != nil {
		return 0, err
	}
	return cast.ToInt(res), nil
}

// KEYS[1]  -> asynq:servers:<host:pid:sid>
// KEYS[2]  -> asynq:servers
// KEYS[3]  -> asynq:workers<host:pid:sid>
// KEYS[4]  -> asynq:workers
// ARGV[1]  -> expiration time
// ARGV[2]  -> TTL in seconds
// ARGV[3]  -> server info
// ARGV[4:] -> alternate key-value pair of (worker id, worker data)
// Note: Add key to ZSET with expiration time as score.
// ref: https://github.com/antirez/redis/issues/135#issuecomment-2361996
var writeServerStateCmd = redis.NewScript(`
redis.call("SETEX", KEYS[1], ARGV[2], ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[1], KEYS[1])
redis.call("DEL", KEYS[3])
for i = 4, table.getn(ARGV)-1, 2 do
	redis.call("HSET", KEYS[3], ARGV[i], ARGV[i+1])
end
redis.call("EXPIRE", KEYS[3], ARGV[2])
redis.call("ZADD", KEYS[4], ARGV[1], KEYS[3])
return redis.status_reply("OK")`)

// WriteServerState writes server state data to redis with expiration set to the value ttl.
func (r *RDB) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	exp := time.Now().Add(ttl).UTC()
	args := []interface{}{float64(exp.Unix()), ttl.Seconds(), bytes} // args to the lua script
	for _, w := range workers {
		bytes, err := json.Marshal(w)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, w.ID, bytes)
	}
	skey := base.ServerInfoKey(info.Host, info.PID, info.ServerID)
	wkey := base.WorkersKey(info.Host, info.PID, info.ServerID)
	return writeServerStateCmd.Run(r.client,
		[]string{skey, base.AllServers, wkey, base.AllWorkers},
		args...).Err()
}

// KEYS[1] -> asynq:servers
// KEYS[2] -> asynq:servers:<host:pid:sid>
// KEYS[3] -> asynq:workers
// KEYS[4] -> asynq:workers<host:pid:sid>
var clearProcessInfoCmd = redis.NewScript(`
redis.call("ZREM", KEYS[1], KEYS[2])
redis.call("DEL", KEYS[2])
redis.call("ZREM", KEYS[3], KEYS[4])
redis.call("DEL", KEYS[4])
return redis.status_reply("OK")`)

// ClearServerState deletes server state data from redis.
func (r *RDB) ClearServerState(host string, pid int, serverID string) error {
	skey := base.ServerInfoKey(host, pid, serverID)
	wkey := base.WorkersKey(host, pid, serverID)
	return clearProcessInfoCmd.Run(r.client,
		[]string{base.AllServers, skey, base.AllWorkers, wkey}).Err()
}

// CancelationPubSub returns a pubsub for cancelation messages.
func (r *RDB) CancelationPubSub() (*redis.PubSub, error) {
	pubsub := r.client.Subscribe(base.CancelChannel)
	_, err := pubsub.Receive()
	if err != nil {
		return nil, err
	}
	return pubsub, nil
}

// PublishCancelation publish cancelation message to all subscribers.
// The message is the ID for the task to be canceled.
func (r *RDB) PublishCancelation(id string) error {
	return r.client.Publish(base.CancelChannel, id).Err()
}
