// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package rdb encapsulates the interactions with redis.
package rdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
	client redis.UniversalClient
}

// NewRDB returns a new instance of RDB.
func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{client}
}

// Close closes the connection with redis server.
func (r *RDB) Close() error {
	return r.client.Close()
}

// Ping checks the connection with redis server.
func (r *RDB) Ping() error {
	return r.client.Ping().Err()
}

// Enqueue inserts the given task to the tail of the queue.
func (r *RDB) Enqueue(msg *base.TaskMessage) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return err
	}
	key := base.QueueKey(msg.Queue)
	return r.client.LPush(key, encoded).Err()
}

// KEYS[1] -> unique key
// KEYS[2] -> asynq:{<qname>}
// ARGV[1] -> task ID
// ARGV[2] -> uniqueness lock TTL
// ARGV[3] -> task message data
var enqueueUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return 0
end
redis.call("LPUSH", KEYS[2], ARGV[3])
return 1
`)

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) EnqueueUnique(msg *base.TaskMessage, ttl time.Duration) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return err
	}
	res, err := enqueueUniqueCmd.Run(r.client,
		[]string{msg.UniqueKey, base.QueueKey(msg.Queue)},
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
// off a queue if one exists and returns the message and deadline.
// Dequeue skips a queue if the queue is paused.
// If all queues are empty, ErrNoProcessableTask error is returned.
func (r *RDB) Dequeue(qnames ...string) (msg *base.TaskMessage, deadline time.Time, err error) {
	data, d, err := r.dequeue(qnames...)
	if err != nil {
		return nil, time.Time{}, err
	}
	if msg, err = base.DecodeMessage(data); err != nil {
		return nil, time.Time{}, err
	}
	return msg, time.Unix(d, 0), nil
}

// KEYS[1] -> asynq:{<qname>}
// KEYS[2] -> asynq:{<qname>}:paused
// KEYS[3] -> asynq:{<qname>}:in_progress
// KEYS[4] -> asynq:{<qname>}:deadlines
// ARGV[1]  -> current time in Unix time
//
// dequeueCmd checks whether a queue is paused first, before
// calling RPOPLPUSH to pop a task from the queue.
// It computes the task deadline by inspecting Timout and Deadline fields,
// and inserts the task with deadlines set.
var dequeueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 0 then
	local msg = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
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
		redis.call("ZADD", KEYS[4], score, msg)
		return {msg, score}
	end
end
return nil`)

func (r *RDB) dequeue(qnames ...string) (msgjson string, deadline int64, err error) {
	for _, qname := range qnames {
		keys := []string{
			base.QueueKey(qname),
			base.PausedKey(qname),
			base.InProgressKey(qname),
			base.DeadlinesKey(qname),
		}
		res, err := dequeueCmd.Run(r.client, keys, time.Now().Unix()).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			return "", 0, err
		}
		data, err := cast.ToSliceE(res)
		if err != nil {
			return "", 0, err
		}
		if len(data) != 2 {
			return "", 0, fmt.Errorf("asynq: internal error: dequeue command returned %d values", len(data))
		}
		if msgjson, err = cast.ToStringE(data[0]); err != nil {
			return "", 0, err
		}
		if deadline, err = cast.ToInt64E(data[1]); err != nil {
			return "", 0, err
		}
		return msgjson, deadline, nil
	}
	return "", 0, ErrNoProcessableTask
}

// KEYS[1] -> asynq:{<qname>}:in_progress
// KEYS[2] -> asynq:{<qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// ARGV[1] -> base.TaskMessage value
// ARGV[2] -> stats expiration timestamp
var doneCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[3])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[3], ARGV[2])
end
return redis.status_reply("OK")
`)

// KEYS[1] -> asynq:{<qname>}:in_progress
// KEYS[2] -> asynq:{<qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[4] -> unique key
// ARGV[1] -> base.TaskMessage value
// ARGV[2] -> stats expiration timestamp
// ARGV[3] -> task ID
var doneUniqueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[3])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[3], ARGV[2])
end
if redis.call("GET", KEYS[4]) == ARGV[3] then
  redis.call("DEL", KEYS[4])
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
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.InProgressKey(msg.Queue),
		base.DeadlinesKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
	}
	args := []interface{}{encoded, expireAt.Unix()}
	if len(msg.UniqueKey) > 0 {
		keys = append(keys, msg.UniqueKey)
		args = append(args, msg.ID.String())
		return doneUniqueCmd.Run(r.client, keys, args...).Err()
	}
	return doneCmd.Run(r.client, keys, args...).Err()
}

// KEYS[1] -> asynq:{<qname>}:in_progress
// KEYS[2] -> asynq:{<qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}
// ARGV[1] -> base.TaskMessage value
// Note: Use RPUSH to push to the head of the queue.
var requeueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("RPUSH", KEYS[3], ARGV[1])
return redis.status_reply("OK")`)

// Requeue moves the task from in-progress queue to the specified queue.
func (r *RDB) Requeue(msg *base.TaskMessage) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	return requeueCmd.Run(r.client,
		[]string{base.InProgressKey(msg.Queue), base.DeadlinesKey(msg.Queue), base.QueueKey(msg.Queue)},
		encoded).Err()
}

// Schedule adds the task to the backlog queue to be processed in the future.
func (r *RDB) Schedule(msg *base.TaskMessage, processAt time.Time) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return err
	}
	score := float64(processAt.Unix())
	return r.client.ZAdd(base.ScheduledKey(msg.Queue), &redis.Z{Score: score, Member: encoded}).Err()
}

// KEYS[1] -> unique key
// KEYS[2] -> asynq:{<qname>}:scheduled
// ARGV[1] -> task ID
// ARGV[2] -> uniqueness lock TTL
// ARGV[3] -> score (process_at timestamp)
// ARGV[4] -> task message
var scheduleUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return 0
end
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
return 1
`)

// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) ScheduleUnique(msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return err
	}
	score := float64(processAt.Unix())
	res, err := scheduleUniqueCmd.Run(r.client,
		[]string{msg.UniqueKey, base.ScheduledKey(msg.Queue)},
		msg.ID.String(), int(ttl.Seconds()), score, encoded).Result()
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

// KEYS[1] -> asynq:{<qname>}:in_progress
// KEYS[2] -> asynq:{<qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}:retry
// KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[5] -> asynq:{<qname>}:failed:<yyyy-mm-dd>
// ARGV[1] -> base.TaskMessage value to remove from base.InProgressQueue queue
// ARGV[2] -> base.TaskMessage value to add to Retry queue
// ARGV[3] -> retry_at UNIX timestamp
// ARGV[4] -> stats expiration timestamp
var retryCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[2])
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[4])
end
local m = redis.call("INCR", KEYS[5])
if tonumber(m) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[4])
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
	processedKey := base.ProcessedKey(msg.Queue, now)
	failedKey := base.FailedKey(msg.Queue, now)
	expireAt := now.Add(statsTTL)
	return retryCmd.Run(r.client,
		[]string{base.InProgressKey(msg.Queue), base.DeadlinesKey(msg.Queue), base.RetryKey(msg.Queue), processedKey, failedKey},
		msgToRemove, msgToAdd, processAt.Unix(), expireAt.Unix()).Err()
}

const (
	maxDeadTasks         = 10000
	deadExpirationInDays = 90
)

// KEYS[1] -> asynq:{<qname>}:in_progress
// KEYS[2] -> asynq:{<qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}:dead
// KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[5] -> asynq:{<qname>}:failed:<yyyy-mm-dd>
// ARGV[1] -> base.TaskMessage value to remove from base.InProgressQueue queue
// ARGV[2] -> base.TaskMessage value to add to Dead queue
// ARGV[3] -> died_at UNIX timestamp
// ARGV[4] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[5] -> max number of tasks in dead queue (e.g., 100)
// ARGV[6] -> stats expiration timestamp
var killCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[2])
redis.call("ZREMRANGEBYSCORE", KEYS[3], "-inf", ARGV[4])
redis.call("ZREMRANGEBYRANK", KEYS[3], 0, -ARGV[5])
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[6])
end
local m = redis.call("INCR", KEYS[5])
if tonumber(m) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[6])
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
	processedKey := base.ProcessedKey(msg.Queue, now)
	failedKey := base.FailedKey(msg.Queue, now)
	expireAt := now.Add(statsTTL)
	return killCmd.Run(r.client,
		[]string{base.InProgressKey(msg.Queue), base.DeadlinesKey(msg.Queue), base.DeadKey(msg.Queue), processedKey, failedKey},
		msgToRemove, msgToAdd, now.Unix(), limit, maxDeadTasks, expireAt.Unix()).Err()
}

// CheckAndEnqueue checks for scheduled/retry tasks for the given queues
//and enqueues any tasks that  are ready to be processed.
func (r *RDB) CheckAndEnqueue(qnames ...string) error {
	for _, qname := range qnames {
		if err := r.forwardAll(base.ScheduledKey(qname), base.QueueKey(qname)); err != nil {
			return err
		}
		if err := r.forwardAll(base.RetryKey(qname), base.QueueKey(qname)); err != nil {
			return err
		}
	}
	return nil
}

// KEYS[1] -> source queue (e.g. asynq:{<qname>:scheduled or asynq:{<qname>}:retry})
// KEYS[2] -> destination queue (e.g. asynq:{<qname>})
// ARGV[1] -> current unix time
// Note: Script moves tasks up to 100 at a time to keep the runtime of script short.
var forwardCmd = redis.NewScript(`
local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, msg in ipairs(msgs) do
	redis.call("LPUSH", KEYS[2], msg)
	redis.call("ZREM", KEYS[1], msg)
end
return table.getn(msgs)`)

// forward moves tasks with a score less than the current unix time
// from the src zset to the dst list. It returns the number of tasks moved.
func (r *RDB) forward(src, dst string) (int, error) {
	now := float64(time.Now().Unix())
	res, err := forwardCmd.Run(r.client, []string{src, dst}, now).Result()
	if err != nil {
		return 0, err
	}
	return cast.ToInt(res), nil
}

// forwardAll moves tasks with a score less than the current unix time from the src zset,
// until there's no more tasks.
func (r *RDB) forwardAll(src, dst string) (err error) {
	n := 1
	for n != 0 {
		n, err = r.forward(src, dst)
		if err != nil {
			return err
		}
	}
	return nil
}

// ListDeadlineExceeded returns a list of task messages that have exceeded the deadline from the given queues.
func (r *RDB) ListDeadlineExceeded(deadline time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	var msgs []*base.TaskMessage
	opt := &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(deadline.Unix(), 10),
	}
	for _, qname := range qnames {
		res, err := r.client.ZRangeByScore(base.DeadlinesKey(qname), opt).Result()
		if err != nil {
			return nil, err
		}
		for _, s := range res {
			msg, err := base.DecodeMessage(s)
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, msg)
		}
	}
	return msgs, nil
}

// KEYS[1]  -> asynq:servers:{<host:pid:sid>}
// KEYS[2]  -> asynq:workers:{<host:pid:sid>}
// ARGV[1]  -> TTL in seconds
// ARGV[2]  -> server info
// ARGV[3:] -> alternate key-value pair of (worker id, worker data)
// Note: Add key to ZSET with expiration time as score.
// ref: https://github.com/antirez/redis/issues/135#issuecomment-2361996
var writeServerStateCmd = redis.NewScript(`
redis.call("SETEX", KEYS[1], ARGV[1], ARGV[2])
redis.call("DEL", KEYS[2])
for i = 3, table.getn(ARGV)-1, 2 do
	redis.call("HSET", KEYS[2], ARGV[i], ARGV[i+1])
end
redis.call("EXPIRE", KEYS[2], ARGV[1])
return redis.status_reply("OK")`)

// WriteServerState writes server state data to redis with expiration set to the value ttl.
func (r *RDB) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	exp := time.Now().Add(ttl).UTC()
	args := []interface{}{ttl.Seconds(), bytes} // args to the lua script
	for _, w := range workers {
		bytes, err := json.Marshal(w)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, w.ID, bytes)
	}
	skey := base.ServerInfoKey(info.Host, info.PID, info.ServerID)
	wkey := base.WorkersKey(info.Host, info.PID, info.ServerID)
	if err := r.client.ZAdd(base.AllServers, &redis.Z{Score: float64(exp.Unix()), Member: skey}).Err(); err != nil {
		return err
	}
	if err := r.client.ZAdd(base.AllWorkers, &redis.Z{Score: float64(exp.Unix()), Member: wkey}).Err(); err != nil {
		return err
	}
	return writeServerStateCmd.Run(r.client, []string{skey, wkey}, args...).Err()
}

// KEYS[1] -> asynq:servers:{<host:pid:sid>}
// KEYS[2] -> asynq:workers:{<host:pid:sid>}
var clearServerStateCmd = redis.NewScript(`
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
return redis.status_reply("OK")`)

// ClearServerState deletes server state data from redis.
func (r *RDB) ClearServerState(host string, pid int, serverID string) error {
	skey := base.ServerInfoKey(host, pid, serverID)
	wkey := base.WorkersKey(host, pid, serverID)
	if err := r.client.ZRem(base.AllServers, skey).Err(); err != nil {
		return err
	}
	if err := r.client.ZRem(base.AllWorkers, wkey).Err(); err != nil {
		return err
	}
	return clearServerStateCmd.Run(r.client, []string{skey, wkey}).Err()
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
