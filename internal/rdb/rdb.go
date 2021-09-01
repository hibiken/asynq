// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package rdb encapsulates the interactions with redis.
package rdb

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/spf13/cast"
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

// Client returns the reference to underlying redis client.
func (r *RDB) Client() redis.UniversalClient {
	return r.client
}

// Ping checks the connection with redis server.
func (r *RDB) Ping() error {
	return r.client.Ping().Err()
}

func (r *RDB) runScript(op errors.Op, script *redis.Script, keys []string, args ...interface{}) error {
	if err := script.Run(r.client, keys, args...).Err(); err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	return nil
}

// enqueueCmd enqueues a given task message.
//
// Input:
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:pending
// --
// ARGV[1] -> task message data
// ARGV[2] -> task ID
// ARGV[3] -> task timeout in seconds (0 if not timeout)
// ARGV[4] -> task deadline in unix time (0 if no deadline)
//
// Output:
// Returns 1 if successfully enqueued
var enqueueCmd = redis.NewScript(`
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "pending",
           "timeout", ARGV[3],
           "deadline", ARGV[4])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
`)

// Enqueue adds the given task to the pending list of the queue.
func (r *RDB) Enqueue(msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Enqueue"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID.String()),
		base.PendingKey(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		msg.ID.String(),
		msg.Timeout,
		msg.Deadline,
	}
	return r.runScript(op, enqueueCmd, keys, argv...)
}

// enqueueUniqueCmd enqueues the task message if the task is unique.
//
// KEYS[1] -> unique key
// KEYS[2] -> asynq:{<qname>}:t:<taskid>
// KEYS[3] -> asynq:{<qname>}:pending
// --
// ARGV[1] -> task ID
// ARGV[2] -> uniqueness lock TTL
// ARGV[3] -> task message data
// ARGV[4] -> task timeout in seconds (0 if not timeout)
// ARGV[5] -> task deadline in unix time (0 if no deadline)
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task already exists
var enqueueUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[3],
           "state", "pending",
           "timeout", ARGV[4],
           "deadline", ARGV[5],
           "unique_key", KEYS[1])
redis.call("LPUSH", KEYS[3], ARGV[1])
return 1
`)

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) EnqueueUnique(msg *base.TaskMessage, ttl time.Duration) error {
	var op errors.Op = "rdb.EnqueueUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, "cannot encode task message: %v", err)
	}
	if err := r.client.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		msg.UniqueKey,
		base.TaskKey(msg.Queue, msg.ID.String()),
		base.PendingKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID.String(),
		int(ttl.Seconds()),
		encoded,
		msg.Timeout,
		msg.Deadline,
	}
	res, err := enqueueUniqueCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}
	n, ok := res.(int64)
	if !ok {
		return errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	return nil
}

// Input:
// KEYS[1] -> asynq:{<qname>}:pending
// KEYS[2] -> asynq:{<qname>}:paused
// KEYS[3] -> asynq:{<qname>}:active
// KEYS[4] -> asynq:{<qname>}:deadlines
// --
// ARGV[1] -> current time in Unix time
// ARGV[2] -> task key prefix
//
// Output:
// Returns nil if no processable task is found in the given queue.
// Returns tuple {msg , deadline} if task is found, where `msg` is the encoded
// TaskMessage, and `deadline` is Unix time in seconds.
//
// Note: dequeueCmd checks whether a queue is paused first, before
// calling RPOPLPUSH to pop a task from the queue.
// It computes the task deadline by inspecting Timout and Deadline fields,
// and inserts the task to the deadlines zset with the computed deadline.
var dequeueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 0 then
	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	if id then
		local key = ARGV[2] .. id
		redis.call("HSET", key, "state", "active")
		local data = redis.call("HMGET", key, "msg", "timeout", "deadline")
		local msg = data[1]	
		local timeout = tonumber(data[2])
		local deadline = tonumber(data[3])
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
		redis.call("ZADD", KEYS[4], score, id)
		return {msg, score}
	end
end
return nil`)

// Dequeue queries given queues in order and pops a task message
// off a queue if one exists and returns the message and deadline.
// Dequeue skips a queue if the queue is paused.
// If all queues are empty, ErrNoProcessableTask error is returned.
func (r *RDB) Dequeue(qnames ...string) (msg *base.TaskMessage, deadline time.Time, err error) {
	var op errors.Op = "rdb.Dequeue"
	for _, qname := range qnames {
		keys := []string{
			base.PendingKey(qname),
			base.PausedKey(qname),
			base.ActiveKey(qname),
			base.DeadlinesKey(qname),
		}
		argv := []interface{}{
			time.Now().Unix(),
			base.TaskKeyPrefix(qname),
		}
		res, err := dequeueCmd.Run(r.client, keys, argv...).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
		}
		data, err := cast.ToSliceE(res)
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
		}
		if len(data) != 2 {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("Lua script returned %d values; expected 2", len(data)))
		}
		encoded, err := cast.ToStringE(data[0])
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
		}
		d, err := cast.ToInt64E(data[1])
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
		}
		if msg, err = base.DecodeMessage([]byte(encoded)); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		}
		return msg, time.Unix(d, 0), nil
	}
	return nil, time.Time{}, errors.E(op, errors.NotFound, errors.ErrNoProcessableTask)
}

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}:t:<task_id>
// KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// ARGV[1] -> task ID
// ARGV[2] -> stats expiration timestamp
var doneCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("DEL", KEYS[3]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[2])
end
return redis.status_reply("OK")
`)

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}:t:<task_id>
// KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[5] -> unique key
// ARGV[1] -> task ID
// ARGV[2] -> stats expiration timestamp
var doneUniqueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("DEL", KEYS[3]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[2])
end
if redis.call("GET", KEYS[5]) == ARGV[1] then
  redis.call("DEL", KEYS[5])
end
return redis.status_reply("OK")
`)

// Done removes the task from active queue to mark the task as done.
// It removes a uniqueness lock acquired by the task, if any.
func (r *RDB) Done(msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Done"
	now := time.Now()
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.DeadlinesKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID.String()),
		base.ProcessedKey(msg.Queue, now),
	}
	argv := []interface{}{
		msg.ID.String(),
		expireAt.Unix(),
	}
	if len(msg.UniqueKey) > 0 {
		keys = append(keys, msg.UniqueKey)
		return r.runScript(op, doneUniqueCmd, keys, argv...)
	}
	return r.runScript(op, doneCmd, keys, argv...)
}

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:deadlines
// KEYS[3] -> asynq:{<qname>}:pending
// KEYS[4] -> asynq:{<qname>}:t:<task_id>
// ARGV[1] -> task ID
// Note: Use RPUSH to push to the head of the queue.
var requeueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("RPUSH", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[4], "state", "pending")
return redis.status_reply("OK")`)

// Requeue moves the task from active queue to the specified queue.
func (r *RDB) Requeue(msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Requeue"
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.DeadlinesKey(msg.Queue),
		base.PendingKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID.String()),
	}
	return r.runScript(op, requeueCmd, keys, msg.ID.String())
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:scheduled
// ARGV[1] -> task message data
// ARGV[2] -> process_at time in Unix time
// ARGV[3] -> task ID
// ARGV[4] -> task timeout in seconds (0 if not timeout)
// ARGV[5] -> task deadline in unix time (0 if no deadline)
var scheduleCmd = redis.NewScript(`
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "scheduled",
           "timeout", ARGV[4],
           "deadline", ARGV[5])
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
`)

// Schedule adds the task to the scheduled set to be processed in the future.
func (r *RDB) Schedule(msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "rdb.Schedule"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID.String()),
		base.ScheduledKey(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		processAt.Unix(),
		msg.ID.String(),
		msg.Timeout,
		msg.Deadline,
	}
	return r.runScript(op, scheduleCmd, keys, argv...)
}

// KEYS[1] -> unique key
// KEYS[2] -> asynq:{<qname>}:t:<task_id>
// KEYS[3] -> asynq:{<qname>}:scheduled
// ARGV[1] -> task ID
// ARGV[2] -> uniqueness lock TTL
// ARGV[3] -> score (process_at timestamp)
// ARGV[4] -> task message
// ARGV[5] -> task timeout in seconds (0 if not timeout)
// ARGV[6] -> task deadline in unix time (0 if no deadline)
var scheduleUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[4],
           "state", "scheduled",
           "timeout", ARGV[5],
           "deadline", ARGV[6],
           "unique_key", KEYS[1])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
return 1
`)

// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) ScheduleUnique(msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	var op errors.Op = "rdb.ScheduleUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode task message: %v", err))
	}
	if err := r.client.SAdd(base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		msg.UniqueKey,
		base.TaskKey(msg.Queue, msg.ID.String()),
		base.ScheduledKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID.String(),
		int(ttl.Seconds()),
		processAt.Unix(),
		encoded,
		msg.Timeout,
		msg.Deadline,
	}
	res, err := scheduleUniqueCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}
	n, ok := res.(int64)
	if !ok {
		return errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	return nil
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:deadlines
// KEYS[4] -> asynq:{<qname>}:retry
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[6] -> asynq:{<qname>}:failed:<yyyy-mm-dd>
// ARGV[1] -> task ID
// ARGV[2] -> updated base.TaskMessage value
// ARGV[3] -> retry_at UNIX timestamp
// ARGV[4] -> stats expiration timestamp
// ARGV[5] -> is_failure (bool)
var retryCmd = redis.NewScript(`
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "retry")
if tonumber(ARGV[5]) == 1 then
	local n = redis.call("INCR", KEYS[5])
	if tonumber(n) == 1 then
		redis.call("EXPIREAT", KEYS[5], ARGV[4])
	end
	local m = redis.call("INCR", KEYS[6])
	if tonumber(m) == 1 then
		redis.call("EXPIREAT", KEYS[6], ARGV[4])
	end
end
return redis.status_reply("OK")`)

// Retry moves the task from active to retry queue.
// It also annotates the message with the given error message and
// if isFailure is true increments the retried counter.
func (r *RDB) Retry(msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	var op errors.Op = "rdb.Retry"
	now := time.Now()
	modified := *msg
	if isFailure {
		modified.Retried++
	}
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID.String()),
		base.ActiveKey(msg.Queue),
		base.DeadlinesKey(msg.Queue),
		base.RetryKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
		base.FailedKey(msg.Queue, now),
	}
	argv := []interface{}{
		msg.ID.String(),
		encoded,
		processAt.Unix(),
		expireAt.Unix(),
		isFailure,
	}
	return r.runScript(op, retryCmd, keys, argv...)
}

const (
	maxArchiveSize           = 10000 // maximum number of tasks in archive
	archivedExpirationInDays = 90    // number of days before an archived task gets deleted permanently
)

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:deadlines
// KEYS[4] -> asynq:{<qname>}:archived
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[6] -> asynq:{<qname>}:failed:<yyyy-mm-dd>
// ARGV[1] -> task ID
// ARGV[2] -> updated base.TaskMessage value
// ARGV[3] -> died_at UNIX timestamp
// ARGV[4] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[5] -> max number of tasks in archive (e.g., 100)
// ARGV[6] -> stats expiration timestamp
var archiveCmd = redis.NewScript(`
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", ARGV[4])
redis.call("ZREMRANGEBYRANK", KEYS[4], 0, -ARGV[5])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "archived")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[6])
end
local m = redis.call("INCR", KEYS[6])
if tonumber(m) == 1 then
	redis.call("EXPIREAT", KEYS[6], ARGV[6])
end
return redis.status_reply("OK")`)

// Archive sends the given task to archive, attaching the error message to the task.
// It also trims the archive by timestamp and set size.
func (r *RDB) Archive(msg *base.TaskMessage, errMsg string) error {
	var op errors.Op = "rdb.Archive"
	now := time.Now()
	modified := *msg
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	cutoff := now.AddDate(0, 0, -archivedExpirationInDays)
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID.String()),
		base.ActiveKey(msg.Queue),
		base.DeadlinesKey(msg.Queue),
		base.ArchivedKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
		base.FailedKey(msg.Queue, now),
	}
	argv := []interface{}{
		msg.ID.String(),
		encoded,
		now.Unix(),
		cutoff.Unix(),
		maxArchiveSize,
		expireAt.Unix(),
	}
	return r.runScript(op, archiveCmd, keys, argv...)
}

// ForwardIfReady checks scheduled and retry sets of the given queues
// and move any tasks that are ready to be processed to the pending set.
func (r *RDB) ForwardIfReady(qnames ...string) error {
	var op errors.Op = "rdb.ForwardIfReady"
	for _, qname := range qnames {
		if err := r.forwardAll(qname); err != nil {
			return errors.E(op, errors.CanonicalCode(err), err)
		}
	}
	return nil
}

// KEYS[1] -> source queue (e.g. asynq:{<qname>:scheduled or asynq:{<qname>}:retry})
// KEYS[2] -> asynq:{<qname>}:pending
// ARGV[1] -> current unix time
// ARGV[2] -> task key prefix
// Note: Script moves tasks up to 100 at a time to keep the runtime of script short.
var forwardCmd = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(ids) do
	redis.call("LPUSH", KEYS[2], id)
	redis.call("ZREM", KEYS[1], id)
	redis.call("HSET", ARGV[2] .. id, "state", "pending")
end
return table.getn(ids)`)

// forward moves tasks with a score less than the current unix time
// from the src zset to the dst list. It returns the number of tasks moved.
func (r *RDB) forward(src, dst, taskKeyPrefix string) (int, error) {
	now := float64(time.Now().Unix())
	res, err := forwardCmd.Run(r.client, []string{src, dst}, now, taskKeyPrefix).Result()
	if err != nil {
		return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	n, err := cast.ToIntE(res)
	if err != nil {
		return 0, errors.E(errors.Internal, fmt.Sprintf("cast error: Lua script returned unexpected value: %v", res))
	}
	return n, nil
}

// forwardAll checks for tasks in scheduled/retry state that are ready to be run, and updates
// their state to "pending".
func (r *RDB) forwardAll(qname string) (err error) {
	sources := []string{base.ScheduledKey(qname), base.RetryKey(qname)}
	dst := base.PendingKey(qname)
	taskKeyPrefix := base.TaskKeyPrefix(qname)
	for _, src := range sources {
		n := 1
		for n != 0 {
			n, err = r.forward(src, dst, taskKeyPrefix)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// KEYS[1] -> asynq:{<qname>}:deadlines
// ARGV[1] -> deadline in unix time
// ARGV[2] -> task key prefix
var listDeadlineExceededCmd = redis.NewScript(`
local res = {}
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, id in ipairs(ids) do
	local key = ARGV[2] .. id
	table.insert(res, redis.call("HGET", key, "msg"))
end
return res
`)

// ListDeadlineExceeded returns a list of task messages that have exceeded the deadline from the given queues.
func (r *RDB) ListDeadlineExceeded(deadline time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	var op errors.Op = "rdb.ListDeadlineExceeded"
	var msgs []*base.TaskMessage
	for _, qname := range qnames {
		res, err := listDeadlineExceededCmd.Run(r.client,
			[]string{base.DeadlinesKey(qname)},
			deadline.Unix(), base.TaskKeyPrefix(qname)).Result()
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
		}
		data, err := cast.ToStringSliceE(res)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("cast error: Lua script returned unexpected value: %v", res))
		}
		for _, s := range data {
			msg, err := base.DecodeMessage([]byte(s))
			if err != nil {
				return nil, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
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
	var op errors.Op = "rdb.WriteServerState"
	bytes, err := base.EncodeServerInfo(info)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode server info: %v", err))
	}
	exp := time.Now().Add(ttl).UTC()
	args := []interface{}{ttl.Seconds(), bytes} // args to the lua script
	for _, w := range workers {
		bytes, err := base.EncodeWorkerInfo(w)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, w.ID, bytes)
	}
	skey := base.ServerInfoKey(info.Host, info.PID, info.ServerID)
	wkey := base.WorkersKey(info.Host, info.PID, info.ServerID)
	if err := r.client.ZAdd(base.AllServers, &redis.Z{Score: float64(exp.Unix()), Member: skey}).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	if err := r.client.ZAdd(base.AllWorkers, &redis.Z{Score: float64(exp.Unix()), Member: wkey}).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return r.runScript(op, writeServerStateCmd, []string{skey, wkey}, args...)
}

// KEYS[1] -> asynq:servers:{<host:pid:sid>}
// KEYS[2] -> asynq:workers:{<host:pid:sid>}
var clearServerStateCmd = redis.NewScript(`
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
return redis.status_reply("OK")`)

// ClearServerState deletes server state data from redis.
func (r *RDB) ClearServerState(host string, pid int, serverID string) error {
	var op errors.Op = "rdb.ClearServerState"
	skey := base.ServerInfoKey(host, pid, serverID)
	wkey := base.WorkersKey(host, pid, serverID)
	if err := r.client.ZRem(base.AllServers, skey).Err(); err != nil {
		return errors.E(op, errors.Internal, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	if err := r.client.ZRem(base.AllWorkers, wkey).Err(); err != nil {
		return errors.E(op, errors.Internal, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	return r.runScript(op, clearServerStateCmd, []string{skey, wkey})
}

// KEYS[1]  -> asynq:schedulers:{<schedulerID>}
// ARGV[1]  -> TTL in seconds
// ARGV[2:] -> schedler entries
var writeSchedulerEntriesCmd = redis.NewScript(`
redis.call("DEL", KEYS[1])
for i = 2, #ARGV do
	redis.call("LPUSH", KEYS[1], ARGV[i])
end
redis.call("EXPIRE", KEYS[1], ARGV[1])
return redis.status_reply("OK")`)

// WriteSchedulerEntries writes scheduler entries data to redis with expiration set to the value ttl.
func (r *RDB) WriteSchedulerEntries(schedulerID string, entries []*base.SchedulerEntry, ttl time.Duration) error {
	var op errors.Op = "rdb.WriteSchedulerEntries"
	args := []interface{}{ttl.Seconds()}
	for _, e := range entries {
		bytes, err := base.EncodeSchedulerEntry(e)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, bytes)
	}
	exp := time.Now().Add(ttl).UTC()
	key := base.SchedulerEntriesKey(schedulerID)
	err := r.client.ZAdd(base.AllSchedulers, &redis.Z{Score: float64(exp.Unix()), Member: key}).Err()
	if err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return r.runScript(op, writeSchedulerEntriesCmd, []string{key}, args...)
}

// ClearSchedulerEntries deletes scheduler entries data from redis.
func (r *RDB) ClearSchedulerEntries(scheduelrID string) error {
	var op errors.Op = "rdb.ClearSchedulerEntries"
	key := base.SchedulerEntriesKey(scheduelrID)
	if err := r.client.ZRem(base.AllSchedulers, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	if err := r.client.Del(key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// CancelationPubSub returns a pubsub for cancelation messages.
func (r *RDB) CancelationPubSub() (*redis.PubSub, error) {
	var op errors.Op = "rdb.CancelationPubSub"
	pubsub := r.client.Subscribe(base.CancelChannel)
	_, err := pubsub.Receive()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Sprintf("redis pubsub receive error: %v", err))
	}
	return pubsub, nil
}

// PublishCancelation publish cancelation message to all subscribers.
// The message is the ID for the task to be canceled.
func (r *RDB) PublishCancelation(id string) error {
	var op errors.Op = "rdb.PublishCancelation"
	if err := r.client.Publish(base.CancelChannel, id).Err(); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("redis pubsub publish error: %v", err))
	}
	return nil
}

// KEYS[1] -> asynq:scheduler_history:<entryID>
// ARGV[1] -> enqueued_at timestamp
// ARGV[2] -> serialized SchedulerEnqueueEvent data
// ARGV[3] -> max number of events to be persisted
var recordSchedulerEnqueueEventCmd = redis.NewScript(`
redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -ARGV[3])
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
return redis.status_reply("OK")`)

// Maximum number of enqueue events to store per entry.
const maxEvents = 1000

// RecordSchedulerEnqueueEvent records the time when the given task was enqueued.
func (r *RDB) RecordSchedulerEnqueueEvent(entryID string, event *base.SchedulerEnqueueEvent) error {
	var op errors.Op = "rdb.RecordSchedulerEnqueueEvent"
	data, err := base.EncodeSchedulerEnqueueEvent(event)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode scheduler enqueue event: %v", err))
	}
	keys := []string{
		base.SchedulerHistoryKey(entryID),
	}
	argv := []interface{}{
		event.EnqueuedAt.Unix(),
		data,
		maxEvents,
	}
	return r.runScript(op, recordSchedulerEnqueueEventCmd, keys, argv...)
}

// ClearSchedulerHistory deletes the enqueue event history for the given scheduler entry.
func (r *RDB) ClearSchedulerHistory(entryID string) error {
	var op errors.Op = "rdb.ClearSchedulerHistory"
	key := base.SchedulerHistoryKey(entryID)
	if err := r.client.Del(key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}
