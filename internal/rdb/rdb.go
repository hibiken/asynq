// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package rdb encapsulates the interactions with redis.
package rdb

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Kua-Fu/asynq/internal/base"
	"github.com/Kua-Fu/asynq/internal/errors"
	"github.com/Kua-Fu/asynq/internal/timeutil"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

const statsTTL = 90 * 24 * time.Hour // 90 days

// LeaseDuration is the duration used to initially create a lease and to extend it thereafter.
const LeaseDuration = 30 * time.Second

// RDB is a client interface to query and mutate task queues.
type RDB struct {
	client redis.UniversalClient
	clock  timeutil.Clock
}

// NewRDB returns a new instance of RDB.
func NewRDB(client redis.UniversalClient) *RDB {
	return &RDB{
		client: client,
		clock:  timeutil.NewRealClock(),
	}
}

// Close closes the connection with redis server.
func (r *RDB) Close() error {
	return r.client.Close()
}

// Client returns the reference to underlying redis client.
func (r *RDB) Client() redis.UniversalClient {
	return r.client
}

// SetClock sets the clock used by RDB to the given clock.
//
// Use this function to set the clock to SimulatedClock in tests.
func (r *RDB) SetClock(c timeutil.Clock) {
	r.clock = c
}

// Ping checks the connection with redis server.
func (r *RDB) Ping() error {
	return r.client.Ping(context.Background()).Err()
}

func (r *RDB) runScript(ctx context.Context, op errors.Op, script *redis.Script, keys []string, args ...interface{}) error {
	if err := script.Run(ctx, r.client, keys, args...).Err(); err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	return nil
}

// Runs the given script with keys and args and returns the script's return value as int64.
func (r *RDB) runScriptWithErrorCode(ctx context.Context, op errors.Op, script *redis.Script, keys []string, args ...interface{}) (int64, error) {
	res, err := script.Run(ctx, r.client, keys, args...).Result()
	if err != nil {
		return 0, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}
	n, ok := res.(int64)
	if !ok {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	return n, nil
}

// enqueueCmd enqueues a given task message.
//
// Input:
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:pending
// --
// ARGV[1] -> task message data
// ARGV[2] -> task ID
// ARGV[3] -> current unix time in nsec
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID already exists
var enqueueCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "pending",
           "pending_since", ARGV[3])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
`)

// Enqueue adds the given task to the pending list of the queue.
func (r *RDB) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Enqueue"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.PendingKey(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		msg.ID,
		r.clock.Now().UnixNano(),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, enqueueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
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
// ARGV[4] -> current unix time in nsec
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID conflicts with another task
// Returns -1 if task unique key already exists
var enqueueUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[3],
           "state", "pending",
           "pending_since", ARGV[4],
           "unique_key", KEYS[1])
redis.call("LPUSH", KEYS[3], ARGV[1])
return 1
`)

// EnqueueUnique inserts the given task if the task's uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	var op errors.Op = "rdb.EnqueueUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, "cannot encode task message: %v", err)
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		msg.UniqueKey,
		base.TaskKey(msg.Queue, msg.ID),
		base.PendingKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		int(ttl.Seconds()),
		encoded,
		r.clock.Now().UnixNano(),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, enqueueUniqueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// Input:
// KEYS[1] -> asynq:{<qname>}:pending
// KEYS[2] -> asynq:{<qname>}:paused
// KEYS[3] -> asynq:{<qname>}:active
// KEYS[4] -> asynq:{<qname>}:lease
// --
// ARGV[1] -> initial lease expiration Unix time
// ARGV[2] -> task key prefix
//
// Output:
// Returns nil if no processable task is found in the given queue.
// Returns an encoded TaskMessage.
//
// Note: dequeueCmd checks whether a queue is paused first, before
// calling RPOPLPUSH to pop a task from the queue.
// var dequeueCmd = redis.NewScript(`
// if redis.call("EXISTS", KEYS[2]) == 0 then
// 	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
// 	if id then
// 		local key = ARGV[2] .. id
// 		redis.call("HSET", key, "state", "active")
// 		redis.call("HDEL", key, "pending_since")
// 		redis.call("ZADD", KEYS[4], ARGV[1], id)
// 		return redis.call("HGET", key, "msg")
// 	end
// end
// return nil`)

var getDequeueTaskIDCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 0 then
	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	return id
end
return nil`)

func getUpdateDequeueCmd(taskID string) *redis.Script {

	cmd := ""

	hSetCmd := fmt.Sprintf(`redis.call("HSET", KEYS[1], "state", "active") `)
	cmd += hSetCmd

	hDelCmd := fmt.Sprintf(`redis.call("HDEL", KEYS[1], "pending_since") `)
	cmd += hDelCmd

	zAddCmd := fmt.Sprintf(`redis.call("ZADD", KEYS[5], ARGV[1], "%s") `, taskID)
	cmd += zAddCmd

	cmd += fmt.Sprintf(`return redis.call("HGET", KEYS[1], "msg")`)

	return redis.NewScript(cmd)

}

// Dequeue queries given queues in order and pops a task message
// off a queue if one exists and returns the message and its lease expiration time.
// Dequeue skips a queue if the queue is paused.
// If all queues are empty, ErrNoProcessableTask error is returned.
func (r *RDB) Dequeue(qnames ...string) (msg *base.TaskMessage, leaseExpirationTime time.Time, err error) {
	var op errors.Op = "rdb.Dequeue"
	for _, qname := range qnames {
		keys := []string{
			base.PendingKey(qname),
			base.PausedKey(qname),
			base.ActiveKey(qname),
			base.LeaseKey(qname),
		}
		leaseExpirationTime = r.clock.Now().Add(LeaseDuration)
		argv := []interface{}{
			leaseExpirationTime.Unix(),
			base.TaskKeyPrefix(qname),
		}

		tRes, err := getDequeueTaskIDCmd.Run(context.Background(), r.client, keys, argv...).Result()

		if err == redis.Nil {
			continue
		} else if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
		}

		// if id exists
		taskID, ok := tRes.(string)
		if !ok {
			continue
		}

		argv2 := base.TaskKeyPrefix(qname)
		updateCmd := getUpdateDequeueCmd(taskID)

		nKeys := []string{}

		nKeys = append(nKeys, argv2+taskID)
		for _, item := range keys {
			nKeys = append(nKeys, item)
		}

		res, err := updateCmd.Run(context.Background(), r.client, nKeys, argv...).Result()

		if err == redis.Nil {
			continue
		} else if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
		}

		encoded, err := cast.ToStringE(res)
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
		}
		if msg, err = base.DecodeMessage([]byte(encoded)); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		}
		return msg, leaseExpirationTime, nil
	}
	return nil, time.Time{}, errors.E(op, errors.NotFound, errors.ErrNoProcessableTask)
}

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:lease
// KEYS[3] -> asynq:{<qname>}:t:<task_id>
// KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[5] -> asynq:{<qname>}:processed
// -------
// ARGV[1] -> task ID
// ARGV[2] -> stats expiration timestamp
// ARGV[3] -> max int64 value
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
local total = redis.call("GET", KEYS[5])
if tonumber(total) == tonumber(ARGV[3]) then
	redis.call("SET", KEYS[5], 1)
else
	redis.call("INCR", KEYS[5])
end
return redis.status_reply("OK")
`)

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:lease
// KEYS[3] -> asynq:{<qname>}:t:<task_id>
// KEYS[4] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[5] -> asynq:{<qname>}:processed
// KEYS[6] -> unique key
// -------
// ARGV[1] -> task ID
// ARGV[2] -> stats expiration timestamp
// ARGV[3] -> max int64 value
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
local total = redis.call("GET", KEYS[5])
if tonumber(total) == tonumber(ARGV[3]) then
	redis.call("SET", KEYS[5], 1)
else
	redis.call("INCR", KEYS[5])
end
if redis.call("GET", KEYS[6]) == ARGV[1] then
  redis.call("DEL", KEYS[6])
end
return redis.status_reply("OK")
`)

// Done removes the task from active queue and deletes the task.
// It removes a uniqueness lock acquired by the task, if any.
func (r *RDB) Done(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Done"
	now := r.clock.Now()
	expireAt := now.Add(statsTTL)
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
		base.ProcessedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		expireAt.Unix(),
		int64(math.MaxInt64),
	}
	// Note: We cannot pass empty unique key when running this script in redis-cluster.
	if len(msg.UniqueKey) > 0 {
		keys = append(keys, msg.UniqueKey)
		return r.runScript(ctx, op, doneUniqueCmd, keys, argv...)
	}
	return r.runScript(ctx, op, doneCmd, keys, argv...)
}

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:lease
// KEYS[3] -> asynq:{<qname>}:completed
// KEYS[4] -> asynq:{<qname>}:t:<task_id>
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[6] -> asynq:{<qname>}:processed
//
// ARGV[1] -> task ID
// ARGV[2] -> stats expiration timestamp
// ARGV[3] -> task expiration time in unix time
// ARGV[4] -> task message data
// ARGV[5] -> max int64 value
var markAsCompleteCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
  return redis.error_reply("INTERNAL")
end
redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[2])
end
local total = redis.call("GET", KEYS[6])
if tonumber(total) == tonumber(ARGV[5]) then
	redis.call("SET", KEYS[6], 1)
else
	redis.call("INCR", KEYS[6])
end
return redis.status_reply("OK")
`)

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:lease
// KEYS[3] -> asynq:{<qname>}:completed
// KEYS[4] -> asynq:{<qname>}:t:<task_id>
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[6] -> asynq:{<qname>}:processed
// KEYS[7] -> asynq:{<qname>}:unique:{<checksum>}
//
// ARGV[1] -> task ID
// ARGV[2] -> stats expiration timestamp
// ARGV[3] -> task expiration time in unix time
// ARGV[4] -> task message data
// ARGV[5] -> max int64 value
var markAsCompleteUniqueCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
  return redis.error_reply("INTERNAL")
end
redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[2])
end
local total = redis.call("GET", KEYS[6])
if tonumber(total) == tonumber(ARGV[5]) then
	redis.call("SET", KEYS[6], 1)
else
	redis.call("INCR", KEYS[6])
end
if redis.call("GET", KEYS[7]) == ARGV[1] then
  redis.call("DEL", KEYS[7])
end
return redis.status_reply("OK")
`)

// MarkAsComplete removes the task from active queue to mark the task as completed.
// It removes a uniqueness lock acquired by the task, if any.
func (r *RDB) MarkAsComplete(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.MarkAsComplete"
	now := r.clock.Now()
	statsExpireAt := now.Add(statsTTL)
	msg.CompletedAt = now.Unix()
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.CompletedKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
		base.ProcessedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		statsExpireAt.Unix(),
		now.Unix() + msg.Retention,
		encoded,
		int64(math.MaxInt64),
	}
	// Note: We cannot pass empty unique key when running this script in redis-cluster.
	if len(msg.UniqueKey) > 0 {
		keys = append(keys, msg.UniqueKey)
		return r.runScript(ctx, op, markAsCompleteUniqueCmd, keys, argv...)
	}
	return r.runScript(ctx, op, markAsCompleteCmd, keys, argv...)
}

// KEYS[1] -> asynq:{<qname>}:active
// KEYS[2] -> asynq:{<qname>}:lease
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
func (r *RDB) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.Requeue"
	keys := []string{
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.PendingKey(msg.Queue),
		base.TaskKey(msg.Queue, msg.ID),
	}
	return r.runScript(ctx, op, requeueCmd, keys, msg.ID)
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:g:<group_key>
// KEYS[3] -> asynq:{<qname>}:groups
// -------
// ARGV[1] -> task message data
// ARGV[2] -> task ID
// ARGV[3] -> current time in Unix time
// ARGV[4] -> group key
//
// Output:
// Returns 1 if successfully added
// Returns 0 if task ID already exists
var addToGroupCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "aggregating",
	       "group", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
`)

func (r *RDB) AddToGroup(ctx context.Context, msg *base.TaskMessage, groupKey string) error {
	var op errors.Op = "rdb.AddToGroup"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.GroupKey(msg.Queue, groupKey),
		base.AllGroups(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		msg.ID,
		r.clock.Now().Unix(),
		groupKey,
	}
	n, err := r.runScriptWithErrorCode(ctx, op, addToGroupCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:g:<group_key>
// KEYS[3] -> asynq:{<qname>}:groups
// KEYS[4] -> unique key
// -------
// ARGV[1] -> task message data
// ARGV[2] -> task ID
// ARGV[3] -> current time in Unix time
// ARGV[4] -> group key
// ARGV[5] -> uniqueness lock TTL
//
// Output:
// Returns 1 if successfully added
// Returns 0 if task ID already exists
// Returns -1 if task unique key already exists
var addToGroupUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[4], ARGV[2], "NX", "EX", ARGV[5])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "aggregating",
	       "group", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
`)

func (r *RDB) AddToGroupUnique(ctx context.Context, msg *base.TaskMessage, groupKey string, ttl time.Duration) error {
	var op errors.Op = "rdb.AddToGroupUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.GroupKey(msg.Queue, groupKey),
		base.AllGroups(msg.Queue),
		base.UniqueKey(msg.Queue, msg.Type, msg.Payload),
	}
	argv := []interface{}{
		encoded,
		msg.ID,
		r.clock.Now().Unix(),
		groupKey,
		int(ttl.Seconds()),
	}
	n, err := r.runScriptWithErrorCode(ctx, op, addToGroupUniqueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:scheduled
// -------
// ARGV[1] -> task message data
// ARGV[2] -> process_at time in Unix time
// ARGV[3] -> task ID
//
// Output:
// Returns 1 if successfully enqueued
// Returns 0 if task ID already exists
var scheduleCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "scheduled")
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
`)

// Schedule adds the task to the scheduled set to be processed in the future.
func (r *RDB) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "rdb.Schedule"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ScheduledKey(msg.Queue),
	}
	argv := []interface{}{
		encoded,
		processAt.Unix(),
		msg.ID,
	}
	n, err := r.runScriptWithErrorCode(ctx, op, scheduleCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// KEYS[1] -> unique key
// KEYS[2] -> asynq:{<qname>}:t:<task_id>
// KEYS[3] -> asynq:{<qname>}:scheduled
// -------
// ARGV[1] -> task ID
// ARGV[2] -> uniqueness lock TTL
// ARGV[3] -> score (process_at timestamp)
// ARGV[4] -> task message
//
// Output:
// Returns 1 if successfully scheduled
// Returns 0 if task ID already exists
// Returns -1 if task unique key already exists
var scheduleUniqueCmd = redis.NewScript(`
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[4],
           "state", "scheduled",
           "unique_key", KEYS[1])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
return 1
`)

// ScheduleUnique adds the task to the backlog queue to be processed in the future if the uniqueness lock can be acquired.
// It returns ErrDuplicateTask if the lock cannot be acquired.
func (r *RDB) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	var op errors.Op = "rdb.ScheduleUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode task message: %v", err))
	}
	if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	keys := []string{
		msg.UniqueKey,
		base.TaskKey(msg.Queue, msg.ID),
		base.ScheduledKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		int(ttl.Seconds()),
		processAt.Unix(),
		encoded,
	}
	n, err := r.runScriptWithErrorCode(ctx, op, scheduleUniqueCmd, keys, argv...)
	if err != nil {
		return err
	}
	if n == -1 {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:lease
// KEYS[4] -> asynq:{<qname>}:retry
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[6] -> asynq:{<qname>}:failed:<yyyy-mm-dd>
// KEYS[7] -> asynq:{<qname>}:processed
// KEYS[8] -> asynq:{<qname>}:failed
// -------
// ARGV[1] -> task ID
// ARGV[2] -> updated base.TaskMessage value
// ARGV[3] -> retry_at UNIX timestamp
// ARGV[4] -> stats expiration timestamp
// ARGV[5] -> is_failure (bool)
// ARGV[6] -> max int64 value
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
    local total = redis.call("GET", KEYS[7])
    if tonumber(total) == tonumber(ARGV[6]) then
    	redis.call("SET", KEYS[7], 1)
    	redis.call("SET", KEYS[8], 1)
    else
    	redis.call("INCR", KEYS[7])
    	redis.call("INCR", KEYS[8])
    end
end
return redis.status_reply("OK")`)

// Retry moves the task from active to retry queue.
// It also annotates the message with the given error message and
// if isFailure is true increments the retried counter.
func (r *RDB) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	var op errors.Op = "rdb.Retry"
	now := r.clock.Now()
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
		base.TaskKey(msg.Queue, msg.ID),
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.RetryKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
		base.FailedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
		base.FailedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		encoded,
		processAt.Unix(),
		expireAt.Unix(),
		isFailure,
		int64(math.MaxInt64),
	}
	return r.runScript(ctx, op, retryCmd, keys, argv...)
}

const (
	maxArchiveSize           = 10000 // maximum number of tasks in archive
	archivedExpirationInDays = 90    // number of days before an archived task gets deleted permanently
)

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:lease
// KEYS[4] -> asynq:{<qname>}:archived
// KEYS[5] -> asynq:{<qname>}:processed:<yyyy-mm-dd>
// KEYS[6] -> asynq:{<qname>}:failed:<yyyy-mm-dd>
// KEYS[7] -> asynq:{<qname>}:processed
// KEYS[8] -> asynq:{<qname>}:failed
// -------
// ARGV[1] -> task ID
// ARGV[2] -> updated base.TaskMessage value
// ARGV[3] -> died_at UNIX timestamp
// ARGV[4] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[5] -> max number of tasks in archive (e.g., 100)
// ARGV[6] -> stats expiration timestamp
// ARGV[7] -> max int64 value
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
local total = redis.call("GET", KEYS[7])
if tonumber(total) == tonumber(ARGV[7]) then
   	redis.call("SET", KEYS[7], 1)
   	redis.call("SET", KEYS[8], 1)
else
  	redis.call("INCR", KEYS[7])
   	redis.call("INCR", KEYS[8])
end
return redis.status_reply("OK")`)

// Archive sends the given task to archive, attaching the error message to the task.
// It also trims the archive by timestamp and set size.
func (r *RDB) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	var op errors.Op = "rdb.Archive"
	now := r.clock.Now()
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
		base.TaskKey(msg.Queue, msg.ID),
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.ArchivedKey(msg.Queue),
		base.ProcessedKey(msg.Queue, now),
		base.FailedKey(msg.Queue, now),
		base.ProcessedTotalKey(msg.Queue),
		base.FailedTotalKey(msg.Queue),
	}
	argv := []interface{}{
		msg.ID,
		encoded,
		now.Unix(),
		cutoff.Unix(),
		maxArchiveSize,
		expireAt.Unix(),
		int64(math.MaxInt64),
	}
	return r.runScript(ctx, op, archiveCmd, keys, argv...)
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
// ARGV[1] -> current unix time in seconds
// ARGV[2] -> task key prefix
// ARGV[3] -> current unix time in nsec
// ARGV[4] -> group key prefix
// Note: Script moves tasks up to 100 at a time to keep the runtime of script short.
var forwardCmd = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(ids) do
	local taskKey = ARGV[2] .. id
	local group = redis.call("HGET", taskKey, "group")
	if group and group ~= '' then
	    redis.call("ZADD", ARGV[4] .. group, ARGV[1], id)
		redis.call("ZREM", KEYS[1], id)
		redis.call("HSET", taskKey,
				   "state", "aggregating")
	else
		redis.call("LPUSH", KEYS[2], id)
		redis.call("ZREM", KEYS[1], id)
		redis.call("HSET", taskKey,
				   "state", "pending",
				   "pending_since", ARGV[3])
	end
end
return table.getn(ids)`)

// forward (1) get taskIDs
var getForwardTasksCmd = redis.NewScript(`return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)`)

// forward (2) get group info
func getForwardTaskGroupsCmd(taskIDs []string) *redis.Script {
	cmd := ""
	gTableCmd := fmt.Sprintf("local gTable  = {} ")

	cmd += gTableCmd

	for i := range taskIDs {
		groupCmd := fmt.Sprintf(
			`local group = redis.call("HGET", KEYS[%d], "group") if group == nil then group = '' end gTable[%d] = group `,
			i+1, i+1)
		cmd += groupCmd
	}

	cmd += fmt.Sprintf("return gTable ")
	return redis.NewScript(cmd)
}

// forward (4) scheduled
func getScheduledForwardTaskCmd(taskIDs []string, argv1 int64) *redis.Script {

	cmd := ""

	for i, taskID := range taskIDs {
		zAddCmd := fmt.Sprintf(`redis.call("ZADD", KEYS[%d], %d, "%s") `, (i+1)*2, argv1, taskID)
		cmd += zAddCmd

		zRemCmd := fmt.Sprintf(`redis.call("ZREM", KEYS[1], "%s") `, taskID)
		cmd += zRemCmd

		hSetCmd := fmt.Sprintf(`redis.call("HSET", KEYS[%d], "state", "aggregating") `, (i+1)*2+1)
		cmd += hSetCmd
	}

	cmd += "return '1'"
	return redis.NewScript(cmd)

}

// forward (3) retry
func getRetryForwardTaskCmd(taskIDs []string, argv3 int64) *redis.Script {

	cmd := ""

	for i, taskID := range taskIDs {
		lPushCmd := fmt.Sprintf(`redis.call("LPUSH", KEYS[1], "%s") `, taskID)
		cmd += lPushCmd

		zRemCmd := fmt.Sprintf(`redis.call("ZREM", KEYS[2], "%s") `, taskID)
		cmd += zRemCmd

		hSetCmd := fmt.Sprintf(`redis.call("HSET", KEYS[%d], "state", "pending", "pending_since", "%s") `, i+3, fmt.Sprintf("%d", argv3))
		cmd += hSetCmd
	}

	cmd += "return '1'"
	return redis.NewScript(cmd)

}

func (r *RDB) doForward(IDs []string, keys []string, argv []interface{}) (int, error) {

	argv1, _ := argv[0].(int64)
	argv2, _ := argv[1].(string)
	argv3, _ := argv[2].(int64)
	argv4, _ := argv[3].(string)
	groupCmd := getForwardTaskGroupsCmd(IDs)
	nKeys := []string{}
	for _, item := range IDs {
		nKeys = append(nKeys, argv2+item)
	}

	// 1. get group info
	res, err := groupCmd.Run(context.Background(), r.client, nKeys, argv...).Result()
	if err != nil {
		return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}

	// 2. get retry or scheduled
	gInfo, ok := res.([]interface{})
	if !ok {
		return 0, nil
	}

	retryIDs := map[string]string{}
	scheduleIDs := map[string]string{}
	for i, item := range gInfo {
		taskGroup, ok := item.(string)
		if !ok {
			continue
		}
		if taskGroup != "" {
			scheduleIDs[IDs[i]] = taskGroup
		} else {
			retryIDs[IDs[i]] = taskGroup
		}
	}

	updateSize := 0
	if len(retryIDs) > 0 {
		nKeys := []string{keys[1], keys[0]}
		rIDs := []string{}
		for k := range retryIDs {
			nKeys = append(nKeys, argv2+k)
			rIDs = append(rIDs, k)
		}
		retryCmd := getRetryForwardTaskCmd(rIDs, argv3)
		_, err := retryCmd.Run(context.Background(), r.client, nKeys, argv...).Result()
		if err != nil {
			return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
		}
		updateSize += len(retryIDs)
	}

	if len(scheduleIDs) > 0 {
		nKeys := []string{keys[0]}
		sIDs := []string{}
		for k, v := range scheduleIDs {
			nKeys = append(nKeys, argv4+v)
			nKeys = append(nKeys, argv2+k)
			sIDs = append(sIDs, k)
		}
		sCmd := getScheduledForwardTaskCmd(sIDs, argv1)
		_, err := sCmd.Run(context.Background(), r.client, nKeys, argv...).Result()
		if err != nil {
			return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
		}
		updateSize += len(scheduleIDs)
	}
	return updateSize, nil
}

// forward moves tasks with a score less than the current unix time from the delayed (i.e. scheduled | retry) zset
// to the pending list or group set.
// It returns the number of tasks moved.
func (r *RDB) forward(delayedKey, pendingKey, taskKeyPrefix, groupKeyPrefix string) (int, error) {
	now := r.clock.Now()
	keys := []string{delayedKey, pendingKey}
	argv := []interface{}{
		now.Unix(),
		taskKeyPrefix,
		now.UnixNano(),
		groupKeyPrefix,
	}

	res, err := getForwardTasksCmd.Run(context.Background(), r.client, keys, argv...).Result()

	if err != nil {
		return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}

	taskIDs, ok := res.([]interface{})
	if !ok {
		return 0, nil
	}

	IDs := []string{}
	updateSize := 0

	for _, item := range taskIDs {

		taskID, ok := item.(string)
		if !ok {
			continue
		}

		IDs = append(IDs, taskID)

		if len(IDs) == 100 {
			uCount, err := r.doForward(IDs, keys, argv)
			if err != nil {
				return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
			}
			updateSize += uCount
			IDs = []string{}
		}
	}

	if len(IDs) > 0 {
		uCount, err := r.doForward(IDs, keys, argv)
		if err != nil {
			return 0, errors.E(errors.Internal, fmt.Sprintf("redis eval error: %v", err))
		}
		updateSize += uCount
	}

	return updateSize, nil
}

// forwardAll checks for tasks in scheduled/retry state that are ready to be run, and updates
// their state to "pending" or "aggregating".
func (r *RDB) forwardAll(qname string) (err error) {
	delayedKeys := []string{base.ScheduledKey(qname), base.RetryKey(qname)}
	pendingKey := base.PendingKey(qname)
	taskKeyPrefix := base.TaskKeyPrefix(qname)
	groupKeyPrefix := base.GroupKeyPrefix(qname)
	for _, delayedKey := range delayedKeys {
		n := 1
		for n != 0 {
			n, err = r.forward(delayedKey, pendingKey, taskKeyPrefix, groupKeyPrefix)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ListGroups returns a list of all known groups in the given queue.
func (r *RDB) ListGroups(qname string) ([]string, error) {
	var op errors.Op = "RDB.ListGroups"
	groups, err := r.client.SMembers(context.Background(), base.AllGroups(qname)).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "smembers", Err: err})
	}
	return groups, nil
}

// aggregationCheckCmd checks the given group for whether to create an aggregation set.
// An aggregation set is created if one of the aggregation criteria is met:
// 1) group has reached or exceeded its max size
// 2) group's oldest task has reached or exceeded its max delay
// 3) group's latest task has reached or exceeded its grace period
// if aggreation criteria is met, the command moves those tasks from the group
// and put them in an aggregation set. Additionally, if the creation of aggregation set
// empties the group, it will clear the group name from the all groups set.
//
// KEYS[1] -> asynq:{<qname>}:g:<gname>
// KEYS[2] -> asynq:{<qname>}:g:<gname>:<aggregation_set_id>
// KEYS[3] -> asynq:{<qname>}:aggregation_sets
// KEYS[4] -> asynq:{<qname>}:groups
// -------
// ARGV[1] -> max group size
// ARGV[2] -> max group delay in unix time
// ARGV[3] -> start time of the grace period
// ARGV[4] -> aggregation set expire time
// ARGV[5] -> current time in unix time
// ARGV[6] -> group name
//
// Output:
// Returns 0 if no aggregation set was created
// Returns 1 if an aggregation set was created
//
// Time Complexity:
// O(log(N) + M) with N being the number tasks in the group zset
// and M being the max size.
var aggregationCheckCmd = redis.NewScript(`
local size = redis.call("ZCARD", KEYS[1])
if size == 0 then
	return 0
end
local maxSize = tonumber(ARGV[1])
if maxSize ~= 0 and size >= maxSize then
	local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
	end
	redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
	redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
	if size == maxSize then
		redis.call("SREM", KEYS[4], ARGV[6])
	end
	return 1
end
local maxDelay = tonumber(ARGV[2])
local currentTime = tonumber(ARGV[5])
if maxDelay ~= 0 then
	local oldestEntry = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
	local oldestEntryScore = tonumber(oldestEntry[2])
	local maxDelayTime = currentTime - maxDelay
	if oldestEntryScore <= maxDelayTime then
		local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
		for i=1, table.getn(res)-1, 2 do
			redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
		end
		redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
		redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
		if size <= maxSize or maxSize == 0 then
			redis.call("SREM", KEYS[4], ARGV[6])
		end
		return 1
	end
end
local latestEntry = redis.call("ZREVRANGE", KEYS[1], 0, 0, "WITHSCORES")
local latestEntryScore = tonumber(latestEntry[2])
local gracePeriodStartTime = currentTime - tonumber(ARGV[3])
if latestEntryScore <= gracePeriodStartTime then
	local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
	end
	redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
	redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
	if size <= maxSize or maxSize == 0 then
		redis.call("SREM", KEYS[4], ARGV[6])
	end
	return 1
end
return 0
`)

// Task aggregation should finish within this timeout.
// Otherwise an aggregation set should be reclaimed by the recoverer.
const aggregationTimeout = 2 * time.Minute

// AggregationCheck checks the group identified by the given queue and group name to see if the tasks in the
// group are ready to be aggregated. If so, it moves the tasks to be aggregated to a aggregation set and returns
// the set ID. If not, it returns an empty string for the set ID.
// The time for gracePeriod and maxDelay is computed relative to the time t.
//
// Note: It assumes that this function is called at frequency less than or equal to the gracePeriod. In other words,
// the function only checks the most recently added task against the given gracePeriod.
func (r *RDB) AggregationCheck(qname, gname string, t time.Time, gracePeriod, maxDelay time.Duration, maxSize int) (string, error) {
	var op errors.Op = "RDB.AggregationCheck"
	aggregationSetID := uuid.NewString()
	expireTime := r.clock.Now().Add(aggregationTimeout)
	keys := []string{
		base.GroupKey(qname, gname),
		base.AggregationSetKey(qname, gname, aggregationSetID),
		base.AllAggregationSets(qname),
		base.AllGroups(qname),
	}
	argv := []interface{}{
		maxSize,
		int64(maxDelay.Seconds()),
		int64(gracePeriod.Seconds()),
		expireTime.Unix(),
		t.Unix(),
		gname,
	}
	n, err := r.runScriptWithErrorCode(context.Background(), op, aggregationCheckCmd, keys, argv...)
	if err != nil {
		return "", err
	}
	switch n {
	case 0:
		return "", nil
	case 1:
		return aggregationSetID, nil
	default:
		return "", errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from lua script: %d", n))
	}
}

// KEYS[1] -> asynq:{<qname>}:g:<gname>:<aggregation_set_id>
// ------
// ARGV[1] -> task key prefix
//
// Output:
// Array of encoded task messages
//
// Time Complexity:
// O(N) with N being the number of tasks in the aggregation set.
var readAggregationSetCmd = redis.NewScript(`
local msgs = {}
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	local key = ARGV[1] .. id
	table.insert(msgs, redis.call("HGET", key, "msg"))
end
return msgs
`)

var getAggregationSetTasksCmd = redis.NewScript(`return redis.call("ZRANGE", KEYS[1], 0, -1)`)

func getAggregationMsgCmd(taskIDs []string) *redis.Script {

	cmd := ""

	cmd += fmt.Sprintf(`local msgs = {} `)

	for i := range taskIDs {
		hGetCmd := fmt.Sprintf(`table.insert(msgs, redis.call("HGET", KEYS[%d], "msg")) `, i+1)
		cmd += hGetCmd
	}

	cmd += fmt.Sprintf(`return msgs `)
	return redis.NewScript(cmd)
}

// ReadAggregationSet retrieves members of an aggregation set and returns a list of tasks in the set and
// the deadline for aggregating those tasks.
func (r *RDB) ReadAggregationSet(qname, gname, setID string) ([]*base.TaskMessage, time.Time, error) {
	var op errors.Op = "RDB.ReadAggregationSet"
	ctx := context.Background()
	aggSetKey := base.AggregationSetKey(qname, gname, setID)
	keys := []string{aggSetKey}
	argv := []interface{}{base.TaskKeyPrefix(qname)}
	// 1. get taskIDs
	res, err := getAggregationSetTasksCmd.Run(ctx, r.client, keys, argv...).Result()

	if err != nil {
		return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}

	taskIDs, ok := res.([]interface{})
	if !ok {
		return nil, time.Time{}, nil
	}

	IDs := []string{}

	for _, item := range taskIDs {

		taskID, ok := item.(string)
		if !ok {
			continue
		}

		IDs = append(IDs, taskID)
	}

	if len(IDs) == 0 {
		return nil, time.Time{}, nil
	}

	argv1, _ := argv[0].(string)

	// 2. get msg
	msgCmd := getAggregationMsgCmd(IDs)

	nKeys := []string{}
	for _, taskID := range IDs {
		nKeys = append(nKeys, argv1+taskID)
	}

	res, err = msgCmd.Run(ctx, r.client, nKeys, argv...).Result()

	if err != nil {
		return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}

	data, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cast error: Lua script returned unexpected value: %v", res))
	}
	var msgs []*base.TaskMessage
	for _, s := range data {
		msg, err := base.DecodeMessage([]byte(s))
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode message: %v", err))
		}
		msgs = append(msgs, msg)
	}
	deadlineUnix, err := r.client.ZScore(ctx, base.AllAggregationSets(qname), aggSetKey).Result()
	if err != nil {
		return nil, time.Time{}, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zscore", Err: err})
	}
	return msgs, time.Unix(int64(deadlineUnix), 0), nil
}

// KEYS[1] -> asynq:{<qname>}:g:<gname>:<aggregation_set_id>
// KEYS[2] -> asynq:{<qname>}:aggregation_sets
// -------
// ARGV[1] -> task key prefix
//
// Output:
// Redis status reply
//
// Time Complexity:
// max(O(N), O(log(M))) with N being the number of tasks in the aggregation set
// and M being the number of elements in the all-aggregation-sets list.
var deleteAggregationSetCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids)  do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], KEYS[1])
return redis.status_reply("OK")
`)

var getAggregationDeleteTasksCmd = redis.NewScript(`return redis.call("ZRANGE", KEYS[1], 0, -1)`)

func getDeleteAggregationCmd(taskIDs []string) *redis.Script {

	cmd := ""

	for i := range taskIDs {
		delCmd := fmt.Sprintf(`redis.call("DEL", KEYS[%d]) `, i+3)
		cmd += delCmd
	}

	delCmd := fmt.Sprintf(`redis.call("DEL", KEYS[1]) `)
	cmd += delCmd

	zRemCmd := fmt.Sprintf(`redis.call("ZREM", KEYS[2], KEYS[1]) `)
	cmd += zRemCmd

	cmd += fmt.Sprintf(`return redis.status_reply("OK") `)

	return redis.NewScript(cmd)
}

// DeleteAggregationSet deletes the aggregation set and its members identified by the parameters.
func (r *RDB) DeleteAggregationSet(ctx context.Context, qname, gname, setID string) error {
	var op errors.Op = "RDB.DeleteAggregationSet"
	keys := []string{
		base.AggregationSetKey(qname, gname, setID),
		base.AllAggregationSets(qname),
	}

	// 1. get taskIDs
	res, err := getAggregationDeleteTasksCmd.Run(ctx, r.client, keys).Result()
	if err != nil {
		return fmt.Errorf("redis eval error: %v", err)
	}

	taskIDs, ok := res.([]interface{})
	if !ok {
		return nil
	}

	IDs := []string{}

	for _, item := range taskIDs {

		taskID, ok := item.(string)
		if !ok {
			continue
		}

		IDs = append(IDs, taskID)
	}

	if len(IDs) == 0 {
		return nil
	}

	delCmd := getDeleteAggregationCmd(IDs)

	nKeys := []string{keys[0], keys[1]}
	argv1 := base.TaskKeyPrefix(qname)
	for _, taskID := range IDs {
		nKeys = append(nKeys, argv1+taskID)
	}

	return r.runScript(ctx, op, delCmd, nKeys, base.TaskKeyPrefix(qname))
}

// KEYS[1] -> asynq:{<qname>}:aggregation_sets
// -------
// ARGV[1] -> current time in unix time
var reclaimStateAggregationSetsCmd = redis.NewScript(`
local staleSetKeys = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, key in ipairs(staleSetKeys) do
	local idx = string.find(key, ":[^:]*$")
	local groupKey = string.sub(key, 1, idx-1)
	local res = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", groupKey, tonumber(res[i+1]), res[i])
	end
	redis.call("DEL", key)
end
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
return redis.status_reply("OK")
`)

var getReClaimStateKeysCmd = redis.NewScript(`return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])`)

var getReclaimStateTasksCmd = redis.NewScript(`return redis.call("ZRANGE", KEYS[1], 0, -1, "WITHSCORES") `)

var getDeleteReClaimStateKeyCmd = redis.NewScript(`return redis.call("DEL", KEYS[1])`)

func getUpdateReClaimGroupCmd(taskIDs []string, indexes []int64) *redis.Script {
	cmd := ""
	for i, taskID := range taskIDs {
		zAddCmd := fmt.Sprintf(`redis.call("ZADD", KEYS[1], %d, "%s") `, indexes[i], taskID)
		cmd += zAddCmd
	}
	cmd += fmt.Sprintf(`return '1' `)
	return redis.NewScript(cmd)
}

func getDeleteReClaimStateKeysCmd(argv1 int64) *redis.Script {
	cmd := ""
	delCmd := fmt.Sprintf(`redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", %d) `, argv1)
	cmd += delCmd
	cmd += fmt.Sprintf(`return redis.status_reply("OK") `)
	return redis.NewScript(cmd)
}

func findKey(key string) int {
	re := regexp.MustCompile(`:[^:]*$`)
	match := re.FindStringIndex(key)
	if match != nil {
		return match[0]
	}
	return -1
}

func subKey(key string, idx int) string {
	if idx > 0 && idx <= len(key) {
		return key[:idx]
	}
	return ""
}

// ReclaimStateAggregationSets checks for any stale aggregation sets in the given queue, and
// reclaim tasks in the stale aggregation set by putting them back in the group.
func (r *RDB) ReclaimStaleAggregationSets(qname string) error {
	var op errors.Op = "RDB.ReclaimStaleAggregationSets"
	ctx := context.Background()
	keys := []string{base.AllAggregationSets(qname)}
	argv := []interface{}{r.clock.Now().Unix()}
	// 1. get stale keys
	res, err := getReClaimStateKeysCmd.Run(ctx, r.client, keys, argv...).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}

	sKeys, ok := res.([]interface{})
	if !ok {
		return nil
	}

	cKeys := []string{}

	for _, item := range sKeys {

		cKey, ok := item.(string)
		if !ok {
			continue
		}
		cKeys = append(cKeys, cKey)
	}

	for _, group := range cKeys {
		// 1. get taskIDs
		nKeys := []string{group}
		res, err = getReclaimStateTasksCmd.Run(ctx, r.client, nKeys).Result()
		if err != nil {
			return errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
		}

		taskIDs, ok := res.([]interface{})
		if !ok {
			continue
		}

		IDs := []string{}
		indexes := []int64{}

		for i, item := range taskIDs {
			if i%2 == 0 {
				taskID, ok := item.(string)
				if !ok {
					continue
				}
				IDs = append(IDs, taskID)
			} else {
				index, ok := item.(string)
				if !ok {
					continue
				}
				num, err := strconv.ParseInt(index, 10, 64)
				if err != nil {
					continue
				}
				indexes = append(indexes, num)
			}
		}

		if len(IDs) != len(indexes) {
			continue
		}

		// 2. update tasks
		updateCmd := getUpdateReClaimGroupCmd(IDs, indexes)

		nKeys = []string{}
		idx := findKey(group)
		groupKey := subKey(group, idx)
		nKeys = append(nKeys, groupKey)

		res, err = updateCmd.Run(ctx, r.client, nKeys).Result()

		if err != nil {
			return errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
		}

		// 3. delete tasks
		nKeys = []string{group}
		res, err = getDeleteReClaimStateKeyCmd.Run(ctx, r.client, nKeys).Result()
		if err != nil {
			return errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
		}
	}

	// delete reclaim groups
	argv1, _ := argv[0].(int64)
	delCmd := getDeleteReClaimStateKeysCmd(argv1)
	return r.runScript(context.Background(), op, delCmd, keys, argv...)
}

// KEYS[1] -> asynq:{<qname>}:completed
// ARGV[1] -> current time in unix time
// ARGV[2] -> task key prefix
// ARGV[3] -> batch size (i.e. maximum number of tasks to delete)
//
// Returns the number of tasks deleted.
var deleteExpiredCompletedTasksCmd = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[2] .. id)
	redis.call("ZREM", KEYS[1], id)
end
return table.getn(ids)`)

var getExpiredCompletedTasksCmd = redis.NewScript(`
return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[3]))`)

// DeleteExpiredCompletedTasks checks for any expired tasks in the given queue's completed set,
// and delete all expired tasks.
func (r *RDB) DeleteExpiredCompletedTasks(qname string) error {
	// Note: Do this operation in fix batches to prevent long running script.
	const batchSize = 100
	for {
		n, err := r.deleteExpiredCompletedTasks(qname, batchSize)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

func getDeleteExpiredCompletedTasksCmd(taskIDs []string) *redis.Script {
	delStrs := []string{}
	for i := range taskIDs {
		delStrs = append(delStrs, fmt.Sprintf("KEYS[%d]", i+2))
	}
	cmd := ""

	// redis.call("DEL", ARGV[2] .. id)
	delCmd := fmt.Sprintf(`redis.call("DEL", %s) `, strings.Join(delStrs, ","))

	cmd += delCmd

	// redis.call("ZREM", KEYS[1], id)
	for _, taskID := range taskIDs {
		zRemCmd := fmt.Sprintf(`redis.call("ZREM", KEYS[1], "%s") `, taskID)
		cmd += zRemCmd
	}

	cmd += "return 0"

	return redis.NewScript(cmd)

}

func (r *RDB) doDeleteExpireTask(IDs []string, keys []string, argv []interface{}) (int64, error) {
	var op errors.Op = "rdb.DeleteExpiredCompletedTasks"
	argv2, _ := argv[1].(string)
	delSize := 0
	delCmd := getDeleteExpiredCompletedTasksCmd(IDs)
	nKeys := []string{}
	nKeys = append(nKeys, keys[0])
	for _, item := range IDs {
		nKeys = append(nKeys, argv2+item)
	}
	_, err := delCmd.Run(context.Background(), r.client, nKeys).Result()
	if err != nil {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}
	delSize += len(IDs)
	return int64(delSize), nil
}

// deleteExpiredCompletedTasks runs the lua script to delete expired deleted task with the specified
// batch size. It reports the number of tasks deleted.
func (r *RDB) deleteExpiredCompletedTasks(qname string, batchSize int) (int64, error) {
	var op errors.Op = "rdb.DeleteExpiredCompletedTasks"
	keys := []string{base.CompletedKey(qname)}
	argv := []interface{}{
		r.clock.Now().Unix(),
		base.TaskKeyPrefix(qname),
		batchSize,
	}

	res, err := getExpiredCompletedTasksCmd.Run(context.Background(), r.client, keys, argv...).Result()

	if err != nil {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
	}

	taskIDs, ok := res.([]interface{})
	if !ok {
		return 0, nil
	}

	// batch delete keys
	IDs := []string{}
	delSize := int64(0)

	for _, item := range taskIDs {

		taskID, ok := item.(string)
		if !ok {
			continue
		}

		IDs = append(IDs, taskID)

		if len(IDs) == 100 {
			dSize, err := r.doDeleteExpireTask(IDs, keys, argv)
			if err != nil {
				return 0, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
			}
			delSize += dSize
			IDs = []string{}
		}
	}

	if len(IDs) > 0 {
		dSize, err := r.doDeleteExpireTask(IDs, keys, argv)
		if err != nil {
			return 0, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
		}
		delSize += dSize
	}

	return delSize, nil
}

// KEYS[1] -> asynq:{<qname>}:lease
// ARGV[1] -> cutoff in unix time
// ARGV[2] -> task key prefix
var listLeaseExpiredCmd = redis.NewScript(`
local res = {}
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, id in ipairs(ids) do
	local key = ARGV[2] .. id
	table.insert(res, redis.call("HGET", key, "msg"))
end
return res
`)

var getLeaseExpiredTasksCmd = redis.NewScript(`return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1]) `)

func getLeaseExpiredMsgCmd(taskIDs []string) *redis.Script {
	cmd := ""

	cmd += fmt.Sprintf(`local res = {} `)
	for i := range taskIDs {
		hGetCmd := fmt.Sprintf(`table.insert(res, redis.call("HGET", KEYS[%d], "msg")) `, i+1)
		cmd += hGetCmd
	}

	cmd += fmt.Sprintf(`return res `)
	return redis.NewScript(cmd)

}

// ListLeaseExpired returns a list of task messages with an expired lease from the given queues.
func (r *RDB) ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	var op errors.Op = "rdb.ListLeaseExpired"
	var msgs []*base.TaskMessage
	for _, qname := range qnames {

		keys := []string{base.LeaseKey(qname)}
		argv := []interface{}{cutoff.Unix(), base.TaskKeyPrefix(qname)}
		res, err := getLeaseExpiredTasksCmd.Run(context.Background(), r.client, keys, argv...).Result()

		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("redis eval error: %v", err))
		}

		taskIDs, ok := res.([]interface{})
		if !ok {
			return msgs, nil
		}

		IDs := []string{}

		for _, item := range taskIDs {

			taskID, ok := item.(string)
			if !ok {
				continue
			}

			IDs = append(IDs, taskID)
		}

		if len(IDs) == 0 {
			continue
		}

		argv2, _ := argv[1].(string)

		msgCmd := getLeaseExpiredMsgCmd(IDs)
		nKeys := []string{}

		for _, taskID := range IDs {
			nKeys = append(nKeys, argv2+taskID)
		}
		res, err = msgCmd.Run(context.Background(), r.client, nKeys, argv...).Result()

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

// ExtendLease extends the lease for the given tasks by LeaseDuration (30s).
// It returns a new expiration time if the operation was successful.
func (r *RDB) ExtendLease(qname string, ids ...string) (expirationTime time.Time, err error) {
	expireAt := r.clock.Now().Add(LeaseDuration)
	var zs []redis.Z
	for _, id := range ids {
		zs = append(zs, redis.Z{Member: id, Score: float64(expireAt.Unix())})
	}
	// Use XX option to only update elements that already exist; Don't add new elements
	// TODO: Consider adding GT option to ensure we only "extend" the lease. Ceveat is that GT is supported from redis v6.2.0 or above.
	err = r.client.ZAddXX(context.Background(), base.LeaseKey(qname), zs...).Err()
	if err != nil {
		return time.Time{}, err
	}
	return expireAt, nil
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
	ctx := context.Background()
	bytes, err := base.EncodeServerInfo(info)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode server info: %v", err))
	}
	exp := r.clock.Now().Add(ttl).UTC()
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
	if err := r.client.ZAdd(ctx, base.AllServers, redis.Z{Score: float64(exp.Unix()), Member: skey}).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	if err := r.client.ZAdd(ctx, base.AllWorkers, redis.Z{Score: float64(exp.Unix()), Member: wkey}).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return r.runScript(ctx, op, writeServerStateCmd, []string{skey, wkey}, args...)
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
	ctx := context.Background()
	skey := base.ServerInfoKey(host, pid, serverID)
	wkey := base.WorkersKey(host, pid, serverID)
	if err := r.client.ZRem(ctx, base.AllServers, skey).Err(); err != nil {
		return errors.E(op, errors.Internal, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	if err := r.client.ZRem(ctx, base.AllWorkers, wkey).Err(); err != nil {
		return errors.E(op, errors.Internal, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	return r.runScript(ctx, op, clearServerStateCmd, []string{skey, wkey})
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
	ctx := context.Background()
	args := []interface{}{ttl.Seconds()}
	for _, e := range entries {
		bytes, err := base.EncodeSchedulerEntry(e)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, bytes)
	}
	exp := r.clock.Now().Add(ttl).UTC()
	key := base.SchedulerEntriesKey(schedulerID)
	err := r.client.ZAdd(ctx, base.AllSchedulers, redis.Z{Score: float64(exp.Unix()), Member: key}).Err()
	if err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return r.runScript(ctx, op, writeSchedulerEntriesCmd, []string{key}, args...)
}

// ClearSchedulerEntries deletes scheduler entries data from redis.
func (r *RDB) ClearSchedulerEntries(scheduelrID string) error {
	var op errors.Op = "rdb.ClearSchedulerEntries"
	ctx := context.Background()
	key := base.SchedulerEntriesKey(scheduelrID)
	if err := r.client.ZRem(ctx, base.AllSchedulers, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zrem", Err: err})
	}
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// CancelationPubSub returns a pubsub for cancelation messages.
func (r *RDB) CancelationPubSub() (*redis.PubSub, error) {
	var op errors.Op = "rdb.CancelationPubSub"
	ctx := context.Background()
	pubsub := r.client.Subscribe(ctx, base.CancelChannel)
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Sprintf("redis pubsub receive error: %v", err))
	}
	return pubsub, nil
}

// PublishCancelation publish cancelation message to all subscribers.
// The message is the ID for the task to be canceled.
func (r *RDB) PublishCancelation(id string) error {
	var op errors.Op = "rdb.PublishCancelation"
	ctx := context.Background()
	if err := r.client.Publish(ctx, base.CancelChannel, id).Err(); err != nil {
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
	ctx := context.Background()
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
	return r.runScript(ctx, op, recordSchedulerEnqueueEventCmd, keys, argv...)
}

// ClearSchedulerHistory deletes the enqueue event history for the given scheduler entry.
func (r *RDB) ClearSchedulerHistory(entryID string) error {
	var op errors.Op = "rdb.ClearSchedulerHistory"
	ctx := context.Background()
	key := base.SchedulerHistoryKey(entryID)
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// WriteResult writes the given result data for the specified task.
func (r *RDB) WriteResult(qname, taskID string, data []byte) (int, error) {
	var op errors.Op = "rdb.WriteResult"
	ctx := context.Background()
	taskKey := base.TaskKey(qname, taskID)
	if err := r.client.HSet(ctx, taskKey, "result", data).Err(); err != nil {
		return 0, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	return len(data), nil
}
