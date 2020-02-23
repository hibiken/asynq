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

// KEYS[1] -> asynq:queues:<qname>
// KEYS[2] -> asynq:queues
// ARGV[1] -> task message data
var enqueueCmd = redis.NewScript(`
redis.call("LPUSH", KEYS[1], ARGV[1])
redis.call("SADD", KEYS[2], KEYS[1])
return 1`)

// Enqueue inserts the given task to the tail of the queue.
func (r *RDB) Enqueue(msg *base.TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	key := base.QueueKey(msg.Queue)
	return enqueueCmd.Run(r.client, []string{key, base.AllQueues}, bytes).Err()
}

// Dequeue queries given queues in order and pops a task message if there is one and returns it.
// If all queues are empty, ErrNoProcessableTask error is returned.
func (r *RDB) Dequeue(qnames ...string) (*base.TaskMessage, error) {
	var data string
	var err error
	if len(qnames) == 1 {
		data, err = r.dequeueSingle(base.QueueKey(qnames[0]))
	} else {
		// TODO(hibiken): Take keys are argument and don't compute every time
		var keys []string
		for _, q := range qnames {
			keys = append(keys, base.QueueKey(q))
		}
		data, err = r.dequeue(keys...)
	}
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

func (r *RDB) dequeueSingle(queue string) (data string, err error) {
	// timeout needed to avoid blocking forever
	return r.client.BRPopLPush(queue, base.InProgressQueue, time.Second).Result()
}

// KEYS[1] -> asynq:in_progress
// ARGV    -> List of queues to query in order
var dequeueCmd = redis.NewScript(`
local res
for _, qkey in ipairs(ARGV) do
	res = redis.call("RPOPLPUSH", qkey, KEYS[1])
	if res then
		return res
	end
end
return res`)

func (r *RDB) dequeue(queues ...string) (data string, err error) {
	var args []interface{}
	for _, qkey := range queues {
		args = append(args, qkey)
	}
	res, err := dequeueCmd.Run(r.client, []string{base.InProgressQueue}, args...).Result()
	if err != nil {
		return "", err
	}
	return cast.ToStringE(res)
}

// KEYS[1] -> asynq:in_progress
// KEYS[2] -> asynq:processed:<yyyy-mm-dd>
// ARGV[1] -> base.TaskMessage value
// ARGV[2] -> stats expiration timestamp
// Note: LREM count ZERO means "remove all elements equal to val"
var doneCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1]) 
local n = redis.call("INCR", KEYS[2])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[2], ARGV[2])
end
return redis.status_reply("OK")
`)

// Done removes the task from in-progress queue to mark the task as done.
func (r *RDB) Done(msg *base.TaskMessage) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	now := time.Now()
	processedKey := base.ProcessedKey(now)
	expireAt := now.Add(statsTTL)
	return doneCmd.Run(r.client,
		[]string{base.InProgressQueue, processedKey},
		bytes, expireAt.Unix()).Err()
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
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return requeueCmd.Run(r.client,
		[]string{base.InProgressQueue, base.QueueKey(msg.Queue)},
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

// KEYS[1] -> asynq:in_progress
// KEYS[2] -> asynq:retry
// KEYS[3] -> asynq:processed:<yyyy-mm-dd>
// KEYS[4] -> asynq:failure:<yyyy-mm-dd>
// ARGV[1] -> base.TaskMessage value to remove from base.InProgressQueue queue
// ARGV[2] -> base.TaskMessage value to add to Retry queue
// ARGV[3] -> retry_at UNIX timestamp
// ARGV[4] -> stats expiration timestamp
var retryCmd = redis.NewScript(`
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
return redis.status_reply("OK")`)

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
	now := time.Now()
	processedKey := base.ProcessedKey(now)
	failureKey := base.FailureKey(now)
	expireAt := now.Add(statsTTL)
	return retryCmd.Run(r.client,
		[]string{base.InProgressQueue, base.RetryQueue, processedKey, failureKey},
		string(bytesToRemove), string(bytesToAdd), processAt.Unix(), expireAt.Unix()).Err()
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
return redis.status_reply("OK")`)

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
	return killCmd.Run(r.client,
		[]string{base.InProgressQueue, base.DeadQueue, processedKey, failureKey},
		string(bytesToRemove), string(bytesToAdd), now.Unix(), limit, maxDeadTasks, expireAt.Unix()).Err()
}

// KEYS[1] -> asynq:in_progress
// ARGV[1] -> queue prefix
var requeueAllCmd = redis.NewScript(`
local msgs = redis.call("LRANGE", KEYS[1], 0, -1)
for _, msg in ipairs(msgs) do
	local decoded = cjson.decode(msg)
	local qkey = ARGV[1] .. decoded["Queue"]
	redis.call("RPUSH", qkey, msg)
	redis.call("LREM", KEYS[1], 0, msg)
end
return table.getn(msgs)`)

// RequeueAll moves all tasks from in-progress list to the queue
// and reports the number of tasks restored.
func (r *RDB) RequeueAll() (int64, error) {
	res, err := requeueAllCmd.Run(r.client, []string{base.InProgressQueue}, base.QueuePrefix).Result()
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
//
// qnames specifies to which queues to send tasks.
func (r *RDB) CheckAndEnqueue(qnames ...string) error {
	delayed := []string{base.ScheduledQueue, base.RetryQueue}
	for _, zset := range delayed {
		var err error
		if len(qnames) == 1 {
			err = r.forwardSingle(zset, base.QueueKey(qnames[0]))
		} else {
			err = r.forward(zset)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// KEYS[1] -> source queue (e.g. scheduled or retry queue)
// ARGV[1] -> current unix time
// ARGV[2] -> queue prefix
var forwardCmd = redis.NewScript(`
local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, msg in ipairs(msgs) do
	local decoded = cjson.decode(msg)
	local qkey = ARGV[2] .. decoded["Queue"]
	redis.call("LPUSH", qkey, msg)
	redis.call("ZREM", KEYS[1], msg)
end
return msgs`)

// forward moves all tasks with a score less than the current unix time
// from the src zset.
func (r *RDB) forward(src string) error {
	now := float64(time.Now().Unix())
	return forwardCmd.Run(r.client,
		[]string{src}, now, base.QueuePrefix).Err()
}

// KEYS[1] -> source queue (e.g. scheduled or retry queue)
// KEYS[2] -> destination queue
var forwardSingleCmd = redis.NewScript(`
local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, msg in ipairs(msgs) do
	redis.call("LPUSH", KEYS[2], msg)
	redis.call("ZREM", KEYS[1], msg)
end
return msgs`)

// forwardSingle moves all tasks with a score less than the current unix time
// from the src zset to dst list.
func (r *RDB) forwardSingle(src, dst string) error {
	now := float64(time.Now().Unix())
	return forwardSingleCmd.Run(r.client,
		[]string{src, dst}, now).Err()
}

// KEYS[1]  -> asynq:ps:<host:pid>
// KEYS[2]  -> asynq:ps
// KEYS[3]  -> asynq:workers<host:pid>
// keys[4]  -> asynq:workers
// ARGV[1]  -> expiration time
// ARGV[2]  -> TTL in seconds
// ARGV[3]  -> process info
// ARGV[4:] -> alternate key-value pair of (worker id, worker data)
// Note: Add key to ZSET with expiration time as score.
// ref: https://github.com/antirez/redis/issues/135#issuecomment-2361996
var writeProcessInfoCmd = redis.NewScript(`
redis.call("SETEX", KEYS[1], ARGV[2], ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[1], KEYS[1])
redis.call("DEL", KEYS[3])
for i = 4, table.getn(ARGV)-1, 2 do
	redis.call("HSET", KEYS[3], ARGV[i], ARGV[i+1])
end
redis.call("EXPIRE", KEYS[3], ARGV[2])
redis.call("ZADD", KEYS[4], ARGV[1], KEYS[3])
return redis.status_reply("OK")`)

// WriteProcessState writes process state data to redis with expiration  set to the value ttl.
func (r *RDB) WriteProcessState(ps *base.ProcessState, ttl time.Duration) error {
	info := ps.Get()
	bytes, err := json.Marshal(info)
	if err != nil {
		return err
	}
	var args []interface{} // args to the lua script
	exp := time.Now().Add(ttl).UTC()
	workers := ps.GetWorkers()
	args = append(args, float64(exp.Unix()), ttl.Seconds(), bytes)
	for _, w := range workers {
		bytes, err := json.Marshal(w)
		if err != nil {
			continue // skip bad data
		}
		args = append(args, w.ID.String(), bytes)
	}
	pkey := base.ProcessInfoKey(info.Host, info.PID)
	wkey := base.WorkersKey(info.Host, info.PID)
	return writeProcessInfoCmd.Run(r.client,
		[]string{pkey, base.AllProcesses, wkey, base.AllWorkers},
		args...).Err()
}

// KEYS[1] -> asynq:ps
// KEYS[2] -> asynq:ps:<host:pid>
// KEYS[3] -> asynq:workers
// KEYS[4] -> asynq:workers<host:pid>
var clearProcessInfoCmd = redis.NewScript(`
redis.call("ZREM", KEYS[1], KEYS[2])
redis.call("DEL", KEYS[2])
redis.call("ZREM", KEYS[3], KEYS[4])
redis.call("DEL", KEYS[4])
return redis.status_reply("OK")`)

// ClearProcessState deletes process state data from redis.
func (r *RDB) ClearProcessState(ps *base.ProcessState) error {
	info := ps.Get()
	host, pid := info.Host, info.PID
	pkey := base.ProcessInfoKey(host, pid)
	wkey := base.WorkersKey(host, pid)
	return clearProcessInfoCmd.Run(r.client,
		[]string{base.AllProcesses, pkey, base.AllWorkers, wkey}).Err()
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
