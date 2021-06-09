// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/spf13/cast"
)

// AllQueues returns a list of all queue names.
func (r *RDB) AllQueues() ([]string, error) {
	return r.client.SMembers(base.AllQueues).Result()
}

// Stats represents a state of queues at a certain time.
type Stats struct {
	// Name of the queue (e.g. "default", "critical").
	Queue string
	// MemoryUsage is the total number of bytes the queue and its tasks require
	// to be stored in redis.
	MemoryUsage int64
	// Paused indicates whether the queue is paused.
	// If true, tasks in the queue should not be processed.
	Paused bool
	// Size is the total number of tasks in the queue.
	Size int
	// Number of tasks in each state.
	Pending   int
	Active    int
	Scheduled int
	Retry     int
	Archived  int
	// Total number of tasks processed during the current date.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed during the current date.
	Failed int
	// Time this stats was taken.
	Timestamp time.Time
}

// DailyStats holds aggregate data for a given day.
type DailyStats struct {
	// Name of the queue (e.g. "default", "critical").
	Queue string
	// Total number of tasks processed during the given day.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed during the given day.
	Failed int
	// Date this stats was taken.
	Time time.Time
}

// KEYS[1] -> asynq:<qname>
// KEYS[2] -> asynq:<qname>:active
// KEYS[3] -> asynq:<qname>:scheduled
// KEYS[4] -> asynq:<qname>:retry
// KEYS[5] -> asynq:<qname>:archived
// KEYS[6] -> asynq:<qname>:processed:<yyyy-mm-dd>
// KEYS[7] -> asynq:<qname>:failed:<yyyy-mm-dd>
// KEYS[8] -> asynq:<qname>:paused
var currentStatsCmd = redis.NewScript(`
local res = {}
table.insert(res, KEYS[1])
table.insert(res, redis.call("LLEN", KEYS[1]))
table.insert(res, KEYS[2])
table.insert(res, redis.call("LLEN", KEYS[2]))
table.insert(res, KEYS[3])
table.insert(res, redis.call("ZCARD", KEYS[3]))
table.insert(res, KEYS[4])
table.insert(res, redis.call("ZCARD", KEYS[4]))
table.insert(res, KEYS[5])
table.insert(res, redis.call("ZCARD", KEYS[5]))
local pcount = 0
local p = redis.call("GET", KEYS[6])
if p then
	pcount = tonumber(p) 
end
table.insert(res, KEYS[6])
table.insert(res, pcount)
local fcount = 0
local f = redis.call("GET", KEYS[7])
if f then
	fcount = tonumber(f)
end
table.insert(res, KEYS[7])
table.insert(res, fcount)
table.insert(res, KEYS[8])
table.insert(res, redis.call("EXISTS", KEYS[8]))
return res`)

// CurrentStats returns a current state of the queues.
func (r *RDB) CurrentStats(qname string) (*Stats, error) {
	var op errors.Op = "rdb.CurrentStats"
	exists, err := r.client.SIsMember(base.AllQueues, qname).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, err)
	}
	if !exists {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	now := time.Now()
	res, err := currentStatsCmd.Run(r.client, []string{
		base.PendingKey(qname),
		base.ActiveKey(qname),
		base.ScheduledKey(qname),
		base.RetryKey(qname),
		base.ArchivedKey(qname),
		base.ProcessedKey(qname, now),
		base.FailedKey(qname, now),
		base.PausedKey(qname),
	}).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, err)
	}
	data, err := cast.ToSliceE(res)
	if err != nil {
		return nil, errors.E(op, errors.Internal, "cast error: unexpected return value from Lua script")
	}
	stats := &Stats{
		Queue:     qname,
		Timestamp: now,
	}
	size := 0
	for i := 0; i < len(data); i += 2 {
		key := cast.ToString(data[i])
		val := cast.ToInt(data[i+1])
		switch key {
		case base.PendingKey(qname):
			stats.Pending = val
			size += val
		case base.ActiveKey(qname):
			stats.Active = val
			size += val
		case base.ScheduledKey(qname):
			stats.Scheduled = val
			size += val
		case base.RetryKey(qname):
			stats.Retry = val
			size += val
		case base.ArchivedKey(qname):
			stats.Archived = val
			size += val
		case base.ProcessedKey(qname, now):
			stats.Processed = val
		case base.FailedKey(qname, now):
			stats.Failed = val
		case base.PausedKey(qname):
			if val == 0 {
				stats.Paused = false
			} else {
				stats.Paused = true
			}
		}
	}
	stats.Size = size
	memusg, err := r.memoryUsage(qname)
	if err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	stats.MemoryUsage = memusg
	return stats, nil
}

func (r *RDB) memoryUsage(qname string) (int64, error) {
	var op errors.Op = "rdb.memoryUsage"
	var (
		keys   []string
		data   []string
		cursor uint64
		err    error
	)
	for {
		data, cursor, err = r.client.Scan(cursor, fmt.Sprintf("asynq:{%s}*", qname), 100).Result()
		if err != nil {
			return 0, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "scan", Err: err})
		}
		keys = append(keys, data...)
		if cursor == 0 {
			break
		}
	}
	var usg int64
	for _, k := range keys {
		n, err := r.client.MemoryUsage(k).Result()
		if err != nil {
			return 0, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "memory usage", Err: err})
		}
		usg += n
	}
	return usg, nil
}

var historicalStatsCmd = redis.NewScript(`
local res = {}
for _, key in ipairs(KEYS) do
	local n = redis.call("GET", key)
	if not n then
		n = 0
	end
	table.insert(res, tonumber(n))
end
return res`)

// HistoricalStats returns a list of stats from the last n days for the given queue.
func (r *RDB) HistoricalStats(qname string, n int) ([]*DailyStats, error) {
	var op errors.Op = "rdb.HistoricalStats"
	if n < 1 {
		return nil, errors.E(op, errors.FailedPrecondition, "the number of days must be positive")
	}
	exists, err := r.client.SIsMember(base.AllQueues, qname).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sismember", Err: err})
	}
	if !exists {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	const day = 24 * time.Hour
	now := time.Now().UTC()
	var days []time.Time
	var keys []string
	for i := 0; i < n; i++ {
		ts := now.Add(-time.Duration(i) * day)
		days = append(days, ts)
		keys = append(keys, base.ProcessedKey(qname, ts))
		keys = append(keys, base.FailedKey(qname, ts))
	}
	res, err := historicalStatsCmd.Run(r.client, keys).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Sprintf("redis eval error: %v", err))
	}
	data, err := cast.ToIntSliceE(res)
	if err != nil {
		return nil, errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
	}
	var stats []*DailyStats
	for i := 0; i < len(data); i += 2 {
		stats = append(stats, &DailyStats{
			Queue:     qname,
			Processed: data[i],
			Failed:    data[i+1],
			Time:      days[i/2],
		})
	}
	return stats, nil
}

// RedisInfo returns a map of redis info.
func (r *RDB) RedisInfo() (map[string]string, error) {
	res, err := r.client.Info().Result()
	if err != nil {
		return nil, err
	}
	return parseInfo(res)
}

// RedisClusterInfo returns a map of redis cluster info.
func (r *RDB) RedisClusterInfo() (map[string]string, error) {
	res, err := r.client.ClusterInfo().Result()
	if err != nil {
		return nil, err
	}
	return parseInfo(res)
}

func parseInfo(infoStr string) (map[string]string, error) {
	info := make(map[string]string)
	lines := strings.Split(infoStr, "\r\n")
	for _, l := range lines {
		kv := strings.Split(l, ":")
		if len(kv) == 2 {
			info[kv[0]] = kv[1]
		}
	}
	return info, nil
}

func reverse(x []string) {
	for i := len(x)/2 - 1; i >= 0; i-- {
		opp := len(x) - 1 - i
		x[i], x[opp] = x[opp], x[i]
	}
}

// checkQueueExists verifies whether the queue exists.
// It returns QueueNotFoundError if queue doesn't exist.
func (r *RDB) checkQueueExists(qname string) error {
	exists, err := r.client.SIsMember(base.AllQueues, qname).Result()
	if err != nil {
		return errors.E(errors.Unknown, &errors.RedisCommandError{Command: "sismember", Err: err})
	}
	if !exists {
		return errors.E(errors.Internal, &errors.QueueNotFoundError{Queue: qname})
	}
	return nil
}

// Input:
// KEYS[1] -> task key (asynq:{<qname>}:t:<taskid>)
// ARGV[1] -> task id
// ARGV[2] -> current time in Unix time (seconds)
// ARGV[3] -> queue key prefix (asynq:{<qname>}:)
//
// Output:
// Tuple of {msg, state, nextProcessAt}
// msg: encoded task message
// state: string describing the state of the task
// nextProcessAt: unix time in seconds, zero if not applicable.
//
// If the task key doesn't exist, it returns error with a message "NOT FOUND"
var getTaskInfoCmd = redis.NewScript(`
	if redis.call("EXISTS", KEYS[1]) == 0 then
		return redis.error_reply("NOT FOUND")
	end
	local msg, state = unpack(redis.call("HMGET", KEYS[1], "msg", "state"))
	if state == "scheduled" or state == "retry" then
		return {msg, state, redis.call("ZSCORE", ARGV[3] .. state, ARGV[1])}
	end
	if state == "pending" then
		return {msg, state, ARGV[2]}
	end
	return {msg, state, 0}
`)

// GetTaskInfo returns a TaskInfo describing the task from the given queue.
func (r *RDB) GetTaskInfo(qname string, id uuid.UUID) (*base.TaskInfo, error) {
	var op errors.Op = "rdb.GetTaskInfo"
	if err := r.checkQueueExists(qname); err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	keys := []string{base.TaskKey(qname, id.String())}
	argv := []interface{}{
		id.String(),
		time.Now().Unix(),
		base.QueueKeyPrefix(qname),
	}
	res, err := getTaskInfoCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		if err.Error() == "NOT FOUND" {
			return nil, errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: id.String()})
		}
		return nil, errors.E(op, errors.Unknown, err)
	}
	vals, err := cast.ToSliceE(res)
	if err != nil {
		return nil, errors.E(op, errors.Internal, "unexpected value returned from Lua script")
	}
	if len(vals) != 3 {
		return nil, errors.E(op, errors.Internal, "unepxected number of values returned from Lua script")
	}
	encoded, err := cast.ToStringE(vals[0])
	if err != nil {
		return nil, errors.E(op, errors.Internal, "unexpected value returned from Lua script")
	}
	stateStr, err := cast.ToStringE(vals[1])
	if err != nil {
		return nil, errors.E(op, errors.Internal, "unexpected value returned from Lua script")
	}
	processAtUnix, err := cast.ToInt64E(vals[2])
	if err != nil {
		return nil, errors.E(op, errors.Internal, "unexpected value returned from Lua script")
	}
	msg, err := base.DecodeMessage([]byte(encoded))
	if err != nil {
		return nil, errors.E(op, errors.Internal, "could not decode task message")
	}
	state, err := base.TaskStateFromString(stateStr)
	if err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	var nextProcessAt time.Time
	if processAtUnix != 0 {
		nextProcessAt = time.Unix(processAtUnix, 0)
	}
	return &base.TaskInfo{
		Message:       msg,
		State:         state,
		NextProcessAt: nextProcessAt,
	}, nil
}

// Pagination specifies the page size and page number
// for the list operation.
type Pagination struct {
	// Number of items in the page.
	Size int

	// Page number starting from zero.
	Page int
}

func (p Pagination) start() int64 {
	return int64(p.Size * p.Page)
}

func (p Pagination) stop() int64 {
	return int64(p.Size*p.Page + p.Size - 1)
}

// ListPending returns pending tasks that are ready to be processed.
func (r *RDB) ListPending(qname string, pgn Pagination) ([]*base.TaskMessage, error) {
	var op errors.Op = "rdb.ListPending"
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	res, err := r.listMessages(base.PendingKey(qname), qname, pgn)
	if err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	return res, nil
}

// ListActive returns all tasks that are currently being processed for the given queue.
func (r *RDB) ListActive(qname string, pgn Pagination) ([]*base.TaskMessage, error) {
	var op errors.Op = "rdb.ListActive"
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	res, err := r.listMessages(base.ActiveKey(qname), qname, pgn)
	if err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	return res, nil
}

// KEYS[1] -> key for id list (e.g. asynq:{<qname>}:pending)
// ARGV[1] -> start offset
// ARGV[2] -> stop offset
// ARGV[3] -> task key prefix
var listMessagesCmd = redis.NewScript(`
local ids = redis.call("LRange", KEYS[1], ARGV[1], ARGV[2])
local res = {}
for _, id in ipairs(ids) do
	local key = ARGV[3] .. id
	table.insert(res, redis.call("HGET", key, "msg"))
end
return res
`)

// listMessages returns a list of TaskMessage in Redis list with the given key.
func (r *RDB) listMessages(key, qname string, pgn Pagination) ([]*base.TaskMessage, error) {
	// Note: Because we use LPUSH to redis list, we need to calculate the
	// correct range and reverse the list to get the tasks with pagination.
	stop := -pgn.start() - 1
	start := -pgn.stop() - 1
	res, err := listMessagesCmd.Run(r.client,
		[]string{key}, start, stop, base.TaskKeyPrefix(qname)).Result()
	if err != nil {
		return nil, errors.E(errors.Unknown, err)
	}
	data, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, errors.E(errors.Internal, fmt.Errorf("cast error: Lua script returned unexpected value: %v", res))
	}
	reverse(data)
	var msgs []*base.TaskMessage
	for _, s := range data {
		m, err := base.DecodeMessage([]byte(s))
		if err != nil {
			continue // bad data, ignore and continue
		}
		msgs = append(msgs, m)
	}
	return msgs, nil

}

// ListScheduled returns all tasks from the given queue that are scheduled
// to be processed in the future.
func (r *RDB) ListScheduled(qname string, pgn Pagination) ([]base.Z, error) {
	var op errors.Op = "rdb.ListScheduled"
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	res, err := r.listZSetEntries(base.ScheduledKey(qname), qname, pgn)
	if err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	return res, nil
}

// ListRetry returns all tasks from the given queue that have failed before
// and willl be retried in the future.
func (r *RDB) ListRetry(qname string, pgn Pagination) ([]base.Z, error) {
	var op errors.Op = "rdb.ListRetry"
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	res, err := r.listZSetEntries(base.RetryKey(qname), qname, pgn)
	if err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	return res, nil
}

// ListArchived returns all tasks from the given queue that have exhausted its retry limit.
func (r *RDB) ListArchived(qname string, pgn Pagination) ([]base.Z, error) {
	var op errors.Op = "rdb.ListArchived"
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	zs, err := r.listZSetEntries(base.ArchivedKey(qname), qname, pgn)
	if err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	return zs, nil
}

// KEYS[1] -> key for ids set (e.g. asynq:{<qname>}:scheduled)
// ARGV[1] -> min
// ARGV[2] -> max
// ARGV[3] -> task key prefix
//
// Returns an array populated with
// [msg1, score1, msg2, score2, ..., msgN, scoreN]
var listZSetEntriesCmd = redis.NewScript(`
local res = {}
local id_score_pairs = redis.call("ZRANGE", KEYS[1], ARGV[1], ARGV[2], "WITHSCORES")
for i = 1, table.getn(id_score_pairs), 2 do
	local key = ARGV[3] .. id_score_pairs[i]
	table.insert(res, redis.call("HGET", key, "msg"))
	table.insert(res, id_score_pairs[i+1])
end
return res
`)

// listZSetEntries returns a list of message and score pairs in Redis sorted-set
// with the given key.
func (r *RDB) listZSetEntries(key, qname string, pgn Pagination) ([]base.Z, error) {
	res, err := listZSetEntriesCmd.Run(r.client, []string{key},
		pgn.start(), pgn.stop(), base.TaskKeyPrefix(qname)).Result()
	if err != nil {
		return nil, errors.E(errors.Unknown, err)
	}
	data, err := cast.ToSliceE(res)
	if err != nil {
		return nil, errors.E(errors.Internal, fmt.Errorf("cast error: Lua script returned unexpected value: %v", res))
	}
	var zs []base.Z
	for i := 0; i < len(data); i += 2 {
		s, err := cast.ToStringE(data[i])
		if err != nil {
			return nil, errors.E(errors.Internal, fmt.Errorf("cast error: Lua script returned unexpected value: %v", res))
		}
		score, err := cast.ToInt64E(data[i+1])
		if err != nil {
			return nil, errors.E(errors.Internal, fmt.Errorf("cast error: Lua script returned unexpected value: %v", res))
		}
		msg, err := base.DecodeMessage([]byte(s))
		if err != nil {
			continue // bad data, ignore and continue
		}
		zs = append(zs, base.Z{Message: msg, Score: score})
	}
	return zs, nil
}

// RunAllScheduledTasks enqueues all scheduled tasks from the given queue
// and returns the number of tasks enqueued.
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
func (r *RDB) RunAllScheduledTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.RunAllScheduledTasks"
	n, err := r.runAll(base.ScheduledKey(qname), qname)
	if errors.IsQueueNotFound(err) {
		return 0, errors.E(op, errors.NotFound, err)
	}
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return n, nil
}

// RunAllRetryTasks enqueues all retry tasks from the given queue
// and returns the number of tasks enqueued.
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
func (r *RDB) RunAllRetryTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.RunAllRetryTasks"
	n, err := r.runAll(base.RetryKey(qname), qname)
	if errors.IsQueueNotFound(err) {
		return 0, errors.E(op, errors.NotFound, err)
	}
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return n, nil
}

// RunAllArchivedTasks enqueues all archived tasks from the given queue
// and returns the number of tasks enqueued.
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
func (r *RDB) RunAllArchivedTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.RunAllArchivedTasks"
	n, err := r.runAll(base.ArchivedKey(qname), qname)
	if errors.IsQueueNotFound(err) {
		return 0, errors.E(op, errors.NotFound, err)
	}
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return n, nil
}

// runTaskCmd is a Lua script that updates the given task to pending state.
//
// Input:
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:pending
// --
// ARGV[1] -> task ID
// ARGV[2] -> queue key prefix; asynq:{<qname>}:
//
// Output:
// Numeric code indicating the status:
// Returns 1 if task is successfully updated.
// Returns 0 if task is not found.
// Returns -1 if task is in active state.
// Returns -2 if task is in pending state.
// Returns error reply if unexpected error occurs.
var runTaskCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state = redis.call("HGET", KEYS[1], "state")
if state == "active" then
	return -1
elseif state == "pending" then
	return -2
end
local n = redis.call("ZREM", ARGV[2] .. state, ARGV[1])
if n == 0 then
	return redis.error_reply("internal error: task id not found in zset " .. tostring(state))
end
redis.call("LPUSH", KEYS[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "pending")
return 1
`)

// RunTask finds a task that matches the id from the given queue and updates it to pending state.
// It returns nil if it successfully updated the task.
//
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
// If a task with the given id doesn't exist in the queue, it returns TaskNotFoundError
// If a task is in active or pending state it returns non-nil error with Code FailedPrecondition.
func (r *RDB) RunTask(qname string, id uuid.UUID) error {
	var op errors.Op = "rdb.RunTask"
	if err := r.checkQueueExists(qname); err != nil {
		return errors.E(op, errors.CanonicalCode(err), err)
	}
	keys := []string{
		base.TaskKey(qname, id.String()),
		base.PendingKey(qname),
	}
	argv := []interface{}{
		id.String(),
		base.QueueKeyPrefix(qname),
	}
	res, err := runTaskCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, err)
	}
	n, ok := res.(int64)
	if !ok {
		return errors.E(op, errors.Internal, fmt.Sprintf("cast error: unexpected return value from Lua script: %v", res))
	}
	switch n {
	case 1:
		return nil
	case 0:
		return errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: id.String()})
	case -1:
		return errors.E(op, errors.FailedPrecondition, "task is already running")
	case -2:
		return errors.E(op, errors.FailedPrecondition, "task is already in pending state")
	default:
		return errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script %d", n))
	}
}

// runAllCmd is a Lua script that moves all tasks in the given state
// (one of: scheduled, retry, archived) to pending state.
//
// Input:
// KEYS[1] -> zset which holds task ids (e.g. asynq:{<qname>}:scheduled)
// KEYS[2] -> asynq:{<qname>}:pending
// --
// ARGV[1] -> task key prefix
//
// Output:
// integer: number of tasks updated to pending state.
var runAllCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("LPUSH", KEYS[2], id)
	redis.call("HSET", ARGV[1] .. id, "state", "pending")
end
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

func (r *RDB) runAll(zset, qname string) (int64, error) {
	if err := r.checkQueueExists(qname); err != nil {
		return 0, err
	}
	keys := []string{
		zset,
		base.PendingKey(qname),
	}
	argv := []interface{}{
		base.TaskKeyPrefix(qname),
	}
	res, err := runAllCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	if n == -1 {
		return 0, &errors.QueueNotFoundError{Queue: qname}
	}
	return n, nil
}

// ArchiveAllRetryTasks archives all retry tasks from the given queue and
// returns the number of tasks that were moved.
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
func (r *RDB) ArchiveAllRetryTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.ArchiveAllRetryTasks"
	n, err := r.archiveAll(base.RetryKey(qname), base.ArchivedKey(qname), qname)
	if errors.IsQueueNotFound(err) {
		return 0, errors.E(op, errors.NotFound, err)
	}
	if err != nil {
		return 0, errors.E(op, errors.Internal, err)
	}
	return n, nil
}

// ArchiveAllScheduledTasks archives all scheduled tasks from the given queue and
// returns the number of tasks that were moved.
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
func (r *RDB) ArchiveAllScheduledTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.ArchiveAllScheduledTasks"
	n, err := r.archiveAll(base.ScheduledKey(qname), base.ArchivedKey(qname), qname)
	if errors.IsQueueNotFound(err) {
		return 0, errors.E(op, errors.NotFound, err)
	}
	if err != nil {
		return 0, errors.E(op, errors.Internal, err)
	}
	return n, nil
}

// archiveAllPendingCmd is a Lua script that moves all pending tasks from
// the given queue to archived state.
//
// Input:
// KEYS[1] -> asynq:{<qname>}:pending
// KEYS[2] -> asynq:{<qname>}:archived
// --
// ARGV[1] -> current timestamp
// ARGV[2] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[3] -> max number of tasks in archive (e.g., 100)
// ARGV[4] -> task key prefix (asynq:{<qname>}:t:)
//
// Output:
// integer: Number of tasks archived
var archiveAllPendingCmd = redis.NewScript(`
local ids = redis.call("LRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("ZADD", KEYS[2], ARGV[1], id)
	redis.call("HSET", ARGV[4] .. id, "state", "archived")
end
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

// ArchiveAllPendingTasks archives all pending tasks from the given queue and
// returns the number of tasks moved.
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
func (r *RDB) ArchiveAllPendingTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.ArchiveAllPendingTasks"
	if err := r.checkQueueExists(qname); err != nil {
		return 0, errors.E(op, errors.CanonicalCode(err), err)
	}
	keys := []string{
		base.PendingKey(qname),
		base.ArchivedKey(qname),
	}
	now := time.Now()
	argv := []interface{}{
		now.Unix(),
		now.AddDate(0, 0, -archivedExpirationInDays).Unix(),
		maxArchiveSize,
		base.TaskKeyPrefix(qname),
	}
	res, err := archiveAllPendingCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return 0, errors.E(op, errors.Internal, err)
	}
	n, ok := res.(int64)
	if !ok {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from script %v", res))
	}
	return n, nil
}

// archiveTaskCmd is a Lua script that archives a task given a task id.
//
// Input:
// KEYS[1] -> task key (asynq:{<qname>}:t:<task_id>)
// KEYS[2] -> archived key (asynq:{<qname>}:archived)
// --
// ARGV[1] -> id of the task to archive
// ARGV[2] -> current timestamp
// ARGV[3] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[4] -> max number of tasks in archived state (e.g., 100)
// ARGV[5] -> queue key prefix (asynq:{<qname>}:)
//
// Output:
// Numeric code indicating the status:
// Returns 1 if task is successfully archived.
// Returns 0 if task is not found.
// Returns -1 if task is already archived.
// Returns -2 if task is in active state.
// Returns error reply if unexpected error occurs.
var archiveTaskCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state = redis.call("HGET", KEYS[1], "state")
if state == "active" then
	return -2
end
if state == "archived" then
	return -1
end
if state == "pending" then
	if redis.call("LREM", ARGV[5] .. state, 1, ARGV[1]) == 0 then
		return redis.error_reply("task id not found in list " .. tostring(state))
	end
else 
	if redis.call("ZREM", ARGV[5] .. state, ARGV[1]) == 0 then
		return redis.error_reply("task id not found in zset " .. tostring(state))
	end
end
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "archived")
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[3])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[4])
return 1
`)

// ArchiveTask finds a task that matches the id from the given queue and archives it.
// It returns nil if it successfully archived the task.
//
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
// If a task with the given id doesn't exist in the queue, it returns TaskNotFoundError
// If a task is already archived, it returns TaskAlreadyArchivedError.
// If a task is in active state it returns non-nil error with FailedPrecondition code.
func (r *RDB) ArchiveTask(qname string, id uuid.UUID) error {
	var op errors.Op = "rdb.ArchiveTask"
	if err := r.checkQueueExists(qname); err != nil {
		return errors.E(op, errors.CanonicalCode(err), err)
	}
	keys := []string{
		base.TaskKey(qname, id.String()),
		base.ArchivedKey(qname),
	}
	now := time.Now()
	argv := []interface{}{
		id.String(),
		now.Unix(),
		now.AddDate(0, 0, -archivedExpirationInDays).Unix(),
		maxArchiveSize,
		base.QueueKeyPrefix(qname),
	}
	res, err := archiveTaskCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, err)
	}
	n, ok := res.(int64)
	if !ok {
		return errors.E(op, errors.Internal, fmt.Sprintf("could not cast the return value %v from archiveTaskCmd to int64.", res))
	}
	switch n {
	case 1:
		return nil
	case 0:
		return errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: id.String()})
	case -1:
		return errors.E(op, errors.FailedPrecondition, &errors.TaskAlreadyArchivedError{Queue: qname, ID: id.String()})
	case -2:
		return errors.E(op, errors.FailedPrecondition, "cannot archive task in active state. use CancelTask instead.")
	case -3:
		return errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	default:
		return errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from archiveTaskCmd script: %d", n))
	}
}

// archiveAllCmd is a Lua script that archives all tasks in either scheduled
// or retry state from the given queue.
//
// Input:
// KEYS[1] -> ZSET to move task from (e.g., asynq:{<qname>}:retry)
// KEYS[2] -> asynq:{<qname>}:archived
// --
// ARGV[1] -> current timestamp
// ARGV[2] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[3] -> max number of tasks in archive (e.g., 100)
// ARGV[4] -> task key prefix (asynq:{<qname>}:t:)
//
// Output:
// integer: number of tasks archived
var archiveAllCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("ZADD", KEYS[2], ARGV[1], id)
	redis.call("HSET", ARGV[4] .. id, "state", "archived")
end
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

func (r *RDB) archiveAll(src, dst, qname string) (int64, error) {
	if err := r.checkQueueExists(qname); err != nil {
		return 0, err
	}
	keys := []string{
		src,
		dst,
		base.AllQueues,
	}
	now := time.Now()
	argv := []interface{}{
		now.Unix(),
		now.AddDate(0, 0, -archivedExpirationInDays).Unix(),
		maxArchiveSize,
		base.TaskKeyPrefix(qname),
		qname,
	}
	res, err := archiveAllCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected return value from script: %v", res)
	}
	if n == -1 {
		return 0, &errors.QueueNotFoundError{Queue: qname}
	}
	return n, nil
}

// Input:
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// --
// ARGV[1] -> task ID
// ARGV[2] -> queue key prefix
//
// Output:
// Numeric code indicating the status:
// Returns 1 if task is successfully deleted.
// Returns 0 if task is not found.
// Returns -1 if task is in active state.
var deleteTaskCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state = redis.call("HGET", KEYS[1], "state")
if state == "active" then
	return -1
end
if state == "pending" then
	if redis.call("LREM", ARGV[2] .. state, 0, ARGV[1]) == 0 then
		return redis.error_reply("task is not found in list: " .. tostring(state))
	end
else
	if redis.call("ZREM", ARGV[2] .. state, ARGV[1]) == 0 then
		return redis.error_reply("task is not found in zset: " .. tostring(state))
	end
end
local unique_key = redis.call("HGET", KEYS[1], "unique_key")
if unique_key ~= nil and unique_key ~= "" and redis.call("GET", unique_key) == ARGV[1] then
	redis.call("DEL", unique_key)
end
return redis.call("DEL", KEYS[1])
`)

// DeleteTask finds a task that matches the id from the given queue and deletes it.
// It returns nil if it successfully archived the task.
//
// If a queue with the given name doesn't exist, it returns QueueNotFoundError.
// If a task with the given id doesn't exist in the queue, it returns TaskNotFoundError
// If a task is in active state it returns non-nil error with Code FailedPrecondition.
func (r *RDB) DeleteTask(qname string, id uuid.UUID) error {
	var op errors.Op = "rdb.DeleteTask"
	if err := r.checkQueueExists(qname); err != nil {
		return errors.E(op, errors.CanonicalCode(err), err)
	}
	keys := []string{
		base.TaskKey(qname, id.String()),
	}
	argv := []interface{}{
		id.String(),
		base.QueueKeyPrefix(qname),
	}
	res, err := deleteTaskCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, err)
	}
	n, ok := res.(int64)
	if !ok {
		return errors.E(op, errors.Internal, fmt.Sprintf("cast error: deleteTaskCmd script returned unexported value %v", res))
	}
	switch n {
	case 1:
		return nil
	case 0:
		return errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: id.String()})
	case -1:
		return errors.E(op, errors.FailedPrecondition, "cannot delete task in active state. use CancelTask instead.")
	default:
		return errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from deleteTaskCmd script: %d", n))
	}
}

// DeleteAllArchivedTasks deletes all archived tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllArchivedTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.DeleteAllArchivedTasks"
	n, err := r.deleteAll(base.ArchivedKey(qname), qname)
	if errors.IsQueueNotFound(err) {
		return 0, errors.E(op, errors.NotFound, err)
	}
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return n, nil
}

// DeleteAllRetryTasks deletes all retry tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllRetryTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.DeleteAllRetryTasks"
	n, err := r.deleteAll(base.RetryKey(qname), qname)
	if errors.IsQueueNotFound(err) {
		return 0, errors.E(op, errors.NotFound, err)
	}
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return n, nil
}

// DeleteAllScheduledTasks deletes all scheduled tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllScheduledTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.DeleteAllScheduledTasks"
	n, err := r.deleteAll(base.ScheduledKey(qname), qname)
	if errors.IsQueueNotFound(err) {
		return 0, errors.E(op, errors.NotFound, err)
	}
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	return n, nil
}

// deleteAllCmd deletes tasks from the given zset.
//
// Input:
// KEYS[1] -> zset holding the task ids.
// --
// ARGV[1] -> task key prefix
//
// Output:
// integer: number of tasks deleted
var deleteAllCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	local task_key = ARGV[1] .. id
	local unique_key = redis.call("HGET", task_key, "unique_key")
	if unique_key ~= nil and unique_key ~= "" and redis.call("GET", unique_key) == id then
		redis.call("DEL", unique_key)
	end
	redis.call("DEL", task_key)
end
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

func (r *RDB) deleteAll(key, qname string) (int64, error) {
	if err := r.checkQueueExists(qname); err != nil {
		return 0, err
	}
	keys := []string{
		key,
		base.AllQueues,
	}
	argv := []interface{}{
		base.TaskKeyPrefix(qname),
		qname,
	}
	res, err := deleteAllCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected return value from Lua script: %v", res)
	}
	return n, nil
}

// deleteAllPendingCmd deletes all pending tasks from the given queue.
//
// Input:
// KEYS[1] -> asynq:{<qname>}:pending
// --
// ARGV[1] -> task key prefix
//
// Output:
// integer: number of tasks deleted
var deleteAllPendingCmd = redis.NewScript(`
local ids = redis.call("LRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

// DeleteAllPendingTasks deletes all pending tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllPendingTasks(qname string) (int64, error) {
	var op errors.Op = "rdb.DeleteAllPendingTasks"
	if err := r.checkQueueExists(qname); err != nil {
		return 0, errors.E(op, errors.CanonicalCode(err), err)
	}
	keys := []string{
		base.PendingKey(qname),
	}
	argv := []interface{}{
		base.TaskKeyPrefix(qname),
	}
	res, err := deleteAllPendingCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return 0, errors.E(op, errors.Unknown, err)
	}
	n, ok := res.(int64)
	if !ok {
		return 0, errors.E(op, errors.Internal, "command error: unexpected return value %v", res)
	}
	return n, nil
}

// removeQueueForceCmd removes the given queue regardless of
// whether the queue is empty.
// It only check whether active queue is empty before removing.
//
// Input:
// KEYS[1] -> asynq:{<qname>}
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:scheduled
// KEYS[4] -> asynq:{<qname>}:retry
// KEYS[5] -> asynq:{<qname>}:archived
// KEYS[6] -> asynq:{<qname>}:deadlines
// --
// ARGV[1] -> task key prefix
//
// Output:
// Numeric code to indicate the status.
// Returns 1 if successfully removed.
// Returns -2 if the queue has active tasks.
var removeQueueForceCmd = redis.NewScript(`
local active = redis.call("LLEN", KEYS[2])
if active > 0 then
    return -2
end
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
redis.call("DEL", KEYS[5])
redis.call("DEL", KEYS[6])
return 1`)

// removeQueueCmd removes the given queue.
// It checks whether queue is empty before removing.
//
// Input:
// KEYS[1] -> asynq:{<qname>}:pending
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:scheduled
// KEYS[4] -> asynq:{<qname>}:retry
// KEYS[5] -> asynq:{<qname>}:archived
// KEYS[6] -> asynq:{<qname>}:deadlines
// --
// ARGV[1] -> task key prefix
//
// Output:
// Numeric code to indicate the status
// Returns 1 if successfully removed.
// Returns -1 if queue is not empty
var removeQueueCmd = redis.NewScript(`
local ids = {}
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
	table.insert(ids, id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
	table.insert(ids, id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
	table.insert(ids, id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
	table.insert(ids, id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
	table.insert(ids, id)
end
if table.getn(ids) > 0 then
	return -1
end
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
redis.call("DEL", KEYS[5])
redis.call("DEL", KEYS[6])
return 1`)

// RemoveQueue removes the specified queue.
//
// If force is set to true, it will remove the queue regardless
// as long as no tasks are active for the queue.
// If force is set to false, it will only remove the queue if
// the queue is empty.
func (r *RDB) RemoveQueue(qname string, force bool) error {
	var op errors.Op = "rdb.RemoveQueue"
	exists, err := r.client.SIsMember(base.AllQueues, qname).Result()
	if err != nil {
		return err
	}
	if !exists {
		return errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}
	var script *redis.Script
	if force {
		script = removeQueueForceCmd
	} else {
		script = removeQueueCmd
	}
	keys := []string{
		base.PendingKey(qname),
		base.ActiveKey(qname),
		base.ScheduledKey(qname),
		base.RetryKey(qname),
		base.ArchivedKey(qname),
		base.DeadlinesKey(qname),
	}
	res, err := script.Run(r.client, keys, base.TaskKeyPrefix(qname)).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, err)
	}
	n, ok := res.(int64)
	if !ok {
		return errors.E(op, errors.Internal, fmt.Sprintf("unexpeced return value from Lua script: %v", res))
	}
	switch n {
	case 1:
		if err := r.client.SRem(base.AllQueues, qname).Err(); err != nil {
			return errors.E(op, errors.Unknown, err)
		}
		return nil
	case -1:
		return errors.E(op, errors.NotFound, &errors.QueueNotEmptyError{Queue: qname})
	case -2:
		return errors.E(op, errors.FailedPrecondition, "cannot remove queue with active tasks")
	default:
		return errors.E(op, errors.Unknown, fmt.Sprintf("unexpected return value from Lua script: %d", n))
	}
}

// Note: Script also removes stale keys.
var listServerKeysCmd = redis.NewScript(`
local now = tonumber(ARGV[1])
local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
return keys`)

// ListServers returns the list of server info.
func (r *RDB) ListServers() ([]*base.ServerInfo, error) {
	now := time.Now()
	res, err := listServerKeysCmd.Run(r.client, []string{base.AllServers}, now.Unix()).Result()
	if err != nil {
		return nil, err
	}
	keys, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, err
	}
	var servers []*base.ServerInfo
	for _, key := range keys {
		data, err := r.client.Get(key).Result()
		if err != nil {
			continue // skip bad data
		}
		info, err := base.DecodeServerInfo([]byte(data))
		if err != nil {
			continue // skip bad data
		}
		servers = append(servers, info)
	}
	return servers, nil
}

// Note: Script also removes stale keys.
var listWorkersCmd = redis.NewScript(`
local now = tonumber(ARGV[1])
local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
local res = {}
for _, key in ipairs(keys) do
	local vals = redis.call("HVALS", key)
	for _, v in ipairs(vals) do
		table.insert(res, v)
	end
end
return res`)

// ListWorkers returns the list of worker stats.
func (r *RDB) ListWorkers() ([]*base.WorkerInfo, error) {
	now := time.Now()
	res, err := listWorkersCmd.Run(r.client, []string{base.AllWorkers}, now.Unix()).Result()
	if err != nil {
		return nil, err
	}
	data, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, err
	}
	var workers []*base.WorkerInfo
	for _, s := range data {
		w, err := base.DecodeWorkerInfo([]byte(s))
		if err != nil {
			continue // skip bad data
		}
		workers = append(workers, w)
	}
	return workers, nil
}

// Note: Script also removes stale keys.
var listSchedulerKeysCmd = redis.NewScript(`
local now = tonumber(ARGV[1])
local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
return keys`)

// ListSchedulerEntries returns the list of scheduler entries.
func (r *RDB) ListSchedulerEntries() ([]*base.SchedulerEntry, error) {
	now := time.Now()
	res, err := listSchedulerKeysCmd.Run(r.client, []string{base.AllSchedulers}, now.Unix()).Result()
	if err != nil {
		return nil, err
	}
	keys, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, err
	}
	var entries []*base.SchedulerEntry
	for _, key := range keys {
		data, err := r.client.LRange(key, 0, -1).Result()
		if err != nil {
			continue // skip bad data
		}
		for _, s := range data {
			e, err := base.DecodeSchedulerEntry([]byte(s))
			if err != nil {
				continue // skip bad data
			}
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// ListSchedulerEnqueueEvents returns the list of scheduler enqueue events.
func (r *RDB) ListSchedulerEnqueueEvents(entryID string, pgn Pagination) ([]*base.SchedulerEnqueueEvent, error) {
	key := base.SchedulerHistoryKey(entryID)
	zs, err := r.client.ZRevRangeWithScores(key, pgn.start(), pgn.stop()).Result()
	if err != nil {
		return nil, err
	}
	var events []*base.SchedulerEnqueueEvent
	for _, z := range zs {
		data, err := cast.ToStringE(z.Member)
		if err != nil {
			return nil, err
		}
		e, err := base.DecodeSchedulerEnqueueEvent([]byte(data))
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, nil
}

// Pause pauses processing of tasks from the given queue.
func (r *RDB) Pause(qname string) error {
	key := base.PausedKey(qname)
	ok, err := r.client.SetNX(key, time.Now().Unix(), 0).Result()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("queue %q is already paused", qname)
	}
	return nil
}

// Unpause resumes processing of tasks from the given queue.
func (r *RDB) Unpause(qname string) error {
	key := base.PausedKey(qname)
	deleted, err := r.client.Del(key).Result()
	if err != nil {
		return err
	}
	if deleted == 0 {
		return fmt.Errorf("queue %q is not paused", qname)
	}
	return nil
}

// ClusterKeySlot returns an integer identifying the hash slot the given queue hashes to.
func (r *RDB) ClusterKeySlot(qname string) (int64, error) {
	key := base.PendingKey(qname)
	return r.client.ClusterKeySlot(key).Result()
}

// ClusterNodes returns a list of nodes the given queue belongs to.
func (r *RDB) ClusterNodes(qname string) ([]redis.ClusterNode, error) {
	keyslot, err := r.ClusterKeySlot(qname)
	if err != nil {
		return nil, err
	}
	clusterSlots, err := r.client.ClusterSlots().Result()
	if err != nil {
		return nil, err
	}
	for _, slotRange := range clusterSlots {
		if int64(slotRange.Start) <= keyslot && keyslot <= int64(slotRange.End) {
			return slotRange.Nodes, nil
		}
	}
	return nil, fmt.Errorf("nodes not found")
}
