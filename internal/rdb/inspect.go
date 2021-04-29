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
	exists, err := r.client.SIsMember(base.AllQueues, qname).Result()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &ErrQueueNotFound{qname}
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
		return nil, err
	}
	data, err := cast.ToSliceE(res)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	stats.MemoryUsage = memusg
	return stats, nil
}

func (r *RDB) memoryUsage(qname string) (int64, error) {
	var (
		keys   []string
		data   []string
		cursor uint64
		err    error
	)
	for {
		data, cursor, err = r.client.Scan(cursor, fmt.Sprintf("asynq:{%s}*", qname), 100).Result()
		if err != nil {
			return 0, err
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
			return 0, err
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
	if n < 1 {
		return nil, fmt.Errorf("the number of days must be positive")
	}
	exists, err := r.client.SIsMember(base.AllQueues, qname).Result()
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, &ErrQueueNotFound{qname}
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
		return nil, err
	}
	data, err := cast.ToIntSliceE(res)
	if err != nil {
		return nil, err
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
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, fmt.Errorf("queue %q does not exist", qname)
	}
	return r.listMessages(base.PendingKey(qname), qname, pgn)
}

// ListActive returns all tasks that are currently being processed for the given queue.
func (r *RDB) ListActive(qname string, pgn Pagination) ([]*base.TaskMessage, error) {
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, fmt.Errorf("queue %q does not exist", qname)
	}
	return r.listMessages(base.ActiveKey(qname), qname, pgn)
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
		return nil, err
	}
	data, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, err
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
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, fmt.Errorf("queue %q does not exist", qname)
	}
	return r.listZSetEntries(base.ScheduledKey(qname), qname, pgn)
}

// ListRetry returns all tasks from the given queue that have failed before
// and willl be retried in the future.
func (r *RDB) ListRetry(qname string, pgn Pagination) ([]base.Z, error) {
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, fmt.Errorf("queue %q does not exist", qname)
	}
	return r.listZSetEntries(base.RetryKey(qname), qname, pgn)
}

// ListArchived returns all tasks from the given queue that have exhausted its retry limit.
func (r *RDB) ListArchived(qname string, pgn Pagination) ([]base.Z, error) {
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, fmt.Errorf("queue %q does not exist", qname)
	}
	return r.listZSetEntries(base.ArchivedKey(qname), qname, pgn)
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
		return nil, err
	}
	data, err := cast.ToSliceE(res)
	if err != nil {
		return nil, err
	}
	var zs []base.Z
	for i := 0; i < len(data); i += 2 {
		s, err := cast.ToStringE(data[i])
		if err != nil {
			return nil, err
		}
		score, err := cast.ToInt64E(data[i+1])
		if err != nil {
			return nil, err
		}
		msg, err := base.DecodeMessage([]byte(s))
		if err != nil {
			continue // bad data, ignore and continue
		}
		zs = append(zs, base.Z{Message: msg, Score: score})
	}
	return zs, nil
}

// RunTask finds a task that matches the id from the given queue and stages it for processing.
// If a task that matches the id does not exist, it returns ErrTaskNotFound.
func (r *RDB) RunTask(qname string, id uuid.UUID) error {
	n, err := r.runTask(qname, id.String())
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// RunAllScheduledTasks enqueues all scheduled tasks from the given queue
// and returns the number of tasks enqueued.
func (r *RDB) RunAllScheduledTasks(qname string) (int64, error) {
	return r.removeAndRunAll(base.ScheduledKey(qname), base.PendingKey(qname))
}

// RunAllRetryTasks enqueues all retry tasks from the given queue
// and returns the number of tasks enqueued.
func (r *RDB) RunAllRetryTasks(qname string) (int64, error) {
	return r.removeAndRunAll(base.RetryKey(qname), base.PendingKey(qname))
}

// RunAllArchivedTasks enqueues all archived tasks from the given queue
// and returns the number of tasks enqueued.
func (r *RDB) RunAllArchivedTasks(qname string) (int64, error) {
	return r.removeAndRunAll(base.ArchivedKey(qname), base.PendingKey(qname))
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:pending
// ARGV[1] -> task ID
// ARGV[2] -> queue key prefix; asynq:{<qname>}:
var runTaskCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state = redis.call("HGET", KEYS[1], "state")
if state == "active" then
	return redis.error_reply("task is already running")
elseif state == "pending" then
	return redis.error_reply("task is already pending to be run")
end
local n = redis.call("ZREM", ARGV[2] .. state, ARGV[1])
if n == 0 then
	return redis.error_reply("internal error: task id not found in zset " .. tostring(state))
end
redis.call("LPUSH", KEYS[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "pending")
return 1
`)

func (r *RDB) runTask(qname, id string) (int64, error) {
	keys := []string{
		base.TaskKey(qname, id),
		base.PendingKey(qname),
	}
	argv := []interface{}{
		id,
		base.QueueKeyPrefix(qname),
	}
	res, err := runTaskCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("internal error: could not cast %v to int64", res)
	}
	return n, nil
}

var removeAndRunAllCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("LPUSH", KEYS[2], id)
	redis.call("ZREM", KEYS[1], id)
end
return table.getn(ids)`)

func (r *RDB) removeAndRunAll(zset, qkey string) (int64, error) {
	res, err := removeAndRunAllCmd.Run(r.client, []string{zset, qkey}).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	return n, nil
}

// ArchiveTask finds a task that matches the id from the given queue and archives it.
// If there's no match, it returns ErrTaskNotFound.
func (r *RDB) ArchiveTask(qname string, id uuid.UUID) error {
	n, err := r.archiveTask(qname, id.String())
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// ArchiveAllRetryTasks archives all retry tasks from the given queue and
// returns the number of tasks that were moved.
func (r *RDB) ArchiveAllRetryTasks(qname string) (int64, error) {
	return r.removeAndArchiveAll(base.RetryKey(qname), base.ArchivedKey(qname))
}

// ArchiveAllScheduledTasks archives all scheduled tasks from the given queue and
// returns the number of tasks that were moved.
func (r *RDB) ArchiveAllScheduledTasks(qname string) (int64, error) {
	return r.removeAndArchiveAll(base.ScheduledKey(qname), base.ArchivedKey(qname))
}

// KEYS[1] -> asynq:{<qname>}:pending
// KEYS[2] -> asynq:{<qname>}:archived
// ARGV[1] -> current timestamp
// ARGV[2] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[3] -> max number of tasks in archive (e.g., 100)
var archiveAllPendingCmd = redis.NewScript(`
local ids = redis.call("LRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("ZADD", KEYS[2], ARGV[1], id)
	redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
	redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
end
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

// ArchiveAllPendingTasks archives all pending tasks from the given queue and
// returns the number of tasks moved.
func (r *RDB) ArchiveAllPendingTasks(qname string) (int64, error) {
	keys := []string{base.PendingKey(qname), base.ArchivedKey(qname)}
	now := time.Now()
	argv := []interface{}{
		now.Unix(),
		now.AddDate(0, 0, -archivedExpirationInDays).Unix(),
		maxArchiveSize,
	}
	res, err := archiveAllPendingCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("command error: unexpected return value %v", res)
	}
	return n, nil
}

// KEYS[1] -> task key (asynq:{<qname>}:t:<task_id>)
// KEYS[2] -> archived key (asynq:{<qname>}:archived)
// ARGV[1] -> id of the task to archive
// ARGV[2] -> current timestamp
// ARGV[3] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[4] -> max number of tasks in archived state (e.g., 100)
// ARGV[5] -> queue key prefix (asynq:{<qname>}:)
var archiveTaskCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state = redis.call("HGET", KEYS[1], "state")
if state == "active" then
	return redis.error_reply("Cannot archive active task. Use cancel instead.")
end
if state == "archived" then
	return redis.error_reply("Task is already archived")
end
if state == "pending" then
	if redis.call("LREM", ARGV[5] .. state, 1, ARGV[1]) == 0 then
		return redis.error_reply("internal error: task id not found in list " .. tostring(state))
	end
else 
	if redis.call("ZREM", ARGV[5] .. state, ARGV[1]) == 0 then
		return redis.error_reply("internal error: task id not found in zset " .. tostring(state))
	end
end
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "archived")
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[3])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[4])
return 1
`)

func (r *RDB) archiveTask(qname, id string) (int64, error) {
	keys := []string{
		base.TaskKey(qname, id),
		base.ArchivedKey(qname),
	}
	now := time.Now()
	argv := []interface{}{
		id,
		now.Unix(),
		now.AddDate(0, 0, -archivedExpirationInDays).Unix(),
		maxArchiveSize,
		base.QueueKeyPrefix(qname),
	}
	res, err := archiveTaskCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("internal error: could not cast %v to int64", res)
	}
	return n, nil
}

// KEYS[1] -> ZSET to move task from (e.g., asynq:{<qname>}:retry)
// KEYS[2] -> asynq:{<qname>}:archived
// ARGV[1] -> current timestamp
// ARGV[2] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[3] -> max number of tasks in archive (e.g., 100)
var removeAndArchiveAllCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("ZADD", KEYS[2], ARGV[1], id)
	redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
	redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
end
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

func (r *RDB) removeAndArchiveAll(src, dst string) (int64, error) {
	now := time.Now()
	argv := []interface{}{
		now.Unix(),
		now.AddDate(0, 0, -archivedExpirationInDays).Unix(),
		maxArchiveSize,
	}
	res, err := removeAndArchiveAllCmd.Run(r.client,
		[]string{src, dst}, argv...).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("command error: unexpected return value %v", res)
	}
	return n, nil
}

// DeleteArchivedTask deletes an archived task that matches the given id and score from the given queue.
// If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) DeleteArchivedTask(qname string, id uuid.UUID) error {
	return r.deleteTask(base.ArchivedKey(qname), qname, id.String())
}

// DeleteRetryTask deletes a retry task that matches the given id and score from the given queue.
// If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) DeleteRetryTask(qname string, id uuid.UUID) error {
	return r.deleteTask(base.RetryKey(qname), qname, id.String())
}

// DeleteScheduledTask deletes a scheduled task that matches the given id and score from the given queue.
// If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) DeleteScheduledTask(qname string, id uuid.UUID) error {
	return r.deleteTask(base.ScheduledKey(qname), qname, id.String())
}

// KEYS[1] -> asynq:{<qname>}:pending
// KEYS[2] -> asynq:{<qname>}:t:<task_id>
// ARGV[1] -> task ID
var deletePendingTaskCmd = redis.NewScript(`
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
	return 0
end
return redis.call("DEL", KEYS[2])
`)

// DeletePendingTask deletes a pending tasks that matches the given id from the given queue.
// If there's no match, it returns ErrTaskNotFound.
func (r *RDB) DeletePendingTask(qname string, id uuid.UUID) error {
	keys := []string{base.PendingKey(qname), base.TaskKey(qname, id.String())}
	res, err := deletePendingTaskCmd.Run(r.client, keys, id.String()).Result()
	if err != nil {
		return err
	}
	n, ok := res.(int64)
	if !ok {
		return fmt.Errorf("command error: unexpected return value %v", res)
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// KEYS[1] -> ZSET key to remove the task from (e.g. asynq:{<qname>}:retry)
// KEYS[2] -> asynq:{<qname>}:t:<task_id>
// ARGV[1] -> task ID
var deleteTaskCmd = redis.NewScript(`
if redis.call("ZREM", KEYS[1], ARGV[1]) == 0 then
	return 0
end
return redis.call("DEL", KEYS[2])
`)

func (r *RDB) deleteTask(key, qname, id string) error {
	keys := []string{key, base.TaskKey(qname, id)}
	argv := []interface{}{id}
	res, err := deleteTaskCmd.Run(r.client, keys, argv...).Result()
	if err != nil {
		return err
	}
	n, ok := res.(int64)
	if !ok {
		return fmt.Errorf("command error: unexpected return value %v", res)
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// KEYS[1] -> queue to delete
// ARGV[1] -> task key prefix
var deleteAllCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	local key = ARGV[1] .. id
	redis.call("DEL", key)
end
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

// DeleteAllArchivedTasks deletes all archived tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllArchivedTasks(qname string) (int64, error) {
	return r.deleteAll(base.ArchivedKey(qname), qname)
}

// DeleteAllRetryTasks deletes all retry tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllRetryTasks(qname string) (int64, error) {
	return r.deleteAll(base.RetryKey(qname), qname)
}

// DeleteAllScheduledTasks deletes all scheduled tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllScheduledTasks(qname string) (int64, error) {
	return r.deleteAll(base.ScheduledKey(qname), qname)
}

func (r *RDB) deleteAll(key, qname string) (int64, error) {
	res, err := deleteAllCmd.Run(r.client, []string{key}, base.TaskKeyPrefix(qname)).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	return n, nil
}

// KEYS[1] -> asynq:{<qname>}:pending
// ARGV[1] -> task key prefix
var deleteAllPendingCmd = redis.NewScript(`
local ids = redis.call("LRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	local key = ARGV[1] .. id
	redis.call("DEL", key)
end
redis.call("DEL", KEYS[1])
return table.getn(ids)`)

// DeleteAllPendingTasks deletes all pending tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllPendingTasks(qname string) (int64, error) {
	res, err := deleteAllPendingCmd.Run(r.client,
		[]string{base.PendingKey(qname)}, base.TaskKeyPrefix(qname)).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("command error: unexpected return value %v", res)
	}
	return n, nil
}

// ErrQueueNotFound indicates specified queue does not exist.
type ErrQueueNotFound struct {
	qname string
}

func (e *ErrQueueNotFound) Error() string {
	return fmt.Sprintf("queue %q does not exist", e.qname)
}

// ErrQueueNotEmpty indicates specified queue is not empty.
type ErrQueueNotEmpty struct {
	qname string
}

func (e *ErrQueueNotEmpty) Error() string {
	return fmt.Sprintf("queue %q is not empty", e.qname)
}

// Only check whether active queue is empty before removing.
// KEYS[1] -> asynq:{<qname>}
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:scheduled
// KEYS[4] -> asynq:{<qname>}:retry
// KEYS[5] -> asynq:{<qname>}:archived
// KEYS[6] -> asynq:{<qname>}:deadlines
// ARGV[1] -> task key prefix
var removeQueueForceCmd = redis.NewScript(`
local active = redis.call("LLEN", KEYS[2])
if active > 0 then
    return redis.error_reply("Queue has tasks active")
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
return redis.status_reply("OK")`)

// Checks whether queue is empty before removing.
// KEYS[1] -> asynq:{<qname>}:pending
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:scheduled
// KEYS[4] -> asynq:{<qname>}:retry
// KEYS[5] -> asynq:{<qname>}:archived
// KEYS[6] -> asynq:{<qname>}:deadlines
// ARGV[1] -> task key prefix
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
	return redis.error_reply("QUEUE NOT EMPTY")
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
return redis.status_reply("OK")`)

// RemoveQueue removes the specified queue.
//
// If force is set to true, it will remove the queue regardless
// as long as no tasks are active for the queue.
// If force is set to false, it will only remove the queue if
// the queue is empty.
func (r *RDB) RemoveQueue(qname string, force bool) error {
	exists, err := r.client.SIsMember(base.AllQueues, qname).Result()
	if err != nil {
		return err
	}
	if !exists {
		return &ErrQueueNotFound{qname}
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
	if err := script.Run(r.client, keys, base.TaskKeyPrefix(qname)).Err(); err != nil {
		if err.Error() == "QUEUE NOT EMPTY" {
			return &ErrQueueNotEmpty{qname}
		}
		return err
	}
	return r.client.SRem(base.AllQueues, qname).Err()
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
