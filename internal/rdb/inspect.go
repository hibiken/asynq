// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"encoding/json"
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
	Dead      int
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
// KEYS[5] -> asynq:<qname>:dead
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
		base.QueueKey(qname),
		base.ActiveKey(qname),
		base.ScheduledKey(qname),
		base.RetryKey(qname),
		base.DeadKey(qname),
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
		case base.QueueKey(qname):
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
		case base.DeadKey(qname):
			stats.Dead = val
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
	return stats, nil
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
	return r.listMessages(base.QueueKey(qname), pgn)
}

// ListActive returns all tasks that are currently being processed for the given queue.
func (r *RDB) ListActive(qname string, pgn Pagination) ([]*base.TaskMessage, error) {
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, fmt.Errorf("queue %q does not exist", qname)
	}
	return r.listMessages(base.ActiveKey(qname), pgn)
}

// listMessages returns a list of TaskMessage in Redis list with the given key.
func (r *RDB) listMessages(key string, pgn Pagination) ([]*base.TaskMessage, error) {
	// Note: Because we use LPUSH to redis list, we need to calculate the
	// correct range and reverse the list to get the tasks with pagination.
	stop := -pgn.start() - 1
	start := -pgn.stop() - 1
	data, err := r.client.LRange(key, start, stop).Result()
	if err != nil {
		return nil, err
	}
	reverse(data)
	var msgs []*base.TaskMessage
	for _, s := range data {
		m, err := base.DecodeMessage(s)
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
	return r.listZSetEntries(base.ScheduledKey(qname), pgn)
}

// ListRetry returns all tasks from the given queue that have failed before
// and willl be retried in the future.
func (r *RDB) ListRetry(qname string, pgn Pagination) ([]base.Z, error) {
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, fmt.Errorf("queue %q does not exist", qname)
	}
	return r.listZSetEntries(base.RetryKey(qname), pgn)
}

// ListDead returns all tasks from the given queue that have exhausted its retry limit.
func (r *RDB) ListDead(qname string, pgn Pagination) ([]base.Z, error) {
	if !r.client.SIsMember(base.AllQueues, qname).Val() {
		return nil, fmt.Errorf("queue %q does not exist", qname)
	}
	return r.listZSetEntries(base.DeadKey(qname), pgn)
}

// listZSetEntries returns a list of message and score pairs in Redis sorted-set
// with the given key.
func (r *RDB) listZSetEntries(key string, pgn Pagination) ([]base.Z, error) {
	data, err := r.client.ZRangeWithScores(key, pgn.start(), pgn.stop()).Result()
	if err != nil {
		return nil, err
	}
	var res []base.Z
	for _, z := range data {
		s, ok := z.Member.(string)
		if !ok {
			continue // bad data, ignore and continue
		}
		msg, err := base.DecodeMessage(s)
		if err != nil {
			continue // bad data, ignore and continue
		}
		res = append(res, base.Z{Message: msg, Score: int64(z.Score)})
	}
	return res, nil
}

// RunDeadTask finds a dead task that matches the given id and score from
// the given queue and enqueues it for processing.
//If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) RunDeadTask(qname string, id uuid.UUID, score int64) error {
	n, err := r.removeAndRun(base.DeadKey(qname), base.QueueKey(qname), id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// RunRetryTask finds a retry task that matches the given id and score from
// the given queue and enqueues it for processing.
// If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) RunRetryTask(qname string, id uuid.UUID, score int64) error {
	n, err := r.removeAndRun(base.RetryKey(qname), base.QueueKey(qname), id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// RunScheduledTask finds a scheduled task that matches the given id and score from
// from the given queue and enqueues it for processing.
// If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) RunScheduledTask(qname string, id uuid.UUID, score int64) error {
	n, err := r.removeAndRun(base.ScheduledKey(qname), base.QueueKey(qname), id.String(), float64(score))
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
	return r.removeAndRunAll(base.ScheduledKey(qname), base.QueueKey(qname))
}

// RunAllRetryTasks enqueues all retry tasks from the given queue
// and returns the number of tasks enqueued.
func (r *RDB) RunAllRetryTasks(qname string) (int64, error) {
	return r.removeAndRunAll(base.RetryKey(qname), base.QueueKey(qname))
}

// RunAllDeadTasks enqueues all tasks from dead queue
// and returns the number of tasks enqueued.
func (r *RDB) RunAllDeadTasks(qname string) (int64, error) {
	return r.removeAndRunAll(base.DeadKey(qname), base.QueueKey(qname))
}

var removeAndRunCmd = redis.NewScript(`
local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])
for _, msg in ipairs(msgs) do
	local decoded = cjson.decode(msg)
	if decoded["ID"] == ARGV[2] then
		redis.call("LPUSH", KEYS[2], msg)
		redis.call("ZREM", KEYS[1], msg)
		return 1
	end
end
return 0`)

func (r *RDB) removeAndRun(zset, qkey, id string, score float64) (int64, error) {
	res, err := removeAndRunCmd.Run(r.client, []string{zset, qkey}, score, id).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	return n, nil
}

var removeAndRunAllCmd = redis.NewScript(`
local msgs = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, msg in ipairs(msgs) do
	redis.call("LPUSH", KEYS[2], msg)
	redis.call("ZREM", KEYS[1], msg)
end
return table.getn(msgs)`)

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

// KillRetryTask finds a retry task that matches the given id and score from the given queue
// and kills it. If a task that maches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) KillRetryTask(qname string, id uuid.UUID, score int64) error {
	n, err := r.removeAndKill(base.RetryKey(qname), base.DeadKey(qname), id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// KillScheduledTask finds a scheduled task that matches the given id and score from the given queue
// and kills it. If a task that maches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) KillScheduledTask(qname string, id uuid.UUID, score int64) error {
	n, err := r.removeAndKill(base.ScheduledKey(qname), base.DeadKey(qname), id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// KillAllRetryTasks kills all retry tasks from the given queue and
// returns the number of tasks that were moved.
func (r *RDB) KillAllRetryTasks(qname string) (int64, error) {
	return r.removeAndKillAll(base.RetryKey(qname), base.DeadKey(qname))
}

// KillAllScheduledTasks kills all scheduled tasks from the given queue and
// returns the number of tasks that were moved.
func (r *RDB) KillAllScheduledTasks(qname string) (int64, error) {
	return r.removeAndKillAll(base.ScheduledKey(qname), base.DeadKey(qname))
}

// KEYS[1] -> ZSET to move task from (e.g., retry queue)
// KEYS[2] -> asynq:{<qname>}:dead
// ARGV[1] -> score of the task to kill
// ARGV[2] -> id of the task to kill
// ARGV[3] -> current timestamp
// ARGV[4] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[5] -> max number of tasks in dead queue (e.g., 100)
var removeAndKillCmd = redis.NewScript(`
local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])
for _, msg in ipairs(msgs) do
	local decoded = cjson.decode(msg)
	if decoded["ID"] == ARGV[2] then
		redis.call("ZREM", KEYS[1], msg)
		redis.call("ZADD", KEYS[2], ARGV[3], msg)
		redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[4])
		redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[5])
		return 1
	end
end
return 0`)

func (r *RDB) removeAndKill(src, dst, id string, score float64) (int64, error) {
	now := time.Now()
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	res, err := removeAndKillCmd.Run(r.client,
		[]string{src, dst},
		score, id, now.Unix(), limit, maxDeadTasks).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	return n, nil
}

// KEYS[1] -> ZSET to move task from (e.g., retry queue)
// KEYS[2] -> asynq:{<qname>}:dead
// ARGV[1] -> current timestamp
// ARGV[2] -> cutoff timestamp (e.g., 90 days ago)
// ARGV[3] -> max number of tasks in dead queue (e.g., 100)
var removeAndKillAllCmd = redis.NewScript(`
local msgs = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, msg in ipairs(msgs) do
	redis.call("ZADD", KEYS[2], ARGV[1], msg)
	redis.call("ZREM", KEYS[1], msg)
	redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
	redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
end
return table.getn(msgs)`)

func (r *RDB) removeAndKillAll(src, dst string) (int64, error) {
	now := time.Now()
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	res, err := removeAndKillAllCmd.Run(r.client, []string{src, dst},
		now.Unix(), limit, maxDeadTasks).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	return n, nil
}

// DeleteDeadTask deletes a dead task that matches the given id and score from the given queue.
// If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) DeleteDeadTask(qname string, id uuid.UUID, score int64) error {
	return r.deleteTask(base.DeadKey(qname), id.String(), float64(score))
}

// DeleteRetryTask deletes a retry task that matches the given id and score from the given queue.
// If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) DeleteRetryTask(qname string, id uuid.UUID, score int64) error {
	return r.deleteTask(base.RetryKey(qname), id.String(), float64(score))
}

// DeleteScheduledTask deletes a scheduled task that matches the given id and score from the given queue.
// If a task that matches the id and score does not exist, it returns ErrTaskNotFound.
func (r *RDB) DeleteScheduledTask(qname string, id uuid.UUID, score int64) error {
	return r.deleteTask(base.ScheduledKey(qname), id.String(), float64(score))
}

var deleteTaskCmd = redis.NewScript(`
local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])
for _, msg in ipairs(msgs) do
	local decoded = cjson.decode(msg)
	if decoded["ID"] == ARGV[2] then
		redis.call("ZREM", KEYS[1], msg)
		return 1
	end
end
return 0`)

func (r *RDB) deleteTask(key, id string, score float64) error {
	res, err := deleteTaskCmd.Run(r.client, []string{key}, score, id).Result()
	if err != nil {
		return err
	}
	n, ok := res.(int64)
	if !ok {
		return fmt.Errorf("could not cast %v to int64", res)
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// KEYS[1] -> queue to delete
var deleteAllCmd = redis.NewScript(`
local n = redis.call("ZCARD", KEYS[1])
redis.call("DEL", KEYS[1])
return n`)

// DeleteAllDeadTasks deletes all dead tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllDeadTasks(qname string) (int64, error) {
	return r.deleteAll(base.DeadKey(qname))
}

// DeleteAllRetryTasks deletes all retry tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllRetryTasks(qname string) (int64, error) {
	return r.deleteAll(base.RetryKey(qname))
}

// DeleteAllScheduledTasks deletes all scheduled tasks from the given queue
// and returns the number of tasks deleted.
func (r *RDB) DeleteAllScheduledTasks(qname string) (int64, error) {
	return r.deleteAll(base.ScheduledKey(qname))
}

func (r *RDB) deleteAll(key string) (int64, error) {
	res, err := deleteAllCmd.Run(r.client, []string{key}).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
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
// KEYS[5] -> asynq:{<qname>}:dead
// KEYS[6] -> asynq:{<qname>}:deadlines
var removeQueueForceCmd = redis.NewScript(`
local active = redis.call("LLEN", KEYS[2])
if active > 0 then
    return redis.error_reply("Queue has tasks active")
end
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
redis.call("DEL", KEYS[5])
redis.call("DEL", KEYS[6])
return redis.status_reply("OK")`)

// Checks whether queue is empty before removing.
// KEYS[1] -> asynq:{<qname>}
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:scheduled
// KEYS[4] -> asynq:{<qname>}:retry
// KEYS[5] -> asynq:{<qname>}:dead
// KEYS[6] -> asynq:{<qname>}:deadlines
var removeQueueCmd = redis.NewScript(`
local pending = redis.call("LLEN", KEYS[1]) 
local active = redis.call("LLEN", KEYS[2])
local scheduled = redis.call("SCARD", KEYS[3])
local retry = redis.call("SCARD", KEYS[4])
local dead = redis.call("SCARD", KEYS[5])
local total = pending + active + scheduled + retry + dead
if total > 0 then
	return redis.error_reply("QUEUE NOT EMPTY")
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
		base.QueueKey(qname),
		base.ActiveKey(qname),
		base.ScheduledKey(qname),
		base.RetryKey(qname),
		base.DeadKey(qname),
		base.DeadlinesKey(qname),
	}
	if err := script.Run(r.client, keys).Err(); err != nil {
		if err.Error() == "QUEUE NOT EMPTY" {
			return &ErrQueueNotEmpty{qname}
		} else {
			return err
		}
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
		var info base.ServerInfo
		if err := json.Unmarshal([]byte(data), &info); err != nil {
			continue // skip bad data
		}
		servers = append(servers, &info)
	}
	return servers, nil
}

// Note: Script also removes stale keys.
var listWorkerKeysCmd = redis.NewScript(`
local now = tonumber(ARGV[1])
local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
return keys`)

// ListWorkers returns the list of worker stats.
func (r *RDB) ListWorkers() ([]*base.WorkerInfo, error) {
	now := time.Now()
	res, err := listWorkerKeysCmd.Run(r.client, []string{base.AllWorkers}, now.Unix()).Result()
	if err != nil {
		return nil, err
	}
	keys, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, err
	}
	var workers []*base.WorkerInfo
	for _, key := range keys {
		data, err := r.client.HVals(key).Result()
		if err != nil {
			continue // skip bad data
		}
		for _, s := range data {
			var w base.WorkerInfo
			if err := json.Unmarshal([]byte(s), &w); err != nil {
				continue // skip bad data
			}
			workers = append(workers, &w)

		}
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
			var e base.SchedulerEntry
			if err := json.Unmarshal([]byte(s), &e); err != nil {
				continue // skip bad data
			}
			entries = append(entries, &e)
		}
	}
	return entries, nil
}

// ListSchedulerEnqueueEvents returns the list of scheduler enqueue events.
func (r *RDB) ListSchedulerEnqueueEvents(entryID string) ([]*base.SchedulerEnqueueEvent, error) {
	key := base.SchedulerHistoryKey(entryID)
	zs, err := r.client.ZRangeWithScores(key, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var events []*base.SchedulerEnqueueEvent
	for _, z := range zs {
		data, err := cast.ToStringE(z.Member)
		if err != nil {
			return nil, err
		}
		var e base.SchedulerEnqueueEvent
		if err := json.Unmarshal([]byte(data), &e); err != nil {
			return nil, err
		}
		events = append(events, &e)
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
	key := base.QueueKey(qname)
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
