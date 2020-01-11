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
	"github.com/hibiken/asynq/internal/base"
	"github.com/rs/xid"
	"github.com/spf13/cast"
)

// Stats represents a state of queues at a certain time.
type Stats struct {
	Enqueued   int
	InProgress int
	Scheduled  int
	Retry      int
	Dead       int
	Processed  int
	Failed     int
	Queues     map[string]int // map of queue name to number of tasks in the queue (e.g., "default": 100, "critical": 20)
	Timestamp  time.Time
}

// DailyStats holds aggregate data for a given day.
type DailyStats struct {
	Processed int
	Failed    int
	Time      time.Time
}

// EnqueuedTask is a task in a queue and is ready to be processed.
type EnqueuedTask struct {
	ID      xid.ID
	Type    string
	Payload map[string]interface{}
	Queue   string
}

// InProgressTask is a task that's currently being processed.
type InProgressTask struct {
	ID      xid.ID
	Type    string
	Payload map[string]interface{}
}

// ScheduledTask is a task that's scheduled to be processed in the future.
type ScheduledTask struct {
	ID        xid.ID
	Type      string
	Payload   map[string]interface{}
	ProcessAt time.Time
	Score     int64
	Queue     string
}

// RetryTask is a task that's in retry queue because worker failed to process the task.
type RetryTask struct {
	ID      xid.ID
	Type    string
	Payload map[string]interface{}
	// TODO(hibiken): add LastFailedAt time.Time
	ProcessAt time.Time
	ErrorMsg  string
	Retried   int
	Retry     int
	Score     int64
	Queue     string
}

// DeadTask is a task in that has exhausted all retries.
type DeadTask struct {
	ID           xid.ID
	Type         string
	Payload      map[string]interface{}
	LastFailedAt time.Time
	ErrorMsg     string
	Score        int64
	Queue        string
}

// CurrentStats returns a current state of the queues.
func (r *RDB) CurrentStats() (*Stats, error) {
	// KEYS[1] -> asynq:queues
	// KEYS[2] -> asynq:in_progress
	// KEYS[3] -> asynq:scheduled
	// KEYS[4] -> asynq:retry
	// KEYS[5] -> asynq:dead
	// KEYS[6] -> asynq:processed:<yyyy-mm-dd>
	// KEYS[7] -> asynq:failure:<yyyy-mm-dd>
	script := redis.NewScript(`
	local res = {}
	local queues = redis.call("SMEMBERS", KEYS[1])
	for _, qkey in ipairs(queues) do
	  table.insert(res, qkey)
	  table.insert(res, redis.call("LLEN", qkey))
	end
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
	table.insert(res, "processed")
	table.insert(res, pcount)
	local fcount = 0
	local f = redis.call("GET", KEYS[7])
	if f then
		fcount = tonumber(f)
	end
	table.insert(res, "failed")
	table.insert(res, fcount)
	return res
	`)

	now := time.Now()
	res, err := script.Run(r.client, []string{
		base.AllQueues,
		base.InProgressQueue,
		base.ScheduledQueue,
		base.RetryQueue,
		base.DeadQueue,
		base.ProcessedKey(now),
		base.FailureKey(now),
	}).Result()
	if err != nil {
		return nil, err
	}
	data, err := cast.ToSliceE(res)
	if err != nil {
		return nil, err
	}
	stats := &Stats{
		Queues:    make(map[string]int),
		Timestamp: now,
	}
	for i := 0; i < len(data); i += 2 {
		key := cast.ToString(data[i])
		val := cast.ToInt(data[i+1])

		switch {
		case strings.HasPrefix(key, base.QueuePrefix):
			stats.Enqueued += val
			stats.Queues[strings.TrimPrefix(key, base.QueuePrefix)] = val
		case key == base.InProgressQueue:
			stats.InProgress = val
		case key == base.ScheduledQueue:
			stats.Scheduled = val
		case key == base.RetryQueue:
			stats.Retry = val
		case key == base.DeadQueue:
			stats.Dead = val
		case key == "processed":
			stats.Processed = val
		case key == "failed":
			stats.Failed = val
		}
	}
	return stats, nil
}

// HistoricalStats returns a list of stats from the last n days.
func (r *RDB) HistoricalStats(n int) ([]*DailyStats, error) {
	if n < 1 {
		return []*DailyStats{}, nil
	}
	const day = 24 * time.Hour
	now := time.Now().UTC()
	var days []time.Time
	var keys []string
	for i := 0; i < n; i++ {
		ts := now.Add(-time.Duration(i) * day)
		days = append(days, ts)
		keys = append(keys, base.ProcessedKey(ts))
		keys = append(keys, base.FailureKey(ts))
	}
	script := redis.NewScript(`
	local res = {}
	for _, key in ipairs(KEYS) do
	  local n = redis.call("GET", key)
	  if not n then
		n = 0
	  end
	  table.insert(res, tonumber(n))
	end
	return res
	`)
	res, err := script.Run(r.client, keys, len(keys)).Result()
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
	info := make(map[string]string)
	lines := strings.Split(res, "\r\n")
	for _, l := range lines {
		kv := strings.Split(l, ":")
		if len(kv) == 2 {
			info[kv[0]] = kv[1]
		}
	}
	return info, nil
}

// ListEnqueued returns enqueued tasks that are ready to be processed.
//
// Queue names can be optionally passed to query only the specified queues.
// If none are passed, it will query all queues.
func (r *RDB) ListEnqueued(qnames ...string) ([]*EnqueuedTask, error) {
	if len(qnames) == 0 {
		return r.listAllEnqueued()
	}
	return r.listEnqueued(qnames...)
}

func (r *RDB) listAllEnqueued() ([]*EnqueuedTask, error) {
	script := redis.NewScript(`
	local res = {}
	local queues = redis.call("SMEMBERS", KEYS[1])
	for _, qkey in ipairs(queues) do
		local msgs = redis.call("LRANGE", qkey, 0, -1)
		for _, msg in ipairs(msgs) do
			table.insert(res, msg)
		end
	end
	return res
	`)
	res, err := script.Run(r.client, []string{base.AllQueues}).Result()
	if err != nil {
		return nil, err
	}
	data, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, err
	}
	return toEnqueuedTasks(data)
}

func (r *RDB) listEnqueued(qnames ...string) ([]*EnqueuedTask, error) {
	script := redis.NewScript(`
	local res = {}
	for _, qkey in ipairs(KEYS) do
		local msgs = redis.call("LRANGE", qkey, 0, -1)
		for _, msg in ipairs(msgs) do
			table.insert(res, msg)
		end
	end
	return res
	`)
	var keys []string
	for _, q := range qnames {
		keys = append(keys, base.QueueKey(q))
	}
	res, err := script.Run(r.client, keys).Result()
	if err != nil {
		return nil, err
	}
	data, err := cast.ToStringSliceE(res)
	if err != nil {
		return nil, err
	}
	return toEnqueuedTasks(data)
}

func toEnqueuedTasks(data []string) ([]*EnqueuedTask, error) {
	var tasks []*EnqueuedTask
	for _, s := range data {
		var msg base.TaskMessage
		err := json.Unmarshal([]byte(s), &msg)
		if err != nil {
			continue // bad data, ignore and continue
		}
		tasks = append(tasks, &EnqueuedTask{
			ID:      msg.ID,
			Type:    msg.Type,
			Payload: msg.Payload,
			Queue:   msg.Queue,
		})
	}
	return tasks, nil
}

// ListInProgress returns all tasks that are currently being processed.
func (r *RDB) ListInProgress() ([]*InProgressTask, error) {
	data, err := r.client.LRange(base.InProgressQueue, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var tasks []*InProgressTask
	for _, s := range data {
		var msg base.TaskMessage
		err := json.Unmarshal([]byte(s), &msg)
		if err != nil {
			continue // bad data, ignore and continue
		}
		tasks = append(tasks, &InProgressTask{
			ID:      msg.ID,
			Type:    msg.Type,
			Payload: msg.Payload,
		})
	}
	return tasks, nil
}

// ListScheduled returns all tasks that are scheduled to be processed
// in the future.
func (r *RDB) ListScheduled() ([]*ScheduledTask, error) {
	data, err := r.client.ZRangeWithScores(base.ScheduledQueue, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var tasks []*ScheduledTask
	for _, z := range data {
		s, ok := z.Member.(string)
		if !ok {
			continue // bad data, ignore and continue
		}
		var msg base.TaskMessage
		err := json.Unmarshal([]byte(s), &msg)
		if err != nil {
			continue // bad data, ignore and continue
		}
		processAt := time.Unix(int64(z.Score), 0)
		tasks = append(tasks, &ScheduledTask{
			ID:        msg.ID,
			Type:      msg.Type,
			Payload:   msg.Payload,
			Queue:     msg.Queue,
			ProcessAt: processAt,
			Score:     int64(z.Score),
		})
	}
	return tasks, nil
}

// ListRetry returns all tasks that have failed before and willl be retried
// in the future.
func (r *RDB) ListRetry() ([]*RetryTask, error) {
	data, err := r.client.ZRangeWithScores(base.RetryQueue, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var tasks []*RetryTask
	for _, z := range data {
		s, ok := z.Member.(string)
		if !ok {
			continue // bad data, ignore and continue
		}
		var msg base.TaskMessage
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
			Queue:     msg.Queue,
			ProcessAt: processAt,
			Score:     int64(z.Score),
		})
	}
	return tasks, nil
}

// ListDead returns all tasks that have exhausted its retry limit.
func (r *RDB) ListDead() ([]*DeadTask, error) {
	data, err := r.client.ZRangeWithScores(base.DeadQueue, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var tasks []*DeadTask
	for _, z := range data {
		s, ok := z.Member.(string)
		if !ok {
			continue // bad data, ignore and continue
		}
		var msg base.TaskMessage
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
			Queue:        msg.Queue,
			LastFailedAt: lastFailedAt,
			Score:        int64(z.Score),
		})
	}
	return tasks, nil
}

// EnqueueDeadTask finds a task that matches the given id and score from dead queue
// and enqueues it for processing. If a task that matches the id and score
// does not exist, it returns ErrTaskNotFound.
func (r *RDB) EnqueueDeadTask(id xid.ID, score int64) error {
	n, err := r.removeAndEnqueue(base.DeadQueue, id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// EnqueueRetryTask finds a task that matches the given id and score from retry queue
// and enqueues it for processing. If a task that matches the id and score
// does not exist, it returns ErrTaskNotFound.
func (r *RDB) EnqueueRetryTask(id xid.ID, score int64) error {
	n, err := r.removeAndEnqueue(base.RetryQueue, id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// EnqueueScheduledTask finds a task that matches the given id and score from scheduled queue
// and enqueues it for processing. If a task that matches the id and score does not
// exist, it returns ErrTaskNotFound.
func (r *RDB) EnqueueScheduledTask(id xid.ID, score int64) error {
	n, err := r.removeAndEnqueue(base.ScheduledQueue, id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// EnqueueAllScheduledTasks enqueues all tasks from scheduled queue
// and returns the number of tasks enqueued.
func (r *RDB) EnqueueAllScheduledTasks() (int64, error) {
	return r.removeAndEnqueueAll(base.ScheduledQueue)
}

// EnqueueAllRetryTasks enqueues all tasks from retry queue
// and returns the number of tasks enqueued.
func (r *RDB) EnqueueAllRetryTasks() (int64, error) {
	return r.removeAndEnqueueAll(base.RetryQueue)
}

// EnqueueAllDeadTasks enqueues all tasks from dead queue
// and returns the number of tasks enqueued.
func (r *RDB) EnqueueAllDeadTasks() (int64, error) {
	return r.removeAndEnqueueAll(base.DeadQueue)
}

func (r *RDB) removeAndEnqueue(zset, id string, score float64) (int64, error) {
	script := redis.NewScript(`
	local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])
	for _, msg in ipairs(msgs) do
		local decoded = cjson.decode(msg)
		if decoded["ID"] == ARGV[2] then
			redis.call("ZREM", KEYS[1], msg)
			local qkey = ARGV[3] .. decoded["Queue"]
			redis.call("LPUSH", qkey, msg)
			return 1
		end
	end
	return 0
	`)
	res, err := script.Run(r.client, []string{zset}, score, id, base.QueuePrefix).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	return n, nil
}

func (r *RDB) removeAndEnqueueAll(zset string) (int64, error) {
	script := redis.NewScript(`
	local msgs = redis.call("ZRANGE", KEYS[1], 0, -1)
	for _, msg in ipairs(msgs) do
		redis.call("ZREM", KEYS[1], msg)
		local decoded = cjson.decode(msg)
		local qkey = ARGV[1] .. decoded["Queue"]
		redis.call("LPUSH", qkey, msg)
	end
	return table.getn(msgs)
	`)
	res, err := script.Run(r.client, []string{zset}, base.QueuePrefix).Result()
	if err != nil {
		return 0, err
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("could not cast %v to int64", res)
	}
	return n, nil
}

// KillRetryTask finds a task that matches the given id and score from retry queue
// and moves it to dead queue. If a task that maches the id and score does not exist,
// it returns ErrTaskNotFound.
func (r *RDB) KillRetryTask(id xid.ID, score int64) error {
	n, err := r.removeAndKill(base.RetryQueue, id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// KillScheduledTask finds a task that matches the given id and score from scheduled queue
// and moves it to dead queue. If a task that maches the id and score does not exist,
// it returns ErrTaskNotFound.
func (r *RDB) KillScheduledTask(id xid.ID, score int64) error {
	n, err := r.removeAndKill(base.ScheduledQueue, id.String(), float64(score))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// KillAllRetryTasks moves all tasks from retry queue to dead queue and
// returns the number of tasks that were moved.
func (r *RDB) KillAllRetryTasks() (int64, error) {
	return r.removeAndKillAll(base.RetryQueue)
}

// KillAllScheduledTasks moves all tasks from scheduled queue to dead queue and
// returns the number of tasks that were moved.
func (r *RDB) KillAllScheduledTasks() (int64, error) {
	return r.removeAndKillAll(base.ScheduledQueue)
}

func (r *RDB) removeAndKill(zset, id string, score float64) (int64, error) {
	// KEYS[1] -> ZSET to move task from (e.g., retry queue)
	// KEYS[2] -> asynq:dead
	// ARGV[1] -> score of the task to kill
	// ARGV[2] -> id of the task to kill
	// ARGV[3] -> current timestamp
	// ARGV[4] -> cutoff timestamp (e.g., 90 days ago)
	// ARGV[5] -> max number of tasks in dead queue (e.g., 100)
	script := redis.NewScript(`
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
	return 0
	`)
	now := time.Now()
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	res, err := script.Run(r.client,
		[]string{zset, base.DeadQueue},
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

func (r *RDB) removeAndKillAll(zset string) (int64, error) {
	// KEYS[1] -> ZSET to move task from (e.g., retry queue)
	// KEYS[2] -> asynq:dead
	// ARGV[1] -> current timestamp
	// ARGV[2] -> cutoff timestamp (e.g., 90 days ago)
	// ARGV[3] -> max number of tasks in dead queue (e.g., 100)
	script := redis.NewScript(`
	local msgs = redis.call("ZRANGE", KEYS[1], 0, -1)
	for _, msg in ipairs(msgs) do
		redis.call("ZREM", KEYS[1], msg)
		redis.call("ZADD", KEYS[2], ARGV[1], msg)
		redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
		redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
	end
	return table.getn(msgs)
	`)
	now := time.Now()
	limit := now.AddDate(0, 0, -deadExpirationInDays).Unix() // 90 days ago
	res, err := script.Run(r.client, []string{zset, base.DeadQueue},
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

// DeleteDeadTask finds a task that matches the given id and score from dead queue
// and deletes it. If a task that matches the id and score does not exist,
// it returns ErrTaskNotFound.
func (r *RDB) DeleteDeadTask(id xid.ID, score int64) error {
	return r.deleteTask(base.DeadQueue, id.String(), float64(score))
}

// DeleteRetryTask finds a task that matches the given id and score from retry queue
// and deletes it. If a task that matches the id and score does not exist,
// it returns ErrTaskNotFound.
func (r *RDB) DeleteRetryTask(id xid.ID, score int64) error {
	return r.deleteTask(base.RetryQueue, id.String(), float64(score))
}

// DeleteScheduledTask finds a task that matches the given id and score from
// scheduled queue  and deletes it. If a task that matches the id and score
//does not exist, it returns ErrTaskNotFound.
func (r *RDB) DeleteScheduledTask(id xid.ID, score int64) error {
	return r.deleteTask(base.ScheduledQueue, id.String(), float64(score))
}

func (r *RDB) deleteTask(zset, id string, score float64) error {
	script := redis.NewScript(`
	local msgs = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])
	for _, msg in ipairs(msgs) do
		local decoded = cjson.decode(msg)
		if decoded["ID"] == ARGV[2] then
			redis.call("ZREM", KEYS[1], msg)
			return 1
		end
	end
	return 0
	`)
	res, err := script.Run(r.client, []string{zset}, score, id).Result()
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

// DeleteAllDeadTasks deletes all tasks from the dead queue.
func (r *RDB) DeleteAllDeadTasks() error {
	return r.client.Del(base.DeadQueue).Err()
}

// DeleteAllRetryTasks deletes all tasks from the dead queue.
func (r *RDB) DeleteAllRetryTasks() error {
	return r.client.Del(base.RetryQueue).Err()
}

// DeleteAllScheduledTasks deletes all tasks from the dead queue.
func (r *RDB) DeleteAllScheduledTasks() error {
	return r.client.Del(base.ScheduledQueue).Err()
}
