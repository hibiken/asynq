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
	Timestamp  time.Time
}

// EnqueuedTask is a task in a queue and is ready to be processed.
type EnqueuedTask struct {
	ID      xid.ID
	Type    string
	Payload map[string]interface{}
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
}

// DeadTask is a task in that has exhausted all retries.
type DeadTask struct {
	ID           xid.ID
	Type         string
	Payload      map[string]interface{}
	LastFailedAt time.Time
	ErrorMsg     string
	Score        int64
}

// CurrentStats returns a current state of the queues.
func (r *RDB) CurrentStats() (*Stats, error) {
	// KEYS[1] -> asynq:queues:default
	// KEYS[2] -> asynq:in_progress
	// KEYS[3] -> asynq:scheduled
	// KEYS[4] -> asynq:retry
	// KEYS[5] -> asynq:dead
	// KEYS[6] -> asynq:processed:<yyyy-mm-dd>
	// KEYS[7] -> asynq:failure:<yyyy-mm-dd>
	script := redis.NewScript(`
	local qlen = redis.call("LLEN", KEYS[1])
	local plen = redis.call("LLEN", KEYS[2])
	local slen = redis.call("ZCARD", KEYS[3])
	local rlen = redis.call("ZCARD", KEYS[4])
	local dlen = redis.call("ZCARD", KEYS[5])
	local pcount = 0
	local p = redis.call("GET", KEYS[6])
	if p then
		pcount = tonumber(p) 
	end
	local fcount = 0
	local f = redis.call("GET", KEYS[7])
	if f then
		fcount = tonumber(f)
	end
	return {qlen, plen, slen, rlen, dlen, pcount, fcount}
	`)

	now := time.Now()
	res, err := script.Run(r.client, []string{
		base.DefaultQueue,
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
	nums, err := cast.ToIntSliceE(res)
	if err != nil {
		return nil, err
	}
	return &Stats{
		Enqueued:   nums[0],
		InProgress: nums[1],
		Scheduled:  nums[2],
		Retry:      nums[3],
		Dead:       nums[4],
		Processed:  nums[5],
		Failed:     nums[6],
		Timestamp:  now,
	}, nil
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

// ListEnqueued returns all enqueued tasks that are ready to be processed.
func (r *RDB) ListEnqueued() ([]*EnqueuedTask, error) {
	data, err := r.client.LRange(base.DefaultQueue, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var tasks []*EnqueuedTask
	for _, s := range data {
		var msg base.TaskMessage
		err := json.Unmarshal([]byte(s), &msg)
		if err != nil {
			// continue // bad data, ignore and continue
			return nil, err
		}
		tasks = append(tasks, &EnqueuedTask{
			ID:      msg.ID,
			Type:    msg.Type,
			Payload: msg.Payload,
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
			redis.call("LPUSH", KEYS[2], msg)
			return 1
		end
	end
	return 0
	`)
	res, err := script.Run(r.client, []string{zset, base.DefaultQueue}, score, id).Result()
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
		redis.call("LPUSH", KEYS[2], msg)
	end
	return table.getn(msgs)
	`)
	res, err := script.Run(r.client, []string{zset, base.DefaultQueue}).Result()
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
