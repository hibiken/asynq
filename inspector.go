// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/rdb"
)

// Inspector is a client interface to inspect and mutate the state of
// queues and tasks.
type Inspector struct {
	rdb *rdb.RDB
}

// New returns a new instance of Inspector.
func NewInspector(r RedisConnOpt) *Inspector {
	return &Inspector{
		rdb: rdb.NewRDB(createRedisClient(r)),
	}
}

// Stats represents a state of queues at a certain time.
type Stats struct {
	Enqueued   int
	InProgress int
	Scheduled  int
	Retry      int
	Dead       int
	Processed  int
	Failed     int
	Queues     []*QueueInfo
	Timestamp  time.Time
}

// QueueInfo holds information about a queue.
type QueueInfo struct {
	// Name of the queue (e.g. "default", "critical").
	// Note: It doesn't include the prefix "asynq:queues:".
	Name string

	// Paused indicates whether the queue is paused.
	// If true, tasks in the queue should not be processed.
	Paused bool

	// Size is the number of tasks in the queue.
	Size int
}

// CurrentStats returns a current stats of the queues.
func (i *Inspector) CurrentStats() (*Stats, error) {
	stats, err := i.rdb.CurrentStats()
	if err != nil {
		return nil, err
	}
	var qs []*QueueInfo
	for _, q := range stats.Queues {
		qs = append(qs, (*QueueInfo)(q))
	}
	return &Stats{
		Enqueued:   stats.Enqueued,
		InProgress: stats.InProgress,
		Scheduled:  stats.Scheduled,
		Retry:      stats.Retry,
		Dead:       stats.Dead,
		Processed:  stats.Processed,
		Failed:     stats.Failed,
		Queues:     qs,
		Timestamp:  stats.Timestamp,
	}, nil
}

// DailyStats holds aggregate data for a given day.
type DailyStats struct {
	Processed int
	Failed    int
	Date      time.Time
}

// History returns a list of stats from the last n days.
func (i *Inspector) History(n int) ([]*DailyStats, error) {
	stats, err := i.rdb.HistoricalStats(n)
	if err != nil {
		return nil, err
	}
	var res []*DailyStats
	for _, s := range stats {
		res = append(res, &DailyStats{
			Processed: s.Processed,
			Failed:    s.Failed,
			Date:      s.Time,
		})
	}
	return res, nil
}

// EnqueuedTask is a task in a queue and is ready to be processed.
type EnqueuedTask struct {
	*Task
	ID    string
	Queue string
}

// InProgressTask is a task that's currently being processed.
type InProgressTask struct {
	*Task
	ID string
}

// ScheduledTask is a task scheduled to be processed in the future.
type ScheduledTask struct {
	*Task
	ID            string
	Queue         string
	NextEnqueueAt time.Time

	score int64
}

// RetryTask is a task scheduled to be retried in the future.
type RetryTask struct {
	*Task
	ID            string
	Queue         string
	NextEnqueueAt time.Time
	MaxRetry      int
	Retried       int
	ErrorMsg      string
	// TODO: LastFailedAt  time.Time

	score int64
}

// DeadTask is a task exhausted its retries.
// DeadTask won't be retried automatically.
type DeadTask struct {
	*Task
	ID           string
	Queue        string
	MaxRetry     int
	Retried      int
	LastFailedAt time.Time
	ErrorMsg     string

	score int64
}

// Key returns a key used to delete, enqueue, and kill the task.
func (t *ScheduledTask) Key() string {
	return fmt.Sprintf("s:%v:%v", t.ID, t.score)
}

// Key returns a key used to delete, enqueue, and kill the task.
func (t *RetryTask) Key() string {
	return fmt.Sprintf("r:%v:%v", t.ID, t.score)
}

// Key returns a key used to delete, enqueue, and kill the task.
func (t *DeadTask) Key() string {
	return fmt.Sprintf("d:%v:%v", t.ID, t.score)
}

// parseTaskKey parses a key string and returns each part of key with proper
// type if valid, otherwise it reports an error.
func parseTaskKey(key string) (id uuid.UUID, score int64, qtype string, err error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 {
		return uuid.Nil, 0, "", fmt.Errorf("invalid id")
	}
	id, err = uuid.Parse(parts[1])
	if err != nil {
		return uuid.Nil, 0, "", fmt.Errorf("invalid id")
	}
	score, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return uuid.Nil, 0, "", fmt.Errorf("invalid id")
	}
	qtype = parts[0]
	if len(qtype) != 1 || !strings.Contains("srd", qtype) {
		return uuid.Nil, 0, "", fmt.Errorf("invalid id")
	}
	return id, score, qtype, nil
}

// ListOption specifies behavior of list operation.
type ListOption interface{}

// Internal list option representations.
type (
	pageSizeOpt int
	pageNumOpt  int
)

type listOption struct {
	pageSize int
	pageNum  int
}

const (
	// Page size used by default in list operation.
	defaultPageSize = 30

	// Page number used by default in list operation.
	defaultPageNum = 1
)

func composeListOptions(opts ...ListOption) listOption {
	res := listOption{
		pageSize: defaultPageSize,
		pageNum:  defaultPageNum,
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case pageSizeOpt:
			res.pageSize = int(opt)
		case pageNumOpt:
			res.pageNum = int(opt)
		default:
			// ignore unexpected option
		}
	}
	return res
}

// PageSize returns an option to specify the page size for list operation.
//
// Negative page size is treated as zero.
func PageSize(n int) ListOption {
	if n < 0 {
		n = 0
	}
	return pageSizeOpt(n)
}

// Page returns an option to specify the page number for list operation.
// The value 1 fetches the first page.
//
// Negative page number is treated as one.
func Page(n int) ListOption {
	if n < 0 {
		n = 1
	}
	return pageNumOpt(n)
}

// ListScheduledTasks retrieves tasks in the specified queue.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListEnqueuedTasks(qname string, opts ...ListOption) ([]*EnqueuedTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	msgs, err := i.rdb.ListEnqueued(qname, pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*EnqueuedTask
	for _, m := range msgs {
		tasks = append(tasks, &EnqueuedTask{
			Task:  NewTask(m.Type, m.Payload),
			ID:    m.ID.String(),
			Queue: m.Queue,
		})
	}
	return tasks, err
}

// ListScheduledTasks retrieves tasks currently being processed.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListInProgressTasks(opts ...ListOption) ([]*InProgressTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	msgs, err := i.rdb.ListInProgress(pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*InProgressTask
	for _, m := range msgs {
		tasks = append(tasks, &InProgressTask{
			Task: NewTask(m.Type, m.Payload),
			ID:   m.ID.String(),
		})
	}
	return tasks, err
}

// ListScheduledTasks retrieves tasks in scheduled state.
// Tasks are sorted by NextEnqueueAt field in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListScheduledTasks(opts ...ListOption) ([]*ScheduledTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListScheduled(pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*ScheduledTask
	for _, z := range zs {
		enqueueAt := time.Unix(z.Score, 0)
		t := NewTask(z.Message.Type, z.Message.Payload)
		tasks = append(tasks, &ScheduledTask{
			Task:          t,
			ID:            z.Message.ID.String(),
			Queue:         z.Message.Queue,
			NextEnqueueAt: enqueueAt,
			score:         z.Score,
		})
	}
	return tasks, nil
}

// ListScheduledTasks retrieves tasks in retry state.
// Tasks are sorted by NextEnqueueAt field in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListRetryTasks(opts ...ListOption) ([]*RetryTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListRetry(pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*RetryTask
	for _, z := range zs {
		enqueueAt := time.Unix(z.Score, 0)
		t := NewTask(z.Message.Type, z.Message.Payload)
		tasks = append(tasks, &RetryTask{
			Task:          t,
			ID:            z.Message.ID.String(),
			Queue:         z.Message.Queue,
			NextEnqueueAt: enqueueAt,
			MaxRetry:      z.Message.Retry,
			Retried:       z.Message.Retried,
			// TODO: LastFailedAt: z.Message.LastFailedAt
			ErrorMsg: z.Message.ErrorMsg,
			score:    z.Score,
		})
	}
	return tasks, nil
}

// ListScheduledTasks retrieves tasks in retry state.
// Tasks are sorted by LastFailedAt field in descending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListDeadTasks(opts ...ListOption) ([]*DeadTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListDead(pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*DeadTask
	for _, z := range zs {
		failedAt := time.Unix(z.Score, 0)
		t := NewTask(z.Message.Type, z.Message.Payload)
		tasks = append(tasks, &DeadTask{
			Task:         t,
			ID:           z.Message.ID.String(),
			Queue:        z.Message.Queue,
			MaxRetry:     z.Message.Retry,
			Retried:      z.Message.Retried,
			LastFailedAt: failedAt,
			ErrorMsg:     z.Message.ErrorMsg,
			score:        z.Score,
		})
	}
	return tasks, nil
	return nil, nil
}

// DeleteAllScheduledTasks deletes all tasks in scheduled state,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllScheduledTasks() (int, error) {
	n, err := i.rdb.DeleteAllScheduledTasks()
	return int(n), err
}

// DeleteAllRetryTasks deletes all tasks in retry state,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllRetryTasks() (int, error) {
	n, err := i.rdb.DeleteAllRetryTasks()
	return int(n), err
}

// DeleteAllDeadTasks deletes all tasks in dead state,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllDeadTasks() (int, error) {
	n, err := i.rdb.DeleteAllDeadTasks()
	return int(n), err
}

// DeleteTaskByKey deletes a task with the given key.
func (i *Inspector) DeleteTaskByKey(key string) error {
	id, score, qtype, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch qtype {
	case "s":
		return i.rdb.DeleteScheduledTask(id, score)
	case "r":
		return i.rdb.DeleteRetryTask(id, score)
	case "d":
		return i.rdb.DeleteDeadTask(id, score)
	default:
		return fmt.Errorf("invalid key")
	}
}

// EnqueueAllScheduledTasks enqueues all tasks in the scheduled state,
// and reports the number of tasks enqueued.
func (i *Inspector) EnqueueAllScheduledTasks() (int, error) {
	n, err := i.rdb.EnqueueAllScheduledTasks()
	return int(n), err
}

// EnqueueAllRetryTasks enqueues all tasks in the retry state,
// and reports the number of tasks enqueued.
func (i *Inspector) EnqueueAllRetryTasks() (int, error) {
	n, err := i.rdb.EnqueueAllRetryTasks()
	return int(n), err
}

// EnqueueAllDeadTasks enqueues all tasks in the dead state,
// and reports the number of tasks enqueued.
func (i *Inspector) EnqueueAllDeadTasks() (int, error) {
	n, err := i.rdb.EnqueueAllDeadTasks()
	return int(n), err
}

// EnqueueTaskByKey enqueues a task with the given key.
func (i *Inspector) EnqueueTaskByKey(key string) error {
	id, score, qtype, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch qtype {
	case "s":
		return i.rdb.EnqueueScheduledTask(id, score)
	case "r":
		return i.rdb.EnqueueRetryTask(id, score)
	case "d":
		return i.rdb.EnqueueDeadTask(id, score)
	default:
		return fmt.Errorf("invalid key")
	}
}

// KillAllScheduledTasks kills all tasks in scheduled state,
// and reports the number of tasks killed.
func (i *Inspector) KillAllScheduledTasks() (int, error) {
	n, err := i.rdb.KillAllScheduledTasks()
	return int(n), err
}

// KillAllRetryTasks kills all tasks in retry state,
// and reports the number of tasks killed.
func (i *Inspector) KillAllRetryTasks() (int, error) {
	n, err := i.rdb.KillAllRetryTasks()
	return int(n), err
}

// KillTaskByKey kills a task with the given key.
func (i *Inspector) KillTaskByKey(key string) error {
	id, score, qtype, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch qtype {
	case "s":
		return i.rdb.KillScheduledTask(id, score)
	case "r":
		return i.rdb.KillRetryTask(id, score)
	case "d":
		return fmt.Errorf("task already dead")
	default:
		return fmt.Errorf("invalid key")
	}
}

// PauseQueue pauses task processing on the specified queue.
// If the queue is already paused, it will return a non-nil error.
func (i *Inspector) PauseQueue(qname string) error {
	return i.rdb.Pause(qname)
}

// UnpauseQueue resumes task processing on the specified queue.
// If the queue is not paused, it will return a non-nil error.
func (i *Inspector) UnpauseQueue(qname string) error {
	return i.rdb.Unpause(qname)
}
