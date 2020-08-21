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

// Queues returns a list of all queue names.
func (i *Inspector) Queues() ([]string, error) {
	return i.rdb.AllQueues()
}

// QueueStats represents a state of queues at a certain time.
type QueueStats struct {
	// Name of the queue.
	Queue string
	// Size is the total number of tasks in the queue.
	// The value is the sum of Enqueued, InProgress, Scheduled, Retry, and Dead.
	Size int
	// Number of enqueued tasks.
	Enqueued int
	// Number of in-progress tasks.
	InProgress int
	// Number of scheduled tasks.
	Scheduled int
	// Number of retry tasks.
	Retry int
	// Number of dead tasks.
	Dead int
	// Total number of tasks being processed during the given date.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed to be processed during the given date.
	Failed int
	// Paused indicates whether the queue is paused.
	// If true, tasks in the queue will not be processed.
	Paused bool
	// Time when this stats was taken.
	Timestamp time.Time
}

// CurrentStats returns a current stats of the given queue.
func (i *Inspector) CurrentStats(qname string) (*QueueStats, error) {
	stats, err := i.rdb.CurrentStats(qname)
	if err != nil {
		return nil, err
	}
	return &QueueStats{
		Queue:      stats.Queue,
		Size:       stats.Size,
		Enqueued:   stats.Enqueued,
		InProgress: stats.InProgress,
		Scheduled:  stats.Scheduled,
		Retry:      stats.Retry,
		Dead:       stats.Dead,
		Processed:  stats.Processed,
		Failed:     stats.Failed,
		Paused:     stats.Paused,
		Timestamp:  stats.Timestamp,
	}, nil
}

// DailyStats holds aggregate data for a given day for a given queue.
type DailyStats struct {
	// Name of the queue.
	Queue string
	// Total number of tasks being processed during the given date.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed to be processed during the given date.
	Failed int
	// Date this stats was taken.
	Date time.Time
}

// History returns a list of stats from the last n days.
func (i *Inspector) History(qname string, n int) ([]*DailyStats, error) {
	stats, err := i.rdb.HistoricalStats(qname, n)
	if err != nil {
		return nil, err
	}
	var res []*DailyStats
	for _, s := range stats {
		res = append(res, &DailyStats{
			Queue:     s.Queue,
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
	ID    string
	Queue string
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
func parseTaskKey(key string) (id uuid.UUID, score int64, state string, err error) {
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
	state = parts[0]
	if len(state) != 1 || !strings.Contains("srd", state) {
		return uuid.Nil, 0, "", fmt.Errorf("invalid id")
	}
	return id, score, state, nil
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

// ListEnqueuedTasks retrieves enqueued tasks from the specified queue.
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

// ListInProgressTasks retrieves in-progress tasks from the specified queue.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListInProgressTasks(qname string, opts ...ListOption) ([]*InProgressTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	msgs, err := i.rdb.ListInProgress(qname, pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*InProgressTask
	for _, m := range msgs {
		tasks = append(tasks, &InProgressTask{
			Task:  NewTask(m.Type, m.Payload),
			ID:    m.ID.String(),
			Queue: m.Queue,
		})
	}
	return tasks, err
}

// ListScheduledTasks retrieves scheduled tasks from the specified queue.
// Tasks are sorted by NextEnqueueAt field in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListScheduledTasks(qname string, opts ...ListOption) ([]*ScheduledTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListScheduled(qname, pgn)
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

// ListRetryTasks retrieves retry tasks from the specified queue.
// Tasks are sorted by NextEnqueueAt field in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListRetryTasks(qname string, opts ...ListOption) ([]*RetryTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListRetry(qname, pgn)
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

// ListDeadTasks retrieves dead tasks from the specified queue.
// Tasks are sorted by LastFailedAt field in descending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListDeadTasks(qname string, opts ...ListOption) ([]*DeadTask, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListDead(qname, pgn)
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

// DeleteAllScheduledTasks deletes all scheduled tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllScheduledTasks(qname string) (int, error) {
	n, err := i.rdb.DeleteAllScheduledTasks(qname)
	return int(n), err
}

// DeleteAllRetryTasks deletes all retry tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllRetryTasks(qname string) (int, error) {
	n, err := i.rdb.DeleteAllRetryTasks(qname)
	return int(n), err
}

// DeleteAllDeadTasks deletes all dead tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllDeadTasks(qname string) (int, error) {
	n, err := i.rdb.DeleteAllDeadTasks(qname)
	return int(n), err
}

// DeleteTaskByKey deletes a task with the given key from the given queue.
func (i *Inspector) DeleteTaskByKey(qname, key string) error {
	id, score, state, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch state {
	case "s":
		return i.rdb.DeleteScheduledTask(qname, id, score)
	case "r":
		return i.rdb.DeleteRetryTask(qname, id, score)
	case "d":
		return i.rdb.DeleteDeadTask(qname, id, score)
	default:
		return fmt.Errorf("invalid key")
	}
}

// TODO(hibiken): Use different verb here. Idea: Run or Stage
// EnqueueAllScheduledTasks enqueues all scheduled tasks for immediate processing within the given queue,
// and reports the number of tasks enqueued.
func (i *Inspector) EnqueueAllScheduledTasks(qname string) (int, error) {
	n, err := i.rdb.EnqueueAllScheduledTasks(qname)
	return int(n), err
}

// EnqueueAllRetryTasks enqueues all retry tasks for immediate processing within the given queue,
// and reports the number of tasks enqueued.
func (i *Inspector) EnqueueAllRetryTasks(qname string) (int, error) {
	n, err := i.rdb.EnqueueAllRetryTasks(qname)
	return int(n), err
}

// EnqueueAllDeadTasks enqueues all dead tasks for immediate processing within the given queue,
// and reports the number of tasks enqueued.
func (i *Inspector) EnqueueAllDeadTasks(qname string) (int, error) {
	n, err := i.rdb.EnqueueAllDeadTasks(qname)
	return int(n), err
}

// EnqueueTaskByKey enqueues a task with the given key in the given queue.
func (i *Inspector) EnqueueTaskByKey(qname, key string) error {
	id, score, state, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch state {
	case "s":
		return i.rdb.EnqueueScheduledTask(qname, id, score)
	case "r":
		return i.rdb.EnqueueRetryTask(qname, id, score)
	case "d":
		return i.rdb.EnqueueDeadTask(qname, id, score)
	default:
		return fmt.Errorf("invalid key")
	}
}

// KillAllScheduledTasks kills all scheduled tasks within the given queue,
// and reports the number of tasks killed.
func (i *Inspector) KillAllScheduledTasks(qname string) (int, error) {
	n, err := i.rdb.KillAllScheduledTasks(qname)
	return int(n), err
}

// KillAllRetryTasks kills all retry tasks within the given queue,
// and reports the number of tasks killed.
func (i *Inspector) KillAllRetryTasks(qname string) (int, error) {
	n, err := i.rdb.KillAllRetryTasks(qname)
	return int(n), err
}

// KillTaskByKey kills a task with the given key in the given queue.
func (i *Inspector) KillTaskByKey(qname, key string) error {
	id, score, state, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch state {
	case "s":
		return i.rdb.KillScheduledTask(qname, id, score)
	case "r":
		return i.rdb.KillRetryTask(qname, id, score)
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
