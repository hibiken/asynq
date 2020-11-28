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

// Close closes the connection with redis.
func (i *Inspector) Close() error {
	return i.rdb.Close()
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
	// The value is the sum of Pending, Active, Scheduled, Retry, and Dead.
	Size int
	// Number of pending tasks.
	Pending int
	// Number of active tasks.
	Active int
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
	if err := validateQueueName(qname); err != nil {
		return nil, err
	}
	stats, err := i.rdb.CurrentStats(qname)
	if err != nil {
		return nil, err
	}
	return &QueueStats{
		Queue:     stats.Queue,
		Size:      stats.Size,
		Pending:   stats.Pending,
		Active:    stats.Active,
		Scheduled: stats.Scheduled,
		Retry:     stats.Retry,
		Dead:      stats.Dead,
		Processed: stats.Processed,
		Failed:    stats.Failed,
		Paused:    stats.Paused,
		Timestamp: stats.Timestamp,
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
	if err := validateQueueName(qname); err != nil {
		return nil, err
	}
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

// ErrQueueNotFound indicates that the specified queue does not exist.
type ErrQueueNotFound struct {
	qname string
}

func (e *ErrQueueNotFound) Error() string {
	return fmt.Sprintf("queue %q does not exist", e.qname)
}

// ErrQueueNotEmpty indicates that the specified queue is not empty.
type ErrQueueNotEmpty struct {
	qname string
}

func (e *ErrQueueNotEmpty) Error() string {
	return fmt.Sprintf("queue %q is not empty", e.qname)
}

// DeleteQueue removes the specified queue.
//
// If force is set to true, DeleteQueue will remove the queue regardless of
// the queue size as long as no tasks are active in the queue.
// If force is set to false, DeleteQueue will remove the queue only if
// the queue is empty.
//
// If the specified queue does not exist, DeleteQueue returns ErrQueueNotFound.
// If force is set to false and the specified queue is not empty, DeleteQueue
// returns ErrQueueNotEmpty.
func (i *Inspector) DeleteQueue(qname string, force bool) error {
	err := i.rdb.RemoveQueue(qname, force)
	if _, ok := err.(*rdb.ErrQueueNotFound); ok {
		return &ErrQueueNotFound{qname}
	}
	if _, ok := err.(*rdb.ErrQueueNotEmpty); ok {
		return &ErrQueueNotEmpty{qname}
	}
	return err
}

// PendingTask is a task in a queue and is ready to be processed.
type PendingTask struct {
	*Task
	ID    string
	Queue string
}

// ActiveTask is a task that's currently being processed.
type ActiveTask struct {
	*Task
	ID    string
	Queue string
}

// ScheduledTask is a task scheduled to be processed in the future.
type ScheduledTask struct {
	*Task
	ID            string
	Queue         string
	NextProcessAt time.Time

	score int64
}

// RetryTask is a task scheduled to be retried in the future.
type RetryTask struct {
	*Task
	ID            string
	Queue         string
	NextProcessAt time.Time
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

// Key returns a key used to delete, run, and kill the task.
func (t *ScheduledTask) Key() string {
	return fmt.Sprintf("s:%v:%v", t.ID, t.score)
}

// Key returns a key used to delete, run, and kill the task.
func (t *RetryTask) Key() string {
	return fmt.Sprintf("r:%v:%v", t.ID, t.score)
}

// Key returns a key used to delete, run, and kill the task.
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

// ListPendingTasks retrieves pending tasks from the specified queue.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListPendingTasks(qname string, opts ...ListOption) ([]*PendingTask, error) {
	if err := validateQueueName(qname); err != nil {
		return nil, err
	}
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	msgs, err := i.rdb.ListPending(qname, pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*PendingTask
	for _, m := range msgs {
		tasks = append(tasks, &PendingTask{
			Task:  NewTask(m.Type, m.Payload),
			ID:    m.ID.String(),
			Queue: m.Queue,
		})
	}
	return tasks, err
}

// ListActiveTasks retrieves active tasks from the specified queue.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListActiveTasks(qname string, opts ...ListOption) ([]*ActiveTask, error) {
	if err := validateQueueName(qname); err != nil {
		return nil, err
	}
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	msgs, err := i.rdb.ListActive(qname, pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*ActiveTask
	for _, m := range msgs {
		tasks = append(tasks, &ActiveTask{
			Task:  NewTask(m.Type, m.Payload),
			ID:    m.ID.String(),
			Queue: m.Queue,
		})
	}
	return tasks, err
}

// ListScheduledTasks retrieves scheduled tasks from the specified queue.
// Tasks are sorted by NextProcessAt field in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListScheduledTasks(qname string, opts ...ListOption) ([]*ScheduledTask, error) {
	if err := validateQueueName(qname); err != nil {
		return nil, err
	}
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListScheduled(qname, pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*ScheduledTask
	for _, z := range zs {
		processAt := time.Unix(z.Score, 0)
		t := NewTask(z.Message.Type, z.Message.Payload)
		tasks = append(tasks, &ScheduledTask{
			Task:          t,
			ID:            z.Message.ID.String(),
			Queue:         z.Message.Queue,
			NextProcessAt: processAt,
			score:         z.Score,
		})
	}
	return tasks, nil
}

// ListRetryTasks retrieves retry tasks from the specified queue.
// Tasks are sorted by NextProcessAt field in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListRetryTasks(qname string, opts ...ListOption) ([]*RetryTask, error) {
	if err := validateQueueName(qname); err != nil {
		return nil, err
	}
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListRetry(qname, pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*RetryTask
	for _, z := range zs {
		processAt := time.Unix(z.Score, 0)
		t := NewTask(z.Message.Type, z.Message.Payload)
		tasks = append(tasks, &RetryTask{
			Task:          t,
			ID:            z.Message.ID.String(),
			Queue:         z.Message.Queue,
			NextProcessAt: processAt,
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
	if err := validateQueueName(qname); err != nil {
		return nil, err
	}
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
}

// DeleteAllScheduledTasks deletes all scheduled tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllScheduledTasks(qname string) (int, error) {
	if err := validateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllScheduledTasks(qname)
	return int(n), err
}

// DeleteAllRetryTasks deletes all retry tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllRetryTasks(qname string) (int, error) {
	if err := validateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllRetryTasks(qname)
	return int(n), err
}

// DeleteAllDeadTasks deletes all dead tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllDeadTasks(qname string) (int, error) {
	if err := validateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllDeadTasks(qname)
	return int(n), err
}

// DeleteTaskByKey deletes a task with the given key from the given queue.
func (i *Inspector) DeleteTaskByKey(qname, key string) error {
	if err := validateQueueName(qname); err != nil {
		return err
	}
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

// RunAllScheduledTasks transition all scheduled tasks to pending state within the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllScheduledTasks(qname string) (int, error) {
	if err := validateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllScheduledTasks(qname)
	return int(n), err
}

// RunAllRetryTasks transition all retry tasks to pending state within the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllRetryTasks(qname string) (int, error) {
	if err := validateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllRetryTasks(qname)
	return int(n), err
}

// RunAllDeadTasks transition all dead tasks to pending state within the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllDeadTasks(qname string) (int, error) {
	if err := validateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllDeadTasks(qname)
	return int(n), err
}

// RunTaskByKey transition a task to pending state given task key and queue name.
func (i *Inspector) RunTaskByKey(qname, key string) error {
	if err := validateQueueName(qname); err != nil {
		return err
	}
	id, score, state, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch state {
	case "s":
		return i.rdb.RunScheduledTask(qname, id, score)
	case "r":
		return i.rdb.RunRetryTask(qname, id, score)
	case "d":
		return i.rdb.RunDeadTask(qname, id, score)
	default:
		return fmt.Errorf("invalid key")
	}
}

// KillAllScheduledTasks kills all scheduled tasks within the given queue,
// and reports the number of tasks killed.
func (i *Inspector) KillAllScheduledTasks(qname string) (int, error) {
	if err := validateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.KillAllScheduledTasks(qname)
	return int(n), err
}

// KillAllRetryTasks kills all retry tasks within the given queue,
// and reports the number of tasks killed.
func (i *Inspector) KillAllRetryTasks(qname string) (int, error) {
	if err := validateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.KillAllRetryTasks(qname)
	return int(n), err
}

// KillTaskByKey kills a task with the given key in the given queue.
func (i *Inspector) KillTaskByKey(qname, key string) error {
	if err := validateQueueName(qname); err != nil {
		return err
	}
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
	if err := validateQueueName(qname); err != nil {
		return err
	}
	return i.rdb.Pause(qname)
}

// UnpauseQueue resumes task processing on the specified queue.
// If the queue is not paused, it will return a non-nil error.
func (i *Inspector) UnpauseQueue(qname string) error {
	if err := validateQueueName(qname); err != nil {
		return err
	}
	return i.rdb.Unpause(qname)
}

// ClusterKeySlot returns an integer identifying the hash slot the given queue hashes to.
func (i *Inspector) ClusterKeySlot(qname string) (int64, error) {
	return i.rdb.ClusterKeySlot(qname)
}

// ClusterNode describes a node in redis cluster.
type ClusterNode struct {
	// Node ID in the cluster.
	ID string

	// Address of the node.
	Addr string
}

// ClusterNode returns a list of nodes the given queue belongs to.
func (i *Inspector) ClusterNodes(qname string) ([]ClusterNode, error) {
	nodes, err := i.rdb.ClusterNodes(qname)
	if err != nil {
		return nil, err
	}
	var res []ClusterNode
	for _, node := range nodes {
		res = append(res, ClusterNode{ID: node.ID, Addr: node.Addr})
	}
	return res, nil
}
