// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package inspeq

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

// Inspector is a client interface to inspect and mutate the state of
// queues and tasks.
type Inspector struct {
	rdb *rdb.RDB
}

// New returns a new instance of Inspector.
func New(r asynq.RedisConnOpt) *Inspector {
	c, ok := r.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("inspeq: unsupported RedisConnOpt type %T", r))
	}
	return &Inspector{
		rdb: rdb.NewRDB(c),
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
	// Total number of bytes that the queue and its tasks require to be stored in redis.
	MemoryUsage int64
	// Size is the total number of tasks in the queue.
	// The value is the sum of Pending, Active, Scheduled, Retry, and Archived.
	Size int
	// Number of pending tasks.
	Pending int
	// Number of active tasks.
	Active int
	// Number of scheduled tasks.
	Scheduled int
	// Number of retry tasks.
	Retry int
	// Number of archived tasks.
	Archived int
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
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, err
	}
	stats, err := i.rdb.CurrentStats(qname)
	if err != nil {
		return nil, err
	}
	return &QueueStats{
		Queue:       stats.Queue,
		MemoryUsage: stats.MemoryUsage,
		Size:        stats.Size,
		Pending:     stats.Pending,
		Active:      stats.Active,
		Scheduled:   stats.Scheduled,
		Retry:       stats.Retry,
		Archived:    stats.Archived,
		Processed:   stats.Processed,
		Failed:      stats.Failed,
		Paused:      stats.Paused,
		Timestamp:   stats.Timestamp,
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
	if err := base.ValidateQueueName(qname); err != nil {
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
	*asynq.Task
	ID        string
	Queue     string
	MaxRetry  int
	Retried   int
	LastError string
}

// ActiveTask is a task that's currently being processed.
type ActiveTask struct {
	*asynq.Task
	ID        string
	Queue     string
	MaxRetry  int
	Retried   int
	LastError string
}

// ScheduledTask is a task scheduled to be processed in the future.
type ScheduledTask struct {
	*asynq.Task
	ID            string
	Queue         string
	MaxRetry      int
	Retried       int
	LastError     string
	NextProcessAt time.Time

	score int64
}

// RetryTask is a task scheduled to be retried in the future.
type RetryTask struct {
	*asynq.Task
	ID            string
	Queue         string
	NextProcessAt time.Time
	MaxRetry      int
	Retried       int
	LastError     string
	// TODO: LastFailedAt  time.Time

	score int64
}

// ArchivedTask is a task archived for debugging and inspection purposes, and
// it won't be retried automatically.
// A task can be archived when the task exhausts its retry counts or manually
// archived by a user via the CLI or Inspector.
type ArchivedTask struct {
	*asynq.Task
	ID           string
	Queue        string
	MaxRetry     int
	Retried      int
	LastFailedAt time.Time
	LastError    string

	score int64
}

// Format string used for task key.
// Format is <prefix>:<uuid>:<score>.
const taskKeyFormat = "%s:%v:%v"

// Prefix used for task key.
const (
	keyPrefixPending   = "p"
	keyPrefixScheduled = "s"
	keyPrefixRetry     = "r"
	keyPrefixArchived  = "a"

	allKeyPrefixes = keyPrefixPending + keyPrefixScheduled + keyPrefixRetry + keyPrefixArchived
)

// Key returns a key used to delete, and archive the pending task.
func (t *PendingTask) Key() string {
	// Note: Pending tasks are stored in redis LIST, therefore no score.
	// Use zero for the score to use the same key format.
	return fmt.Sprintf(taskKeyFormat, keyPrefixPending, t.ID, 0)
}

// Key returns a key used to delete, run, and archive the scheduled task.
func (t *ScheduledTask) Key() string {
	return fmt.Sprintf(taskKeyFormat, keyPrefixScheduled, t.ID, t.score)
}

// Key returns a key used to delete, run, and archive the retry task.
func (t *RetryTask) Key() string {
	return fmt.Sprintf(taskKeyFormat, keyPrefixRetry, t.ID, t.score)
}

// Key returns a key used to delete and run the archived task.
func (t *ArchivedTask) Key() string {
	return fmt.Sprintf(taskKeyFormat, keyPrefixArchived, t.ID, t.score)
}

// parseTaskKey parses a key string and returns each part of key with proper
// type if valid, otherwise it reports an error.
func parseTaskKey(key string) (prefix string, id uuid.UUID, score int64, err error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 {
		return "", uuid.Nil, 0, fmt.Errorf("invalid id")
	}
	id, err = uuid.Parse(parts[1])
	if err != nil {
		return "", uuid.Nil, 0, fmt.Errorf("invalid id")
	}
	score, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", uuid.Nil, 0, fmt.Errorf("invalid id")
	}
	prefix = parts[0]
	if len(prefix) != 1 || !strings.Contains(allKeyPrefixes, prefix) {
		return "", uuid.Nil, 0, fmt.Errorf("invalid id")
	}
	return prefix, id, score, nil
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
	if err := base.ValidateQueueName(qname); err != nil {
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
			Task:      asynq.NewTask(m.Type, m.Payload),
			ID:        m.ID.String(),
			Queue:     m.Queue,
			MaxRetry:  m.Retry,
			Retried:   m.Retried,
			LastError: m.ErrorMsg,
		})
	}
	return tasks, err
}

// ListActiveTasks retrieves active tasks from the specified queue.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListActiveTasks(qname string, opts ...ListOption) ([]*ActiveTask, error) {
	if err := base.ValidateQueueName(qname); err != nil {
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
			Task:      asynq.NewTask(m.Type, m.Payload),
			ID:        m.ID.String(),
			Queue:     m.Queue,
			MaxRetry:  m.Retry,
			Retried:   m.Retried,
			LastError: m.ErrorMsg,
		})
	}
	return tasks, err
}

// ListScheduledTasks retrieves scheduled tasks from the specified queue.
// Tasks are sorted by NextProcessAt field in ascending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListScheduledTasks(qname string, opts ...ListOption) ([]*ScheduledTask, error) {
	if err := base.ValidateQueueName(qname); err != nil {
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
		t := asynq.NewTask(z.Message.Type, z.Message.Payload)
		tasks = append(tasks, &ScheduledTask{
			Task:          t,
			ID:            z.Message.ID.String(),
			Queue:         z.Message.Queue,
			MaxRetry:      z.Message.Retry,
			Retried:       z.Message.Retried,
			LastError:     z.Message.ErrorMsg,
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
	if err := base.ValidateQueueName(qname); err != nil {
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
		t := asynq.NewTask(z.Message.Type, z.Message.Payload)
		tasks = append(tasks, &RetryTask{
			Task:          t,
			ID:            z.Message.ID.String(),
			Queue:         z.Message.Queue,
			NextProcessAt: processAt,
			MaxRetry:      z.Message.Retry,
			Retried:       z.Message.Retried,
			// TODO: LastFailedAt: z.Message.LastFailedAt
			LastError: z.Message.ErrorMsg,
			score:     z.Score,
		})
	}
	return tasks, nil
}

// ListArchivedTasks retrieves archived tasks from the specified queue.
// Tasks are sorted by LastFailedAt field in descending order.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListArchivedTasks(qname string, opts ...ListOption) ([]*ArchivedTask, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return nil, err
	}
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	zs, err := i.rdb.ListArchived(qname, pgn)
	if err != nil {
		return nil, err
	}
	var tasks []*ArchivedTask
	for _, z := range zs {
		failedAt := time.Unix(z.Score, 0)
		t := asynq.NewTask(z.Message.Type, z.Message.Payload)
		tasks = append(tasks, &ArchivedTask{
			Task:         t,
			ID:           z.Message.ID.String(),
			Queue:        z.Message.Queue,
			MaxRetry:     z.Message.Retry,
			Retried:      z.Message.Retried,
			LastFailedAt: failedAt,
			LastError:    z.Message.ErrorMsg,
			score:        z.Score,
		})
	}
	return tasks, nil
}

// DeleteAllPendingTasks deletes all pending tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllPendingTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllPendingTasks(qname)
	return int(n), err
}

// DeleteAllScheduledTasks deletes all scheduled tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllScheduledTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllScheduledTasks(qname)
	return int(n), err
}

// DeleteAllRetryTasks deletes all retry tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllRetryTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllRetryTasks(qname)
	return int(n), err
}

// DeleteAllArchivedTasks deletes all archived tasks from the specified queue,
// and reports the number tasks deleted.
func (i *Inspector) DeleteAllArchivedTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.DeleteAllArchivedTasks(qname)
	return int(n), err
}

// DeleteTaskByKey deletes a task with the given key from the given queue.
// TODO: We don't need score any more. Update this to delete task by ID
func (i *Inspector) DeleteTaskByKey(qname, key string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return err
	}
	prefix, id, _, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch prefix {
	case keyPrefixPending:
		return i.rdb.DeletePendingTask(qname, id)
	case keyPrefixScheduled:
		return i.rdb.DeleteScheduledTask(qname, id)
	case keyPrefixRetry:
		return i.rdb.DeleteRetryTask(qname, id)
	case keyPrefixArchived:
		return i.rdb.DeleteArchivedTask(qname, id)
	default:
		return fmt.Errorf("invalid key")
	}
}

// RunAllScheduledTasks transition all scheduled tasks to pending state from the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllScheduledTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllScheduledTasks(qname)
	return int(n), err
}

// RunAllRetryTasks transition all retry tasks to pending state from the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllRetryTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllRetryTasks(qname)
	return int(n), err
}

// RunAllArchivedTasks transition all archived tasks to pending state from the given queue,
// and reports the number of tasks transitioned.
func (i *Inspector) RunAllArchivedTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.RunAllArchivedTasks(qname)
	return int(n), err
}

// RunTaskByKey transition a task to pending state given task key and queue name.
// TODO: Update this to run task by ID.
func (i *Inspector) RunTaskByKey(qname, key string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return err
	}
	prefix, id, _, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch prefix {
	case keyPrefixScheduled:
		return i.rdb.RunTask(qname, id)
	case keyPrefixRetry:
		return i.rdb.RunTask(qname, id)
	case keyPrefixArchived:
		return i.rdb.RunTask(qname, id)
	case keyPrefixPending:
		return fmt.Errorf("task is already pending for run")
	default:
		return fmt.Errorf("invalid key")
	}
}

// ArchiveAllPendingTasks archives all pending tasks from the given queue,
// and reports the number of tasks archived.
func (i *Inspector) ArchiveAllPendingTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.ArchiveAllPendingTasks(qname)
	return int(n), err
}

// ArchiveAllScheduledTasks archives all scheduled tasks from the given queue,
// and reports the number of tasks archiveed.
func (i *Inspector) ArchiveAllScheduledTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.ArchiveAllScheduledTasks(qname)
	return int(n), err
}

// ArchiveAllRetryTasks archives all retry tasks from the given queue,
// and reports the number of tasks archiveed.
func (i *Inspector) ArchiveAllRetryTasks(qname string) (int, error) {
	if err := base.ValidateQueueName(qname); err != nil {
		return 0, err
	}
	n, err := i.rdb.ArchiveAllRetryTasks(qname)
	return int(n), err
}

// ArchiveTaskByKey archives a task with the given key in the given queue.
// TODO: Update this to Archive task by ID.
func (i *Inspector) ArchiveTaskByKey(qname, key string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return err
	}
	prefix, id, _, err := parseTaskKey(key)
	if err != nil {
		return err
	}
	switch prefix {
	case keyPrefixPending:
		return i.rdb.ArchivePendingTask(qname, id)
	case keyPrefixScheduled:
		return i.rdb.ArchiveScheduledTask(qname, id)
	case keyPrefixRetry:
		return i.rdb.ArchiveRetryTask(qname, id)
	case keyPrefixArchived:
		return fmt.Errorf("task is already archived")
	default:
		return fmt.Errorf("invalid key")
	}
}

// CancelActiveTask sends a signal to cancel processing of the task with
// the given id. CancelActiveTask is best-effort, which means that it does not
// guarantee that the task with the given id will be canceled. The return
// value only indicates whether the cancelation signal has been sent.
func (i *Inspector) CancelActiveTask(id string) error {
	return i.rdb.PublishCancelation(id)
}

// PauseQueue pauses task processing on the specified queue.
// If the queue is already paused, it will return a non-nil error.
func (i *Inspector) PauseQueue(qname string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return err
	}
	return i.rdb.Pause(qname)
}

// UnpauseQueue resumes task processing on the specified queue.
// If the queue is not paused, it will return a non-nil error.
func (i *Inspector) UnpauseQueue(qname string) error {
	if err := base.ValidateQueueName(qname); err != nil {
		return err
	}
	return i.rdb.Unpause(qname)
}

// Servers return a list of running servers' information.
func (i *Inspector) Servers() ([]*ServerInfo, error) {
	servers, err := i.rdb.ListServers()
	if err != nil {
		return nil, err
	}
	workers, err := i.rdb.ListWorkers()
	if err != nil {
		return nil, err
	}
	m := make(map[string]*ServerInfo) // ServerInfo keyed by serverID
	for _, s := range servers {
		m[s.ServerID] = &ServerInfo{
			ID:             s.ServerID,
			Host:           s.Host,
			PID:            s.PID,
			Concurrency:    s.Concurrency,
			Queues:         s.Queues,
			StrictPriority: s.StrictPriority,
			Started:        s.Started,
			Status:         s.Status,
			ActiveWorkers:  make([]*WorkerInfo, 0),
		}
	}
	for _, w := range workers {
		srvInfo, ok := m[w.ServerID]
		if !ok {
			continue
		}
		wrkInfo := &WorkerInfo{
			Started:  w.Started,
			Deadline: w.Deadline,
			Task: &ActiveTask{
				Task:  asynq.NewTask(w.Type, w.Payload),
				ID:    w.ID,
				Queue: w.Queue,
			},
		}
		srvInfo.ActiveWorkers = append(srvInfo.ActiveWorkers, wrkInfo)
	}
	var out []*ServerInfo
	for _, srvInfo := range m {
		out = append(out, srvInfo)
	}
	return out, nil
}

// ServerInfo describes a running Server instance.
type ServerInfo struct {
	// Unique Identifier for the server.
	ID string
	// Host machine on which the server is running.
	Host string
	// PID of the process in which the server is running.
	PID int

	// Server configuration details.
	// See Config doc for field descriptions.
	Concurrency    int
	Queues         map[string]int
	StrictPriority bool

	// Time the server started.
	Started time.Time
	// Status indicates the status of the server.
	// TODO: Update comment with more details.
	Status string
	// A List of active workers currently processing tasks.
	ActiveWorkers []*WorkerInfo
}

// WorkerInfo describes a running worker processing a task.
type WorkerInfo struct {
	// The task the worker is processing.
	Task *ActiveTask
	// Time the worker started processing the task.
	Started time.Time
	// Time the worker needs to finish processing the task by.
	Deadline time.Time
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

// ClusterNodes returns a list of nodes the given queue belongs to.
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

// SchedulerEntry holds information about a periodic task registered with a scheduler.
type SchedulerEntry struct {
	// Identifier of this entry.
	ID string

	// Spec describes the schedule of this entry.
	Spec string

	// Periodic Task registered for this entry.
	Task *asynq.Task

	// Opts is the options for the periodic task.
	Opts []asynq.Option

	// Next shows the next time the task will be enqueued.
	Next time.Time

	// Prev shows the last time the task was enqueued.
	// Zero time if task was never enqueued.
	Prev time.Time
}

// SchedulerEntries returns a list of all entries registered with
// currently running schedulers.
func (i *Inspector) SchedulerEntries() ([]*SchedulerEntry, error) {
	var entries []*SchedulerEntry
	res, err := i.rdb.ListSchedulerEntries()
	if err != nil {
		return nil, err
	}
	for _, e := range res {
		task := asynq.NewTask(e.Type, e.Payload)
		var opts []asynq.Option
		for _, s := range e.Opts {
			if o, err := parseOption(s); err == nil {
				// ignore bad data
				opts = append(opts, o)
			}
		}
		entries = append(entries, &SchedulerEntry{
			ID:   e.ID,
			Spec: e.Spec,
			Task: task,
			Opts: opts,
			Next: e.Next,
			Prev: e.Prev,
		})
	}
	return entries, nil
}

// parseOption interprets a string s as an Option and returns the Option if parsing is successful,
// otherwise returns non-nil error.
func parseOption(s string) (asynq.Option, error) {
	fn, arg := parseOptionFunc(s), parseOptionArg(s)
	switch fn {
	case "Queue":
		qname, err := strconv.Unquote(arg)
		if err != nil {
			return nil, err
		}
		return asynq.Queue(qname), nil
	case "MaxRetry":
		n, err := strconv.Atoi(arg)
		if err != nil {
			return nil, err
		}
		return asynq.MaxRetry(n), nil
	case "Timeout":
		d, err := time.ParseDuration(arg)
		if err != nil {
			return nil, err
		}
		return asynq.Timeout(d), nil
	case "Deadline":
		t, err := time.Parse(time.UnixDate, arg)
		if err != nil {
			return nil, err
		}
		return asynq.Deadline(t), nil
	case "Unique":
		d, err := time.ParseDuration(arg)
		if err != nil {
			return nil, err
		}
		return asynq.Unique(d), nil
	case "ProcessAt":
		t, err := time.Parse(time.UnixDate, arg)
		if err != nil {
			return nil, err
		}
		return asynq.ProcessAt(t), nil
	case "ProcessIn":
		d, err := time.ParseDuration(arg)
		if err != nil {
			return nil, err
		}
		return asynq.ProcessIn(d), nil
	default:
		return nil, fmt.Errorf("cannot not parse option string %q", s)
	}
}

func parseOptionFunc(s string) string {
	i := strings.Index(s, "(")
	return s[:i]
}

func parseOptionArg(s string) string {
	i := strings.Index(s, "(")
	if i >= 0 {
		j := strings.Index(s, ")")
		if j > i {
			return s[i+1 : j]
		}
	}
	return ""
}

// SchedulerEnqueueEvent holds information about an enqueue event by a scheduler.
type SchedulerEnqueueEvent struct {
	// ID of the task that was enqueued.
	TaskID string

	// Time the task was enqueued.
	EnqueuedAt time.Time
}

// ListSchedulerEnqueueEvents retrieves a list of enqueue events from the specified scheduler entry.
//
// By default, it retrieves the first 30 tasks.
func (i *Inspector) ListSchedulerEnqueueEvents(entryID string, opts ...ListOption) ([]*SchedulerEnqueueEvent, error) {
	opt := composeListOptions(opts...)
	pgn := rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1}
	data, err := i.rdb.ListSchedulerEnqueueEvents(entryID, pgn)
	if err != nil {
		return nil, err
	}
	var events []*SchedulerEnqueueEvent
	for _, e := range data {
		events = append(events, &SchedulerEnqueueEvent{TaskID: e.TaskID, EnqueuedAt: e.EnqueuedAt})
	}
	return events, nil
}
