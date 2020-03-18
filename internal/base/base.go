// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package base defines foundational types and constants used in asynq package.
package base

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
)

// DefaultQueueName is the queue name used if none are specified by user.
const DefaultQueueName = "default"

// Redis keys
const (
	AllProcesses    = "asynq:ps"                     // ZSET
	psPrefix        = "asynq:ps:"                    // STRING - asynq:ps:<host>:<pid>
	AllWorkers      = "asynq:workers"                // ZSET
	workersPrefix   = "asynq:workers:"               // HASH   - asynq:workers:<host:<pid>
	processedPrefix = "asynq:processed:"             // STRING - asynq:processed:<yyyy-mm-dd>
	failurePrefix   = "asynq:failure:"               // STRING - asynq:failure:<yyyy-mm-dd>
	QueuePrefix     = "asynq:queues:"                // LIST   - asynq:queues:<qname>
	AllQueues       = "asynq:queues"                 // SET
	DefaultQueue    = QueuePrefix + DefaultQueueName // LIST
	ScheduledQueue  = "asynq:scheduled"              // ZSET
	RetryQueue      = "asynq:retry"                  // ZSET
	DeadQueue       = "asynq:dead"                   // ZSET
	InProgressQueue = "asynq:in_progress"            // LIST
	CancelChannel   = "asynq:cancel"                 // PubSub channel
)

// QueueKey returns a redis key for the given queue name.
func QueueKey(qname string) string {
	return QueuePrefix + strings.ToLower(qname)
}

// ProcessedKey returns a redis key for processed count for the given day.
func ProcessedKey(t time.Time) string {
	return processedPrefix + t.UTC().Format("2006-01-02")
}

// FailureKey returns a redis key for failure count for the given day.
func FailureKey(t time.Time) string {
	return failurePrefix + t.UTC().Format("2006-01-02")
}

// ProcessInfoKey returns a redis key for process info.
func ProcessInfoKey(hostname string, pid int) string {
	return fmt.Sprintf("%s%s:%d", psPrefix, hostname, pid)
}

// WorkersKey returns a redis key for the workers given hostname and pid.
func WorkersKey(hostname string, pid int) string {
	return fmt.Sprintf("%s%s:%d", workersPrefix, hostname, pid)
}

// TaskMessage is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written to redis.
type TaskMessage struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload map[string]interface{}

	// ID is a unique identifier for each task.
	ID xid.ID

	// Queue is a name this message should be enqueued to.
	Queue string

	// Retry is the max number of retry for this task.
	Retry int

	// Retried is the number of times we've retried this task so far.
	Retried int

	// ErrorMsg holds the error message from the last failure.
	ErrorMsg string

	// Timeout specifies how long a task may run.
	// The string value should be compatible with time.Duration.ParseDuration.
	//
	// Zero means no limit.
	Timeout string

	// Deadline specifies the deadline for the task.
	// Task won't be processed if it exceeded its deadline.
	// The string shoulbe be in RFC3339 format.
	//
	// time.Time's zero value means no deadline.
	Deadline string

	// UniqueKey holds the redis key used for uniqueness lock for this task.
	//
	// Empty string indicates that no uniqueness lock was used.
	UniqueKey string
}

// ProcessState holds process level information.
//
// ProcessStates are safe for concurrent use by multiple goroutines.
type ProcessState struct {
	mu             sync.Mutex // guards all data fields
	concurrency    int
	queues         map[string]int
	strictPriority bool
	pid            int
	host           string
	status         PStatus
	started        time.Time
	workers        map[string]*workerStats
}

// PStatus represents status of a process.
type PStatus int

const (
	// StatusIdle indicates process is in idle state.
	StatusIdle PStatus = iota

	// StatusRunning indicates process is up and processing tasks.
	StatusRunning

	// StatusStopped indicates process is up but not processing new tasks.
	StatusStopped
)

var statuses = []string{
	"idle",
	"running",
	"stopped",
}

func (s PStatus) String() string {
	if StatusIdle <= s && s <= StatusStopped {
		return statuses[s]
	}
	return "unknown status"
}

type workerStats struct {
	msg     *TaskMessage
	started time.Time
}

// NewProcessState returns a new instance of ProcessState.
func NewProcessState(host string, pid, concurrency int, queues map[string]int, strict bool) *ProcessState {
	return &ProcessState{
		host:           host,
		pid:            pid,
		concurrency:    concurrency,
		queues:         cloneQueueConfig(queues),
		strictPriority: strict,
		status:         StatusIdle,
		workers:        make(map[string]*workerStats),
	}
}

// SetStatus updates the state of process.
func (ps *ProcessState) SetStatus(status PStatus) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.status = status
}

// SetStarted records when the process started processing.
func (ps *ProcessState) SetStarted(t time.Time) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.started = t
}

// AddWorkerStats records when a worker started and which task it's processing.
func (ps *ProcessState) AddWorkerStats(msg *TaskMessage, started time.Time) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.workers[msg.ID.String()] = &workerStats{msg, started}
}

// DeleteWorkerStats removes a worker's entry from the process state.
func (ps *ProcessState) DeleteWorkerStats(msg *TaskMessage) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.workers, msg.ID.String())
}

// Get returns current state of process as a ProcessInfo.
func (ps *ProcessState) Get() *ProcessInfo {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return &ProcessInfo{
		Host:              ps.host,
		PID:               ps.pid,
		Concurrency:       ps.concurrency,
		Queues:            cloneQueueConfig(ps.queues),
		StrictPriority:    ps.strictPriority,
		Status:            ps.status.String(),
		Started:           ps.started,
		ActiveWorkerCount: len(ps.workers),
	}
}

// GetWorkers returns a list of currently running workers' info.
func (ps *ProcessState) GetWorkers() []*WorkerInfo {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	var res []*WorkerInfo
	for _, w := range ps.workers {
		res = append(res, &WorkerInfo{
			Host:    ps.host,
			PID:     ps.pid,
			ID:      w.msg.ID,
			Type:    w.msg.Type,
			Queue:   w.msg.Queue,
			Payload: clonePayload(w.msg.Payload),
			Started: w.started,
		})
	}
	return res
}

func cloneQueueConfig(qcfg map[string]int) map[string]int {
	res := make(map[string]int)
	for qname, n := range qcfg {
		res[qname] = n
	}
	return res
}

func clonePayload(payload map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for k, v := range payload {
		res[k] = v
	}
	return res
}

// ProcessInfo holds information about a running background worker process.
type ProcessInfo struct {
	Host              string
	PID               int
	Concurrency       int
	Queues            map[string]int
	StrictPriority    bool
	Status            string
	Started           time.Time
	ActiveWorkerCount int
}

// WorkerInfo holds information about a running worker.
type WorkerInfo struct {
	Host    string
	PID     int
	ID      xid.ID
	Type    string
	Queue   string
	Payload map[string]interface{}
	Started time.Time
}

// Cancelations is a collection that holds cancel functions for all in-progress tasks.
//
// Cancelations are safe for concurrent use by multipel goroutines.
type Cancelations struct {
	mu          sync.Mutex
	cancelFuncs map[string]context.CancelFunc
}

// NewCancelations returns a Cancelations instance.
func NewCancelations() *Cancelations {
	return &Cancelations{
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

// Add adds a new cancel func to the collection.
func (c *Cancelations) Add(id string, fn context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFuncs[id] = fn
}

// Delete deletes a cancel func from the collection given an id.
func (c *Cancelations) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cancelFuncs, id)
}

// Get returns a cancel func given an id.
func (c *Cancelations) Get(id string) (fn context.CancelFunc, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn, ok = c.cancelFuncs[id]
	return fn, ok
}

// GetAll returns all cancel funcs.
func (c *Cancelations) GetAll() []context.CancelFunc {
	c.mu.Lock()
	defer c.mu.Unlock()
	var res []context.CancelFunc
	for _, fn := range c.cancelFuncs {
		res = append(res, fn)
	}
	return res
}
