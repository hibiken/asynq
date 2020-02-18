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
	psPrefix        = "asynq:ps:"                    // HASH
	AllProcesses    = "asynq:ps"                     // ZSET
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

// QueueKey returns a redis key string for the given queue name.
func QueueKey(qname string) string {
	return QueuePrefix + strings.ToLower(qname)
}

// ProcessedKey returns a redis key string for processed count
// for the given day.
func ProcessedKey(t time.Time) string {
	return processedPrefix + t.UTC().Format("2006-01-02")
}

// FailureKey returns a redis key string for failure count
// for the given day.
func FailureKey(t time.Time) string {
	return failurePrefix + t.UTC().Format("2006-01-02")
}

// ProcessInfoKey returns a redis key string for process info.
func ProcessInfoKey(hostname string, pid int) string {
	return fmt.Sprintf("%s%s:%d", psPrefix, hostname, pid)
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
}

// ProcessState holds process level information.
//
// ProcessStates are safe for concurrent use by multiple goroutines.
type ProcessState struct {
	mu                sync.Mutex // guards all data fields
	concurrency       int
	queues            map[string]int
	strictPriority    bool
	pid               int
	host              string
	status            PStatus
	started           time.Time
	activeWorkerCount int
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

// NewProcessState returns a new instance of ProcessState.
func NewProcessState(host string, pid, concurrency int, queues map[string]int, strict bool) *ProcessState {
	return &ProcessState{
		host:           host,
		pid:            pid,
		concurrency:    concurrency,
		queues:         cloneQueueConfig(queues),
		strictPriority: strict,
		status:         StatusIdle,
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

// IncrWorkerCount increments the worker count by delta.
func (ps *ProcessState) IncrWorkerCount(delta int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.activeWorkerCount += delta
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
		ActiveWorkerCount: ps.activeWorkerCount,
	}
}

func cloneQueueConfig(qcfg map[string]int) map[string]int {
	res := make(map[string]int)
	for qname, n := range qcfg {
		res[qname] = n
	}
	return res
}

// ProcessInfo holds information about running background worker process.
type ProcessInfo struct {
	Concurrency       int
	Queues            map[string]int
	StrictPriority    bool
	PID               int
	Host              string
	Status            string
	Started           time.Time
	ActiveWorkerCount int
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
func (c *Cancelations) Get(id string) context.CancelFunc {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancelFuncs[id]
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
