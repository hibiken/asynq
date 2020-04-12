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
	AllServers      = "asynq:servers"                // ZSET
	serversPrefix   = "asynq:servers:"               // STRING - asynq:ps:<host>:<pid>:<serverid>
	AllWorkers      = "asynq:workers"                // ZSET
	workersPrefix   = "asynq:workers:"               // HASH   - asynq:workers:<host:<pid>:<serverid>
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

// ServerInfoKey returns a redis key for process info.
func ServerInfoKey(hostname string, pid int, sid string) string {
	return fmt.Sprintf("%s%s:%d:%s", serversPrefix, hostname, pid, sid)
}

// WorkersKey returns a redis key for the workers given hostname, pid, and server ID.
func WorkersKey(hostname string, pid int, sid string) string {
	return fmt.Sprintf("%s%s:%d:%s", workersPrefix, hostname, pid, sid)
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

// ServerState holds process level information.
//
// ServerStates are safe for concurrent use by multiple goroutines.
type ServerState struct {
	mu             sync.Mutex // guards all data fields
	id             xid.ID
	concurrency    int
	queues         map[string]int
	strictPriority bool
	pid            int
	host           string
	status         ServerStatus
	started        time.Time
	workers        map[string]*workerStats
}

// ServerStatus represents status of a server.
type ServerStatus int

const (
	// StatusIdle indicates the server is in idle state.
	StatusIdle ServerStatus = iota

	// StatusRunning indicates the servier is up and processing tasks.
	StatusRunning

	// StatusQuiet indicates the server is up but not processing new tasks.
	StatusQuiet

	// StatusStopped indicates the server server has been stopped.
	StatusStopped
)

var statuses = []string{
	"idle",
	"running",
	"quiet",
	"stopped",
}

func (s ServerStatus) String() string {
	if StatusIdle <= s && s <= StatusStopped {
		return statuses[s]
	}
	return "unknown status"
}

type workerStats struct {
	msg     *TaskMessage
	started time.Time
}

// NewServerState returns a new instance of ServerState.
func NewServerState(host string, pid, concurrency int, queues map[string]int, strict bool) *ServerState {
	return &ServerState{
		host:           host,
		pid:            pid,
		id:             xid.New(),
		concurrency:    concurrency,
		queues:         cloneQueueConfig(queues),
		strictPriority: strict,
		status:         StatusIdle,
		workers:        make(map[string]*workerStats),
	}
}

// SetStatus updates the status of server.
func (ss *ServerState) SetStatus(status ServerStatus) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.status = status
}

// Status returns the status of server.
func (ss *ServerState) Status() ServerStatus {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.status
}

// SetStarted records when the process started processing.
func (ss *ServerState) SetStarted(t time.Time) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.started = t
}

// AddWorkerStats records when a worker started and which task it's processing.
func (ss *ServerState) AddWorkerStats(msg *TaskMessage, started time.Time) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.workers[msg.ID.String()] = &workerStats{msg, started}
}

// DeleteWorkerStats removes a worker's entry from the process state.
func (ss *ServerState) DeleteWorkerStats(msg *TaskMessage) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.workers, msg.ID.String())
}

// GetInfo returns current state of server as a ServerInfo.
func (ss *ServerState) GetInfo() *ServerInfo {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return &ServerInfo{
		Host:              ss.host,
		PID:               ss.pid,
		ServerID:          ss.id.String(),
		Concurrency:       ss.concurrency,
		Queues:            cloneQueueConfig(ss.queues),
		StrictPriority:    ss.strictPriority,
		Status:            ss.status.String(),
		Started:           ss.started,
		ActiveWorkerCount: len(ss.workers),
	}
}

// GetWorkers returns a list of currently running workers' info.
func (ss *ServerState) GetWorkers() []*WorkerInfo {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	var res []*WorkerInfo
	for _, w := range ss.workers {
		res = append(res, &WorkerInfo{
			Host:    ss.host,
			PID:     ss.pid,
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

// ServerInfo holds information about a running server.
type ServerInfo struct {
	Host              string
	PID               int
	ServerID          string
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
