// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package base defines foundational types and constants used in asynq package.
package base

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
)

// Version of asynq library and CLI.
const Version = "0.13.0"

// DefaultQueueName is the queue name used if none are specified by user.
const DefaultQueueName = "default"

// DefaultQueue is the redis key for the default queue.
var DefaultQueue = QueueKey(DefaultQueueName)

// Global Redis keys.
const (
	AllServers    = "asynq:servers"    // ZSET
	AllWorkers    = "asynq:workers"    // ZSET
	AllSchedulers = "asynq:schedulers" // ZSET
	AllQueues     = "asynq:queues"     // SET
	CancelChannel = "asynq:cancel"     // PubSub channel
)

// QueueKey returns a redis key for the given queue name.
func QueueKey(qname string) string {
	return fmt.Sprintf("asynq:{%s}", qname)
}

// ActiveKey returns a redis key for the active tasks.
func ActiveKey(qname string) string {
	return fmt.Sprintf("asynq:{%s}:active", qname)
}

// ScheduledKey returns a redis key for the scheduled tasks.
func ScheduledKey(qname string) string {
	return fmt.Sprintf("asynq:{%s}:scheduled", qname)
}

// RetryKey returns a redis key for the retry tasks.
func RetryKey(qname string) string {
	return fmt.Sprintf("asynq:{%s}:retry", qname)
}

// DeadKey returns a redis key for the dead tasks.
func DeadKey(qname string) string {
	return fmt.Sprintf("asynq:{%s}:dead", qname)
}

// DeadlinesKey returns a redis key for the deadlines.
func DeadlinesKey(qname string) string {
	return fmt.Sprintf("asynq:{%s}:deadlines", qname)
}

// PausedKey returns a redis key to indicate that the given queue is paused.
func PausedKey(qname string) string {
	return fmt.Sprintf("asynq:{%s}:paused", qname)
}

// ProcessedKey returns a redis key for processed count for the given day for the queue.
func ProcessedKey(qname string, t time.Time) string {
	return fmt.Sprintf("asynq:{%s}:processed:%s", qname, t.UTC().Format("2006-01-02"))
}

// FailedKey returns a redis key for failure count for the given day for the queue.
func FailedKey(qname string, t time.Time) string {
	return fmt.Sprintf("asynq:{%s}:failed:%s", qname, t.UTC().Format("2006-01-02"))
}

// ServerInfoKey returns a redis key for process info.
func ServerInfoKey(hostname string, pid int, serverID string) string {
	return fmt.Sprintf("asynq:servers:{%s:%d:%s}", hostname, pid, serverID)
}

// WorkersKey returns a redis key for the workers given hostname, pid, and server ID.
func WorkersKey(hostname string, pid int, serverID string) string {
	return fmt.Sprintf("asynq:workers:{%s:%d:%s}", hostname, pid, serverID)
}

// SchedulerEntriesKey returns a redis key for the scheduler entries given scheduler ID.
func SchedulerEntriesKey(schedulerID string) string {
	return fmt.Sprintf("asynq:schedulers:{%s}", schedulerID)
}

// SchedulerHistoryKey returns a redis key for the scheduler's history for the given entry.
func SchedulerHistoryKey(entryID string) string {
	return fmt.Sprintf("asynq:scheduler_history:%s", entryID)
}

// UniqueKey returns a redis key with the given type, payload, and queue name.
func UniqueKey(qname, tasktype string, payload map[string]interface{}) string {
	return fmt.Sprintf("asynq:{%s}:unique:%s:%s", qname, tasktype, serializePayload(payload))
}

func serializePayload(payload map[string]interface{}) string {
	if payload == nil {
		return "nil"
	}
	type entry struct {
		k string
		v interface{}
	}
	var es []entry
	for k, v := range payload {
		es = append(es, entry{k, v})
	}
	// sort entries by key
	sort.Slice(es, func(i, j int) bool { return es[i].k < es[j].k })
	var b strings.Builder
	for _, e := range es {
		if b.Len() > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%s=%v", e.k, e.v))
	}
	return b.String()
}

// TaskMessage is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written to redis.
type TaskMessage struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload map[string]interface{}

	// ID is a unique identifier for each task.
	ID uuid.UUID

	// Queue is a name this message should be enqueued to.
	Queue string

	// Retry is the max number of retry for this task.
	Retry int

	// Retried is the number of times we've retried this task so far.
	Retried int

	// ErrorMsg holds the error message from the last failure.
	ErrorMsg string

	// Timeout specifies timeout in seconds.
	// If task processing doesn't complete within the timeout, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the dead queue.
	//
	// Use zero to indicate no timeout.
	Timeout int64

	// Deadline specifies the deadline for the task in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	// If task processing doesn't complete before the deadline, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the dead queue.
	//
	// Use zero to indicate no deadline.
	Deadline int64

	// UniqueKey holds the redis key used for uniqueness lock for this task.
	//
	// Empty string indicates that no uniqueness lock was used.
	UniqueKey string
}

// EncodeMessage marshals the given task message in JSON and returns an encoded string.
func EncodeMessage(msg *TaskMessage) (string, error) {
	b, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// DecodeMessage unmarshals the given encoded string and returns a decoded task message.
func DecodeMessage(s string) (*TaskMessage, error) {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var msg TaskMessage
	if err := d.Decode(&msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// Z represents sorted set member.
type Z struct {
	Message *TaskMessage
	Score   int64
}

// ServerStatus represents status of a server.
// ServerStatus methods are concurrency safe.
type ServerStatus struct {
	mu  sync.Mutex
	val ServerStatusValue
}

// NewServerStatus returns a new status instance given an initial value.
func NewServerStatus(v ServerStatusValue) *ServerStatus {
	return &ServerStatus{val: v}
}

type ServerStatusValue int

const (
	// StatusIdle indicates the server is in idle state.
	StatusIdle ServerStatusValue = iota

	// StatusRunning indicates the server is up and active.
	StatusRunning

	// StatusQuiet indicates the server is up but not active.
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

func (s *ServerStatus) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if StatusIdle <= s.val && s.val <= StatusStopped {
		return statuses[s.val]
	}
	return "unknown status"
}

// Get returns the status value.
func (s *ServerStatus) Get() ServerStatusValue {
	s.mu.Lock()
	v := s.val
	s.mu.Unlock()
	return v
}

// Set sets the status value.
func (s *ServerStatus) Set(v ServerStatusValue) {
	s.mu.Lock()
	s.val = v
	s.mu.Unlock()
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
	ID      string
	Type    string
	Queue   string
	Payload map[string]interface{}
	Started time.Time
}

// SchedulerEntry holds information about a periodic task registered with a scheduler.
type SchedulerEntry struct {
	// Identifier of this entry.
	ID string

	// Spec describes the schedule of this entry.
	Spec string

	// Type is the task type of the periodic task.
	Type string

	// Payload is the payload of the periodic task.
	Payload map[string]interface{}

	// Opts is the options for the periodic task.
	Opts []string

	// Next shows the next time the task will be enqueued.
	Next time.Time

	// Prev shows the last time the task was enqueued.
	// Zero time if task was never enqueued.
	Prev time.Time
}

// SchedulerEnqueueEvent holds information about an enqueue event by a scheduler.
type SchedulerEnqueueEvent struct {
	// ID of the task that was enqueued.
	TaskID string

	// Time the task was enqueued.
	EnqueuedAt time.Time
}

// Cancelations is a collection that holds cancel functions for all active tasks.
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

// Broker is a message broker that supports operations to manage task queues.
//
// See rdb.RDB as a reference implementation.
type Broker interface {
	Ping() error
	Enqueue(msg *TaskMessage) error
	EnqueueUnique(msg *TaskMessage, ttl time.Duration) error
	Dequeue(qnames ...string) (*TaskMessage, time.Time, error)
	Done(msg *TaskMessage) error
	Requeue(msg *TaskMessage) error
	Schedule(msg *TaskMessage, processAt time.Time) error
	ScheduleUnique(msg *TaskMessage, processAt time.Time, ttl time.Duration) error
	Retry(msg *TaskMessage, processAt time.Time, errMsg string) error
	Kill(msg *TaskMessage, errMsg string) error
	CheckAndEnqueue(qnames ...string) error
	ListDeadlineExceeded(deadline time.Time, qnames ...string) ([]*TaskMessage, error)
	WriteServerState(info *ServerInfo, workers []*WorkerInfo, ttl time.Duration) error
	ClearServerState(host string, pid int, serverID string) error
	CancelationPubSub() (*redis.PubSub, error) // TODO: Need to decouple from redis to support other brokers
	PublishCancelation(id string) error
	Close() error
}
