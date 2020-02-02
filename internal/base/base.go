// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package base defines foundational types and constants used in asynq package.
package base

import (
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
}

// ProcessInfo holds information about running background worker process.
type ProcessInfo struct {
	mu                sync.Mutex
	Concurrency       int
	Queues            map[string]uint
	StrictPriority    bool
	PID               int
	Host              string
	State             string
	Started           time.Time
	ActiveWorkerCount int
}

// NewProcessInfo returns a new instance of ProcessInfo.
func NewProcessInfo(host string, pid, concurrency int, queues map[string]uint, strict bool) *ProcessInfo {
	return &ProcessInfo{
		Host:           host,
		PID:            pid,
		Concurrency:    concurrency,
		Queues:         queues,
		StrictPriority: strict,
	}
}

// SetState set the state field of the process info.
func (p *ProcessInfo) SetState(state string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.State = state
}

// SetStarted set the started field of the process info.
func (p *ProcessInfo) SetStarted(t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Started = t
}

// IncrActiveWorkerCount increments active worker count by delta.
func (p *ProcessInfo) IncrActiveWorkerCount(delta int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ActiveWorkerCount += delta
}
