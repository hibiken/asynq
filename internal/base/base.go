// Package base defines foundational types and constants used in asynq package.
package base

import (
	"time"

	"github.com/rs/xid"
)

// Redis keys
const (
	processedPrefix = "asynq:processed:"      // STRING - asynq:processed:<yyyy-mm-dd>
	failurePrefix   = "asynq:failure:"        // STRING - asynq:failure:<yyyy-mm-dd>
	QueuePrefix     = "asynq:queues:"         // LIST   - asynq:queues:<qname>
	DefaultQueue    = QueuePrefix + "default" // LIST
	ScheduledQueue  = "asynq:scheduled"       // ZSET
	RetryQueue      = "asynq:retry"           // ZSET
	DeadQueue       = "asynq:dead"            // ZSET
	InProgressQueue = "asynq:in_progress"     // LIST
)

// ProcessedKey returns a redis key string for procesed count
// for the given day.
func ProcessedKey(t time.Time) string {
	return processedPrefix + t.UTC().Format("2006-01-02")
}

// FailureKey returns a redis key string for failure count
// for the given day.
func FailureKey(t time.Time) string {
	return failurePrefix + t.UTC().Format("2006-01-02")
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
