// Package base defines foundational types and constants used in asynq package.
package base

import "github.com/rs/xid"

// Redis keys
const (
	QueuePrefix     = "asynq:queues:"         // LIST - asynq:queues:<qname>
	DefaultQueue    = QueuePrefix + "default" // LIST
	ScheduledQueue  = "asynq:scheduled"       // ZSET
	RetryQueue      = "asynq:retry"           // ZSET
	DeadQueue       = "asynq:dead"            // ZSET
	InProgressQueue = "asynq:in_progress"     // LIST
)

// TaskMessage is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written in redis.
type TaskMessage struct {
	//-------- Task fields --------
	// Type represents the kind of task.
	Type string
	// Payload holds data needed to process the task.
	Payload map[string]interface{}

	//-------- Metadata fields --------
	// ID is a unique identifier for each task
	ID xid.ID
	// Queue is a name this message should be enqueued to
	Queue string
	// Retry is the max number of retry for this task.
	Retry int
	// Retried is the number of times we've retried this task so far
	Retried int
	// ErrorMsg holds the error message from the last failure
	ErrorMsg string
}
