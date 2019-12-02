package asynq

import (
	"time"

	"github.com/google/uuid"
)

/*
TODOs:
- [P0] Go docs + CONTRIBUTION.md
- [P1] Add Support for multiple queues and priority
- [P1] User defined max-retry count
- [P2] Web UI
*/

// Max retry count by default
const defaultMaxRetry = 25

// Task represents a task to be performed.
type Task struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload is an arbitrary data needed for task execution.
	// The value has to be serializable.
	Payload map[string]interface{}
}

// taskMessage is an internal representation of a task with additional metadata fields.
// This data gets written in redis.
type taskMessage struct {
	//-------- Task fields --------

	Type    string
	Payload map[string]interface{}

	//-------- metadata fields --------

	// unique identifier for each task
	ID uuid.UUID

	// queue name this message should be enqueued to
	Queue string

	// max number of retry for this task.
	Retry int

	// number of times we've retried so far
	Retried int

	// error message from the last failure
	ErrorMsg string
}

// RedisConfig specifies redis configurations.
type RedisConfig struct {
	Addr     string
	Password string

	// DB specifies which redis database to select.
	DB int
}

// Stats represents a state of queues at a certain time.
type Stats struct {
	Queued     int
	InProgress int
	Scheduled  int
	Retry      int
	Dead       int
	Timestamp  time.Time
}

// EnqueuedTask is a task in a queue and is ready to be processed.
// This is read only and used for inspection purpose.
type EnqueuedTask struct {
	ID      uuid.UUID
	Type    string
	Payload map[string]interface{}
}

// InProgressTask is a task that's currently being processed.
// This is read only and used for inspection purpose.
type InProgressTask struct {
	ID      uuid.UUID
	Type    string
	Payload map[string]interface{}
}

// ScheduledTask is a task that's scheduled to be processed in the future.
// This is read only and used for inspection purpose.
type ScheduledTask struct {
	ID        uuid.UUID
	Type      string
	Payload   map[string]interface{}
	ProcessAt time.Time
}

// RetryTask is a task that's in retry queue because worker failed to process the task.
// This is read only and used for inspection purpose.
type RetryTask struct {
	ID      uuid.UUID
	Type    string
	Payload map[string]interface{}
	// TODO(hibiken): add LastFailedAt time.Time
	ProcessAt time.Time
	ErrorMsg  string
	Retried   int
	Retry     int
}

// DeadTask is a task in that has exhausted all retries.
// This is read only and used for inspection purpose.
type DeadTask struct {
	ID           uuid.UUID
	Type         string
	Payload      map[string]interface{}
	LastFailedAt time.Time
	ErrorMsg     string
}
