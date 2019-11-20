package asynq

/*
TODOs:
- [P0] Write tests
- [P1] Add Support for multiple queues
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
	// fields from type Task
	Type    string
	Payload map[string]interface{}

	//------- metadata fields ----------
	// queue name this message should be enqueued to
	Queue string

	// max number of retry for this task.
	Retry int

	// number of times we've retried so far
	Retried int

	// error message from the last failure
	ErrorMsg string
}

// RedisOpt specifies redis options.
type RedisOpt struct {
	Addr     string
	Password string
}
