package asynq

/*
TODOs:
- [P0] Go docs + License comment
*/

// Task represents a task to be performed.
type Task struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload Payload
}
