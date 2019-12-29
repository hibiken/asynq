package asynq

/*
TODOs:
- [P0] Pagination for `asynqmon ls` command
- [P0] Show elapsed time for InProgress tasks (asynqmon ls inprogress)
- [P0] Go docs + CONTRIBUTION.md + Github issue template + License comment
- [P1] Add Support for multiple queues and priority
*/

// Task represents a task to be performed.
type Task struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload Payload
}
