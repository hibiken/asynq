// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

/*
TODOs:
- [P0] Go docs
*/

// Task represents a task to be performed.
type Task struct {
	// Type indicates the type of task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload Payload
}

// NewTask returns a new instance of a task given a task type and payload.
//
// Since payload data gets serialized to JSON, the payload values must be
// composed of JSON supported data types.
func NewTask(typename string, payload map[string]interface{}) *Task {
	return &Task{
		Type:    typename,
		Payload: Payload{payload},
	}
}
