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
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload Payload
}
