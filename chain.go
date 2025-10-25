// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// chainMagic is the magic number identifier for chain tasks "ASYC" (Asynq Chain)
const (
	chainMagic = "\x41\x53\x59\x43" // ASYC
	magicLen   = 4
)

// chainData represents chain task data (JSON data following the magic number)
type chainData struct {
	Tasks []chainTaskInfo `json:"tasks"` // all tasks
	Index int             `json:"index"` // current task index (0-based)
}

// chainTaskInfo represents task information
type chainTaskInfo struct {
	Type    string                     `json:"type"`
	Payload []byte                     `json:"payload"`
	Options map[OptionType]interface{} `json:"options,omitempty"`
}

// NewChainTask creates a chain task
// The returned *Task can be directly enqueued with client.Enqueue()
func NewChainTask(tasks ...*Task) *Task {
	if len(tasks) == 0 {
		return nil
	}
	if len(tasks) == 1 {
		return tasks[0]
	}

	// Serialize all tasks
	taskInfos := make([]chainTaskInfo, len(tasks))
	for i, task := range tasks {
		taskInfos[i] = serializeTask(task)
	}

	// Create chainData
	data := chainData{
		Tasks: taskInfos,
		Index: 0, // start from the first task
	}

	// Serialize and add magic number
	payload, err := marshalChainPayload(&data)
	if err != nil {
		return tasks[0] // return original task on failure
	}

	// Return wrapped first task (preserving the first task's type and opts)
	return NewTask(tasks[0].Type(), payload, tasks[0].opts...)
}

// serializeTask serializes a Task into chainTaskInfo
func serializeTask(task *Task) chainTaskInfo {
	info := chainTaskInfo{
		Type:    task.Type(),
		Payload: task.Payload(),
		Options: make(map[OptionType]interface{}),
	}

	// Extract Options, using OptionType directly as the key
	for _, opt := range task.opts {
		info.Options[opt.Type()] = opt.Value()
	}

	return info
}

// reconstructOptions reconstructs Options from chainTaskInfo
func reconstructOptions(info chainTaskInfo) []Option {
	var opts []Option

	for optType, value := range info.Options {
		if opt := createOption(optType, value); opt != nil {
			opts = append(opts, opt)
		}
	}

	return opts
}

// createOption creates an Option from OptionType and value
func createOption(optType OptionType, value interface{}) Option {
	switch optType {
	case QueueOpt:
		if v, ok := value.(string); ok {
			return Queue(v)
		}
	case MaxRetryOpt:
		if v, ok := value.(float64); ok { // JSON numbers are float64
			return MaxRetry(int(v))
		}
	case TimeoutOpt:
		if v, ok := value.(float64); ok {
			return Timeout(time.Duration(v))
		}
	case DeadlineOpt:
		if v, ok := value.(float64); ok {
			return Deadline(time.Unix(int64(v), 0))
		}
	case ProcessAtOpt:
		if v, ok := value.(float64); ok {
			return ProcessAt(time.Unix(int64(v), 0))
		}
	case ProcessInOpt:
		if v, ok := value.(float64); ok {
			return ProcessIn(time.Duration(v))
		}
	case TaskIDOpt:
		if v, ok := value.(string); ok {
			return TaskID(v)
		}
	case UniqueOpt:
		if v, ok := value.(float64); ok {
			return Unique(time.Duration(v))
		}
	case RetentionOpt:
		if v, ok := value.(float64); ok {
			return Retention(time.Duration(v))
		}
	case GroupOpt:
		if v, ok := value.(string); ok {
			return Group(v)
		}
	}
	return nil
}

// isChainTask checks if the payload is a chain task (by magic number)
func isChainTask(payload []byte) bool {
	return len(payload) >= magicLen && string(payload[:magicLen]) == chainMagic
}

// parseChainData parses chain data from payload
func parseChainData(payload []byte) (*chainData, error) {
	if !isChainTask(payload) {
		return nil, fmt.Errorf("not a chain task")
	}

	var data chainData
	if err := json.Unmarshal(payload[magicLen:], &data); err != nil {
		return nil, err
	}

	return &data, nil
}

// marshalChainPayload serializes chain data and adds magic number
func marshalChainPayload(data *chainData) ([]byte, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	// Construct: [magic number][JSON data]
	payload := make([]byte, magicLen+len(jsonData))
	copy(payload[:magicLen], chainMagic)
	copy(payload[magicLen:], jsonData)

	return payload, nil
}

// chainMiddleware handles chain tasks (inspired by metricsMiddleware design)
func chainMiddleware(client *Client) MiddlewareFunc {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, task *Task) error {
			// 1. Check if this is a chain task
			if !isChainTask(task.Payload()) {
				// Not a chain task, execute normally
				return next.ProcessTask(ctx, task)
			}

			// 2. Parse chain data
			data, err := parseChainData(task.Payload())
			if err != nil {
				// Parse failed, execute as a normal task
				return next.ProcessTask(ctx, task)
			}

			// 3. Validate and get current task
			if data.Index < 0 || data.Index >= len(data.Tasks) {
				return fmt.Errorf("asynq: invalid chain index %d (total %d)", data.Index, len(data.Tasks))
			}

			currentTaskInfo := data.Tasks[data.Index]

			// 4. Create clean task for execution
			cleanTask := newTask(currentTaskInfo.Type, currentTaskInfo.Payload, task.w)

			// 5. Execute business Handler
			err = next.ProcessTask(ctx, cleanTask)

			// 6. Stop chain on failure
			if err != nil {
				return err
			}

			// 7. On success, enqueue next task (if any)
			if data.Index+1 < len(data.Tasks) {
				if err := enqueueNextChainTask(ctx, client, data); err != nil {
					return err
				}
			}

			return nil
		})
	}
}

// enqueueNextChainTask enqueues the next task in the chain
func enqueueNextChainTask(ctx context.Context, client *Client, data *chainData) error {
	// Create new chainData
	nextData := &chainData{
		Tasks: data.Tasks,
		Index: data.Index + 1,
	}

	// Serialize
	nextPayload, err := marshalChainPayload(nextData)
	if err != nil {
		return fmt.Errorf("asynq: failed to marshal next chain: %w", err)
	}

	// Get next task info
	nextTaskInfo := data.Tasks[data.Index+1]

	// Create task
	nextTask := NewTask(
		nextTaskInfo.Type,
		nextPayload,
		reconstructOptions(nextTaskInfo)...,
	)

	// Enqueue
	if _, err := client.EnqueueContext(ctx, nextTask); err != nil {
		return fmt.Errorf("asynq: failed to enqueue next task: %w", err)
	}

	return nil
}
