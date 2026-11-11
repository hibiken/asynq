// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestNewChainTask(t *testing.T) {
	tests := []struct {
		name  string
		tasks []*Task
		want  func(*Task) bool
	}{
		{
			name:  "empty task list",
			tasks: []*Task{},
			want:  func(t *Task) bool { return t == nil },
		},
		{
			name:  "single task",
			tasks: []*Task{NewTask("task1", []byte("payload1"))},
			want: func(t *Task) bool {
				return t != nil && t.Type() == "task1" && !isChainTask(t.Payload())
			},
		},
		{
			name: "multiple tasks",
			tasks: []*Task{
				NewTask("task1", []byte("payload1")),
				NewTask("task2", []byte("payload2")),
				NewTask("task3", []byte("payload3")),
			},
			want: func(t *Task) bool {
				if t == nil {
					return false
				}
				// Should have chain magic number
				if !isChainTask(t.Payload()) {
					return false
				}
				// Should preserve first task's type
				if t.Type() != "task1" {
					return false
				}
				// Should be able to parse chain data
				data, err := parseChainData(t.Payload())
				if err != nil {
					return false
				}
				// Should have 3 tasks with index 0
				return len(data.Tasks) == 3 && data.Index == 0
			},
		},
		{
			name: "tasks with options",
			tasks: []*Task{
				NewTask("task1", []byte("payload1"), Queue("high"), MaxRetry(5)),
				NewTask("task2", []byte("payload2"), Queue("default")),
			},
			want: func(t *Task) bool {
				if t == nil || !isChainTask(t.Payload()) {
					return false
				}
				data, err := parseChainData(t.Payload())
				if err != nil {
					return false
				}
				// Verify options are preserved
				if len(data.Tasks[0].Options) != 2 {
					return false
				}
				if len(data.Tasks[1].Options) != 1 {
					return false
				}
				return true
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := NewChainTask(tc.tasks...)
			if !tc.want(got) {
				t.Errorf("NewChainTask() failed validation for test case: %s", tc.name)
			}
		})
	}
}

func TestSerializeTask(t *testing.T) {
	tests := []struct {
		name string
		task *Task
		want chainTaskInfo
	}{
		{
			name: "task without options",
			task: NewTask("email:send", []byte(`{"to":"user@example.com"}`)),
			want: chainTaskInfo{
				Type:    "email:send",
				Payload: []byte(`{"to":"user@example.com"}`),
				Options: map[OptionType]interface{}{},
			},
		},
		{
			name: "task with queue option",
			task: NewTask("email:send", []byte(`{"to":"user@example.com"}`), Queue("critical")),
			want: chainTaskInfo{
				Type:    "email:send",
				Payload: []byte(`{"to":"user@example.com"}`),
				Options: map[OptionType]interface{}{
					QueueOpt: "critical",
				},
			},
		},
		{
			name: "task with multiple options",
			task: NewTask("email:send", []byte(`{"to":"user@example.com"}`),
				Queue("critical"),
				MaxRetry(10),
				Timeout(30*time.Second),
			),
			want: chainTaskInfo{
				Type:    "email:send",
				Payload: []byte(`{"to":"user@example.com"}`),
				Options: map[OptionType]interface{}{
					QueueOpt:    "critical",
					MaxRetryOpt: 10,
					TimeoutOpt:  30 * time.Second, // Keep as time.Duration
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := serializeTask(tc.task)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("serializeTask() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReconstructOptions(t *testing.T) {
	tests := []struct {
		name string
		info chainTaskInfo
		want []Option
	}{
		{
			name: "queue option",
			info: chainTaskInfo{
				Type:    "test",
				Payload: []byte("test"),
				Options: map[OptionType]interface{}{
					QueueOpt: "high",
				},
			},
			want: []Option{Queue("high")},
		},
		{
			name: "max retry option",
			info: chainTaskInfo{
				Type:    "test",
				Payload: []byte("test"),
				Options: map[OptionType]interface{}{
					MaxRetryOpt: float64(5),
				},
			},
			want: []Option{MaxRetry(5)},
		},
		{
			name: "timeout option",
			info: chainTaskInfo{
				Type:    "test",
				Payload: []byte("test"),
				Options: map[OptionType]interface{}{
					TimeoutOpt: float64(30 * time.Second),
				},
			},
			want: []Option{Timeout(30 * time.Second)},
		},
		{
			name: "task id option",
			info: chainTaskInfo{
				Type:    "test",
				Payload: []byte("test"),
				Options: map[OptionType]interface{}{
					TaskIDOpt: "task-123",
				},
			},
			want: []Option{TaskID("task-123")},
		},
		{
			name: "group option",
			info: chainTaskInfo{
				Type:    "test",
				Payload: []byte("test"),
				Options: map[OptionType]interface{}{
					GroupOpt: "group-1",
				},
			},
			want: []Option{Group("group-1")},
		},
		{
			name: "multiple options",
			info: chainTaskInfo{
				Type:    "test",
				Payload: []byte("test"),
				Options: map[OptionType]interface{}{
					QueueOpt:    "critical",
					MaxRetryOpt: float64(10),
					TimeoutOpt:  float64(60 * time.Second),
				},
			},
			want: []Option{
				Queue("critical"),
				MaxRetry(10),
				Timeout(60 * time.Second),
			},
		},
		{
			name: "empty options",
			info: chainTaskInfo{
				Type:    "test",
				Payload: []byte("test"),
				Options: map[OptionType]interface{}{},
			},
			want: []Option{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := reconstructOptions(tc.info)
			if len(got) != len(tc.want) {
				t.Errorf("reconstructOptions() returned %d options, want %d", len(got), len(tc.want))
				return
			}
			// Map iteration order is non-deterministic, so we need to match options by type
			gotMap := make(map[OptionType]Option)
			for _, opt := range got {
				gotMap[opt.Type()] = opt
			}
			for _, wantOpt := range tc.want {
				gotOpt, ok := gotMap[wantOpt.Type()]
				if !ok {
					t.Errorf("Expected option type %v not found in result", wantOpt.Type())
					continue
				}
				if diff := cmp.Diff(wantOpt.Value(), gotOpt.Value()); diff != "" {
					t.Errorf("Option %v value mismatch (-want +got):\n%s", wantOpt.Type(), diff)
				}
			}
		})
	}
}

func TestIsChainTask(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    bool
	}{
		{
			name:    "valid chain task",
			payload: []byte(chainMagic + `{"tasks":[],"index":0}`),
			want:    true,
		},
		{
			name:    "regular task",
			payload: []byte(`{"data":"value"}`),
			want:    false,
		},
		{
			name:    "empty payload",
			payload: []byte{},
			want:    false,
		},
		{
			name:    "payload too short",
			payload: []byte("ASY"),
			want:    false,
		},
		{
			name:    "wrong magic number",
			payload: []byte("XXXX" + `{"tasks":[],"index":0}`),
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isChainTask(tc.payload)
			if got != tc.want {
				t.Errorf("isChainTask() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseChainData(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		want    *chainData
		wantErr bool
	}{
		{
			name: "valid chain data",
			payload: func() []byte {
				data := &chainData{
					Tasks: []chainTaskInfo{
						{Type: "task1", Payload: []byte("p1"), Options: map[OptionType]interface{}{}},
						{Type: "task2", Payload: []byte("p2"), Options: map[OptionType]interface{}{}},
					},
					Index: 0,
				}
				payload, _ := marshalChainPayload(data)
				return payload
			}(),
			want: &chainData{
				Tasks: []chainTaskInfo{
					{Type: "task1", Payload: []byte("p1"), Options: map[OptionType]interface{}{}},
					{Type: "task2", Payload: []byte("p2"), Options: map[OptionType]interface{}{}},
				},
				Index: 0,
			},
			wantErr: false,
		},
		{
			name:    "not a chain task",
			payload: []byte(`{"regular":"task"}`),
			wantErr: true,
		},
		{
			name:    "invalid json after magic",
			payload: []byte(chainMagic + `{invalid json`),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseChainData(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseChainData() error = nil, wantErr %v", tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("parseChainData() unexpected error: %v", err)
				return
			}
			// Compare with flexibility for empty Options map (nil vs empty map are both acceptable)
			opts := []cmp.Option{
				cmp.Comparer(func(a, b map[OptionType]interface{}) bool {
					if len(a) == 0 && len(b) == 0 {
						return true // treat nil and empty map as equal
					}
					return cmp.Equal(a, b)
				}),
			}
			if diff := cmp.Diff(tc.want, got, opts...); diff != "" {
				t.Errorf("parseChainData() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMarshalChainPayload(t *testing.T) {
	tests := []struct {
		name    string
		data    *chainData
		wantErr bool
	}{
		{
			name: "valid chain data",
			data: &chainData{
				Tasks: []chainTaskInfo{
					{Type: "task1", Payload: []byte("p1")},
					{Type: "task2", Payload: []byte("p2")},
				},
				Index: 0,
			},
			wantErr: false,
		},
		{
			name: "empty tasks",
			data: &chainData{
				Tasks: []chainTaskInfo{},
				Index: 0,
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := marshalChainPayload(tc.data)
			if tc.wantErr {
				if err == nil {
					t.Errorf("marshalChainPayload() error = nil, wantErr %v", tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("marshalChainPayload() unexpected error: %v", err)
				return
			}
			// Verify magic number
			if len(got) < magicLen || string(got[:magicLen]) != chainMagic {
				t.Errorf("marshalChainPayload() missing or invalid magic number")
				return
			}
			// Verify JSON is valid
			var parsed chainData
			if err := json.Unmarshal(got[magicLen:], &parsed); err != nil {
				t.Errorf("marshalChainPayload() invalid JSON: %v", err)
				return
			}
			// Verify round-trip
			if diff := cmp.Diff(tc.data, &parsed); diff != "" {
				t.Errorf("marshalChainPayload() round-trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestChainMiddleware_NotChainTask(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker: broker,
	}

	var executed bool
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		executed = true
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	// Regular task (not a chain)
	task := NewTask("regular:task", []byte(`{"data":"value"}`))
	taskMsg := &base.TaskMessage{
		Type:    task.Type(),
		Payload: task.Payload(),
	}

	wrappedTask := newTask(taskMsg.Type, taskMsg.Payload, nil)
	err := wrappedHandler.ProcessTask(context.Background(), wrappedTask)

	if err != nil {
		t.Errorf("ProcessTask() unexpected error: %v", err)
	}
	if !executed {
		t.Error("Handler was not executed for regular task")
	}
}

func TestChainMiddleware_InvalidChainData(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker: broker,
	}

	var executed bool
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		executed = true
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	// Invalid chain task (magic number but bad JSON)
	invalidPayload := []byte(chainMagic + `{invalid json`)
	task := newTask("chain:task", invalidPayload, nil)

	err := wrappedHandler.ProcessTask(context.Background(), task)

	if err != nil {
		t.Errorf("ProcessTask() unexpected error: %v", err)
	}
	if !executed {
		t.Error("Handler was not executed (should fall back to normal processing)")
	}
}

func TestChainMiddleware_SingleTaskInChain(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker: broker,
	}

	var executedTasks []string
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		executedTasks = append(executedTasks, task.Type())
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	// Create chain with single task
	chainTask := NewChainTask(
		NewTask("task1", []byte("payload1")),
	)

	task := newTask(chainTask.Type(), chainTask.Payload(), nil)
	err := wrappedHandler.ProcessTask(context.Background(), task)

	if err != nil {
		t.Errorf("ProcessTask() unexpected error: %v", err)
	}
	if len(executedTasks) != 1 || executedTasks[0] != "task1" {
		t.Errorf("Expected task1 to be executed, got: %v", executedTasks)
	}
}

func TestChainMiddleware_MultipleTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker:           broker,
		sharedConnection: true,
	}

	var mu sync.Mutex
	var executedTasks []string
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		mu.Lock()
		executedTasks = append(executedTasks, task.Type())
		mu.Unlock()
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	// Create chain with 3 tasks
	chainTask := NewChainTask(
		NewTask("task1", []byte("payload1")),
		NewTask("task2", []byte("payload2")),
		NewTask("task3", []byte("payload3")),
	)

	// Execute first task in chain
	task := newTask(chainTask.Type(), chainTask.Payload(), nil)
	err := wrappedHandler.ProcessTask(context.Background(), task)

	if err != nil {
		t.Errorf("ProcessTask() unexpected error: %v", err)
	}

	mu.Lock()
	if len(executedTasks) != 1 || executedTasks[0] != "task1" {
		t.Errorf("Expected task1 to be executed, got: %v", executedTasks)
	}
	mu.Unlock()

	// Verify second task was enqueued
	qkey := base.PendingKey("default")
	pending := r.LLen(context.Background(), qkey).Val()
	if pending != 1 {
		t.Errorf("Expected 1 task in queue, got %d", pending)
	}
}

func TestChainMiddleware_FailureStopsChain(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker:           broker,
		sharedConnection: true,
	}

	testErr := errors.New("task failed")
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		// Fail on task2
		if task.Type() == "task2" {
			return testErr
		}
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	// Create chain with 3 tasks
	chainTask := NewChainTask(
		NewTask("task1", []byte("payload1")),
		NewTask("task2", []byte("payload2")),
		NewTask("task3", []byte("payload3")),
	)

	// Execute first task (should succeed and enqueue task2)
	task1 := newTask(chainTask.Type(), chainTask.Payload(), nil)
	err := wrappedHandler.ProcessTask(context.Background(), task1)
	if err != nil {
		t.Errorf("Task1 ProcessTask() unexpected error: %v", err)
	}

	// Get and execute second task (should fail)
	qkey := base.PendingKey("default")
	// Wait a bit for task to be enqueued
	time.Sleep(100 * time.Millisecond)

	// Check if task was enqueued
	pending := r.LLen(context.Background(), qkey).Val()
	if pending == 0 {
		t.Fatal("Expected task in queue but found none")
	}

	// Dequeue the task using the broker
	task2Msg, _, err := broker.Dequeue("default")
	if err != nil {
		t.Fatalf("Failed to dequeue task: %v", err)
	}

	task2 := newTask(task2Msg.Type, task2Msg.Payload, nil)
	err2 := wrappedHandler.ProcessTask(context.Background(), task2)
	if err2 != testErr {
		t.Errorf("Task2 ProcessTask() error = %v, want %v", err2, testErr)
	}

	// Verify task3 was NOT enqueued
	pending = r.LLen(context.Background(), qkey).Val()
	if pending != 0 {
		t.Errorf("Expected 0 tasks in queue (chain should stop), got %d", pending)
	}
}

func TestChainMiddleware_InvalidIndex(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker: broker,
	}

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	// Create chain data with invalid index
	data := &chainData{
		Tasks: []chainTaskInfo{
			{Type: "task1", Payload: []byte("p1")},
		},
		Index: 5, // Invalid: out of bounds
	}
	payload, _ := marshalChainPayload(data)

	task := newTask("task1", payload, nil)
	err := wrappedHandler.ProcessTask(context.Background(), task)

	if err == nil {
		t.Error("Expected error for invalid chain index, got nil")
	}
	if err != nil && err.Error() != "asynq: invalid chain index 5 (total 1)" {
		t.Errorf("Expected invalid index error, got: %v", err)
	}
}

func TestEnqueueNextChainTask(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker:           broker,
		sharedConnection: true,
	}

	data := &chainData{
		Tasks: []chainTaskInfo{
			{
				Type:    "task1",
				Payload: []byte("p1"),
				Options: map[OptionType]interface{}{
					QueueOpt: "default",
				},
			},
			{
				Type:    "task2",
				Payload: []byte("p2"),
				Options: map[OptionType]interface{}{
					QueueOpt:    "high",
					MaxRetryOpt: float64(5),
				},
			},
		},
		Index: 0,
	}

	err := enqueueNextChainTask(context.Background(), client, data)
	if err != nil {
		t.Fatalf("enqueueNextChainTask() unexpected error: %v", err)
	}

	// Verify task was enqueued to the correct queue
	qkey := base.PendingKey("high")
	pending := r.LLen(context.Background(), qkey).Val()
	if pending != 1 {
		t.Errorf("Expected 1 task in 'high' queue, got %d", pending)
	}

	// Verify enqueued task using broker.Dequeue
	taskMsg, _, err := broker.Dequeue("high")
	if err != nil {
		t.Fatalf("Failed to dequeue task: %v", err)
	}

	if taskMsg.Type != "task2" {
		t.Errorf("Expected task type 'task2', got '%s'", taskMsg.Type)
	}
	if taskMsg.Retry != 5 {
		t.Errorf("Expected max retry 5, got %d", taskMsg.Retry)
	}

	// Verify it's still a chain task with updated index
	if !isChainTask(taskMsg.Payload) {
		t.Error("Expected chain task")
	}
	parsedData, err := parseChainData(taskMsg.Payload)
	if err != nil {
		t.Fatalf("Failed to parse chain data: %v", err)
	}
	if parsedData.Index != 1 {
		t.Errorf("Expected index 1, got %d", parsedData.Index)
	}
}

func TestChainIntegration_EndToEnd(t *testing.T) {
	// Skip if short test mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	r := setup(t)
	defer r.Close()

	client := NewClient(getRedisConnOpt(t))
	defer client.Close()

	var mu sync.Mutex
	var executedTasks []string

	mux := NewServeMux()
	mux.HandleFunc("task1", func(ctx context.Context, task *Task) error {
		mu.Lock()
		executedTasks = append(executedTasks, "task1:"+string(task.Payload()))
		mu.Unlock()
		return nil
	})
	mux.HandleFunc("task2", func(ctx context.Context, task *Task) error {
		mu.Lock()
		executedTasks = append(executedTasks, "task2:"+string(task.Payload()))
		mu.Unlock()
		return nil
	})
	mux.HandleFunc("task3", func(ctx context.Context, task *Task) error {
		mu.Lock()
		executedTasks = append(executedTasks, "task3:"+string(task.Payload()))
		mu.Unlock()
		return nil
	})

	srv := NewServer(getRedisConnOpt(t), Config{
		Concurrency: 10,
		LogLevel:    ErrorLevel,
	})

	// Start server
	if err := srv.Start(mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	// Enqueue chain task
	chainTask := NewChainTask(
		NewTask("task1", []byte("payload1")),
		NewTask("task2", []byte("payload2")),
		NewTask("task3", []byte("payload3")),
	)

	_, err := client.Enqueue(chainTask)
	if err != nil {
		t.Fatalf("Failed to enqueue chain task: %v", err)
	}

	// Wait for all tasks to complete (longer wait for reliability)
	time.Sleep(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(executedTasks) != 3 {
		t.Errorf("Expected 3 tasks to be executed, got %d: %v", len(executedTasks), executedTasks)
	}

	expected := []string{"task1:payload1", "task2:payload2", "task3:payload3"}
	if diff := cmp.Diff(expected, executedTasks); diff != "" {
		t.Errorf("Executed tasks mismatch (-want +got):\n%s", diff)
	}
}

func TestChainWithDifferentQueues(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker:           broker,
		sharedConnection: true,
	}

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	// Create chain with tasks in different queues
	chainTask := NewChainTask(
		NewTask("task1", []byte("p1"), Queue("low")),
		NewTask("task2", []byte("p2"), Queue("high")),
		NewTask("task3", []byte("p3"), Queue("critical")),
	)

	// Execute first task
	task := newTask(chainTask.Type(), chainTask.Payload(), nil)
	err := wrappedHandler.ProcessTask(context.Background(), task)
	if err != nil {
		t.Fatalf("ProcessTask() unexpected error: %v", err)
	}

	// Verify task2 was enqueued to 'high' queue
	highQkey := base.PendingKey("high")
	pending := r.LLen(context.Background(), highQkey).Val()
	if pending != 1 {
		t.Errorf("Expected 1 task in 'high' queue, got %d", pending)
	}

	// Execute second task using broker.Dequeue
	task2Msg, _, err := broker.Dequeue("high")
	if err != nil {
		t.Fatalf("Failed to dequeue task2: %v", err)
	}
	task2 := newTask(task2Msg.Type, task2Msg.Payload, nil)
	err = wrappedHandler.ProcessTask(context.Background(), task2)
	if err != nil {
		t.Fatalf("Task2 ProcessTask() unexpected error: %v", err)
	}

	// Verify task3 was enqueued to 'critical' queue
	criticalQkey := base.PendingKey("critical")
	pending = r.LLen(context.Background(), criticalQkey).Val()
	if pending != 1 {
		t.Errorf("Expected 1 task in 'critical' queue, got %d", pending)
	}
}

func TestChainWithLongChain(t *testing.T) {
	r := setup(t)
	defer r.Close()

	// Create a chain with 15 tasks
	tasks := make([]*Task, 15)
	for i := 0; i < 15; i++ {
		tasks[i] = NewTask(fmt.Sprintf("task%d", i+1), []byte(fmt.Sprintf("payload%d", i+1)))
	}

	chainTask := NewChainTask(tasks...)
	if chainTask == nil {
		t.Fatal("NewChainTask() returned nil for long chain")
	}

	// Verify chain data
	data, err := parseChainData(chainTask.Payload())
	if err != nil {
		t.Fatalf("Failed to parse chain data: %v", err)
	}

	if len(data.Tasks) != 15 {
		t.Errorf("Expected 15 tasks in chain, got %d", len(data.Tasks))
	}
	if data.Index != 0 {
		t.Errorf("Expected index 0, got %d", data.Index)
	}
}

func TestChainWithTaskOptions(t *testing.T) {
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker:           broker,
		sharedConnection: true,
	}

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	deadline := time.Now().Add(1 * time.Hour)

	// Create chain with various options
	chainTask := NewChainTask(
		NewTask("task1", []byte("p1"), MaxRetry(3), Timeout(30*time.Second)),
		NewTask("task2", []byte("p2"), MaxRetry(5), Deadline(deadline)),
		NewTask("task3", []byte("p3"), TaskID("custom-id"), Retention(1*time.Hour)),
	)

	// Execute first task
	task := newTask(chainTask.Type(), chainTask.Payload(), nil)
	err := wrappedHandler.ProcessTask(context.Background(), task)
	if err != nil {
		t.Fatalf("ProcessTask() unexpected error: %v", err)
	}

	// Verify task2 was enqueued with correct options using broker.Dequeue
	task2Msg, _, err := broker.Dequeue("default")
	if err != nil {
		t.Fatalf("Failed to dequeue task: %v", err)
	}

	if task2Msg.Retry != 5 {
		t.Errorf("Expected max retry 5, got %d", task2Msg.Retry)
	}

	// Verify the chain data contains the correct options
	if !isChainTask(task2Msg.Payload) {
		t.Fatal("Expected task2 to be a chain task")
	}
	chainData, err := parseChainData(task2Msg.Payload)
	if err != nil {
		t.Fatalf("Failed to parse chain data: %v", err)
	}
	if chainData.Index != 1 {
		t.Errorf("Expected index 1, got %d", chainData.Index)
	}
	if len(chainData.Tasks) < 2 {
		t.Fatalf("Expected at least 2 tasks in chain, got %d", len(chainData.Tasks))
	}

	// Check that task2's options contain the Deadline
	// After JSON marshaling/unmarshaling, time.Time becomes string in JSON
	// and could be represented differently
	task2Options := chainData.Tasks[1].Options
	if deadlineVal, ok := task2Options[DeadlineOpt]; ok {
		// After JSON unmarshaling, it could be string (ISO format) or float64 (Unix timestamp)
		switch v := deadlineVal.(type) {
		case float64:
			if int64(v) != deadline.Unix() {
				t.Errorf("Expected deadline %d in chain data, got %d", deadline.Unix(), int64(v))
			}
		case string:
			// Try parsing as time
			parsedTime, err := time.Parse(time.RFC3339, v)
			if err != nil {
				t.Errorf("Failed to parse deadline string: %v", err)
			} else if parsedTime.Unix() != deadline.Unix() {
				t.Errorf("Expected deadline %d, got %d", deadline.Unix(), parsedTime.Unix())
			}
		default:
			t.Errorf("Deadline option value has unexpected type: %T", deadlineVal)
		}
	} else {
		t.Error("Deadline option not found in chain data")
	}
}

func TestChainWithDeadlineOption(t *testing.T) {
	// This test verifies that Deadline option is correctly preserved through chain execution
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker:           broker,
		sharedConnection: true,
	}

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	deadline := time.Now().Add(1 * time.Hour)

	// Create chain with Deadline option on task2
	chainTask := NewChainTask(
		NewTask("task1", []byte("p1")),
		NewTask("task2", []byte("p2"), Deadline(deadline)),
		NewTask("task3", []byte("p3")),
	)

	// Execute first task
	task := newTask(chainTask.Type(), chainTask.Payload(), nil)
	err := wrappedHandler.ProcessTask(context.Background(), task)
	if err != nil {
		t.Fatalf("ProcessTask() unexpected error: %v", err)
	}

	// Dequeue task2 and verify Deadline was preserved
	task2Msg, _, err := broker.Dequeue("default")
	if err != nil {
		t.Fatalf("Failed to dequeue task2: %v", err)
	}

	// This should pass but will FAIL before the fix
	expectedDeadline := deadline.Unix()
	if task2Msg.Deadline != expectedDeadline {
		t.Errorf("Deadline not preserved! Expected %d (%v), got %d (%v)",
			expectedDeadline, deadline.Format(time.RFC3339),
			task2Msg.Deadline, time.Unix(task2Msg.Deadline, 0).Format(time.RFC3339))
	}
}

func TestChainWithProcessAtOption(t *testing.T) {
	// This test verifies that ProcessAt option is correctly preserved through chain execution
	r := setup(t)
	defer r.Close()

	broker := rdb.NewRDB(r)
	client := &Client{
		broker:           broker,
		sharedConnection: true,
	}

	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		return nil
	})

	middleware := chainMiddleware(client)
	wrappedHandler := middleware(handler)

	processAt := time.Now().Add(30 * time.Minute)

	// Create chain with ProcessAt option on task2
	chainTask := NewChainTask(
		NewTask("task1", []byte("p1")),
		NewTask("task2", []byte("p2"), ProcessAt(processAt)),
	)

	// Execute first task
	task := newTask(chainTask.Type(), chainTask.Payload(), nil)
	err := wrappedHandler.ProcessTask(context.Background(), task)
	if err != nil {
		t.Fatalf("ProcessTask() unexpected error: %v", err)
	}

	// Verify task2 was scheduled with correct ProcessAt time
	// Task should be in scheduled queue, not pending
	qkey := base.ScheduledKey("default")
	count := r.ZCard(context.Background(), qkey).Val()
	if count != 1 {
		t.Fatalf("Expected 1 task in scheduled queue, got %d", count)
	}

	// Get the task from scheduled queue
	results := r.ZRangeWithScores(context.Background(), qkey, 0, 0).Val()
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	// The score in sorted set should be the processAt timestamp
	actualProcessAt := int64(results[0].Score)
	expectedProcessAt := processAt.Unix()

	// Allow 1 second tolerance due to potential timing issues
	if actualProcessAt < expectedProcessAt-1 || actualProcessAt > expectedProcessAt+1 {
		t.Errorf("ProcessAt not preserved! Expected %d (%v), got %d (%v)",
			expectedProcessAt, processAt.Format(time.RFC3339),
			actualProcessAt, time.Unix(actualProcessAt, 0).Format(time.RFC3339))
	}
}

func TestCreateOption(t *testing.T) {
	tests := []struct {
		name     string
		optType  OptionType
		value    interface{}
		wantNil  bool
		validate func(Option) bool
	}{
		{
			name:    "valid queue option",
			optType: QueueOpt,
			value:   "high",
			wantNil: false,
			validate: func(opt Option) bool {
				return opt.Type() == QueueOpt && opt.Value() == "high"
			},
		},
		{
			name:    "invalid queue option type",
			optType: QueueOpt,
			value:   123,
			wantNil: true,
		},
		{
			name:    "valid max retry option",
			optType: MaxRetryOpt,
			value:   float64(10),
			wantNil: false,
			validate: func(opt Option) bool {
				return opt.Type() == MaxRetryOpt && opt.Value() == 10
			},
		},
		{
			name:    "invalid max retry option type",
			optType: MaxRetryOpt,
			value:   "10",
			wantNil: true,
		},
		{
			name:    "valid timeout option",
			optType: TimeoutOpt,
			value:   float64(30 * time.Second),
			wantNil: false,
			validate: func(opt Option) bool {
				return opt.Type() == TimeoutOpt && opt.Value() == 30*time.Second
			},
		},
		{
			name:    "valid task id option",
			optType: TaskIDOpt,
			value:   "task-123",
			wantNil: false,
			validate: func(opt Option) bool {
				return opt.Type() == TaskIDOpt && opt.Value() == "task-123"
			},
		},
		{
			name:    "valid group option",
			optType: GroupOpt,
			value:   "group-1",
			wantNil: false,
			validate: func(opt Option) bool {
				return opt.Type() == GroupOpt && opt.Value() == "group-1"
			},
		},
		{
			name:    "valid unique option",
			optType: UniqueOpt,
			value:   float64(1 * time.Hour),
			wantNil: false,
			validate: func(opt Option) bool {
				return opt.Type() == UniqueOpt
			},
		},
		{
			name:    "valid retention option",
			optType: RetentionOpt,
			value:   float64(2 * time.Hour),
			wantNil: false,
			validate: func(opt Option) bool {
				return opt.Type() == RetentionOpt
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := createOption(tc.optType, tc.value)
			if tc.wantNil {
				if got != nil {
					t.Errorf("createOption() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Error("createOption() = nil, want non-nil")
				return
			}
			if tc.validate != nil && !tc.validate(got) {
				t.Errorf("createOption() validation failed for option: %v", got)
			}
		})
	}
}
