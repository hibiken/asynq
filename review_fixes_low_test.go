// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

// Regression: the cancel func must be registered right after Dequeue, not after
// the (potentially blocking) handshake with the heartbeater; otherwise a cancel
// message arriving in between is silently dropped.
func TestProcessorRegistersCancelationBeforeHeartbeatHandshake(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	msg := h.NewTaskMessage("cancel-me", nil)
	msg.Timeout = 60
	h.SeedPendingQueue(t, r, []*base.TaskMessage{msg}, base.DefaultQueueName)

	p := newProcessorForTest(t, rdbClient, HandlerFunc(func(context.Context, *Task) error { return nil }))
	startingCh := make(chan *workerInfo) // nobody reads: the handshake blocks
	p.starting = startingCh
	go p.exec()

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := p.cancelations.Get(msg.ID); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancelation was not registered while the heartbeat handshake was pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
	<-startingCh // unblock the processor so it can finish
	// wait until the worker released its token, i.e. fully finished.
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
}

// Regression: once stop is requested, a processor that wins the select race on
// the semaphore must not dequeue one more task.
func TestProcessorDoesNotDequeueAfterStop(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	var msgs []*base.TaskMessage
	for i := 0; i < 5; i++ {
		msgs = append(msgs, h.NewTaskMessage(fmt.Sprintf("t%d", i), nil))
	}
	h.SeedPendingQueue(t, r, msgs, base.DefaultQueueName)

	p := newProcessorForTest(t, rdbClient, HandlerFunc(func(context.Context, *Task) error { return nil }))
	p.once.Do(func() { close(p.quit) })
	for i := 0; i < 300; i++ {
		p.exec()
	}
	if got := len(h.GetPendingMessages(t, r, base.DefaultQueueName)); got != len(msgs) {
		t.Errorf("pending = %d, want %d: tasks were dequeued after stop", got, len(msgs))
	}
	if got := len(h.GetActiveMessages(t, r, base.DefaultQueueName)); got != 0 {
		t.Errorf("active = %d, want 0", got)
	}
}

// Regression: tasks handed to handlers always have a usable (non-nil) Headers map.
func TestHandlerTaskHeadersAreNeverNil(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	withHeaders := h.NewTaskMessage("with", nil)
	withHeaders.Headers = map[string]string{"k": "v"}
	without := h.NewTaskMessage("without", nil)
	without.Headers = nil
	h.SeedPendingQueue(t, r, []*base.TaskMessage{withHeaders, without}, base.DefaultQueueName)

	var mu sync.Mutex
	nilFor := map[string]bool{}
	var wg sync.WaitGroup
	wg.Add(2)
	p := newProcessorForTest(t, rdbClient, HandlerFunc(func(_ context.Context, task *Task) error {
		defer wg.Done()
		mu.Lock()
		nilFor[task.Type()] = task.Headers() == nil
		mu.Unlock()
		return nil
	}))
	var pwg sync.WaitGroup
	p.start(&pwg)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tasks were not processed")
	}
	p.shutdown()
	pwg.Wait()

	for typ, isNil := range nilFor {
		if isNil {
			t.Errorf("task %q: Headers() is nil", typ)
		}
	}
}

// Regression: enqueueing must never write into the backing array of the
// options stored in the Task (shared by concurrent Enqueue calls).
func TestEnqueueDoesNotShareTaskOptionsBackingArray(t *testing.T) {
	c := NewClient(getRedisConnOpt(t))
	defer c.Close()

	spare := make([]Option, 1, 8)
	spare[0] = Queue("default")
	task := NewTask("alias", nil, spare...)

	if _, err := c.Enqueue(task, MaxRetry(1)); err != nil {
		t.Fatal(err)
	}
	res := c.BatchEnqueueContext(context.Background(), []*Task{NewTask("alias2", nil, spare...)}, MaxRetry(2))
	if res[0].Err != nil {
		t.Fatal(res[0].Err)
	}
	if spare[:2][1] != nil {
		t.Errorf("enqueue wrote into the shared options backing array: %v", spare[:2][1])
	}
}

// Regression: BatchEnqueue must honor the Header option and must not expose the
// task's own headers map through the enqueued message.
func TestBatchEnqueueHeaders(t *testing.T) {
	c := NewClient(getRedisConnOpt(t))
	defer c.Close()

	orig := map[string]string{"a": "1"}
	task := NewTaskWithHeaders("hdr", nil, orig)
	res := c.BatchEnqueueContext(context.Background(), []*Task{task}, Header("b", "2"))
	if res[0].Err != nil {
		t.Fatal(res[0].Err)
	}
	got := res[0].TaskInfo.Headers
	if got["a"] != "1" || got["b"] != "2" {
		t.Errorf("headers = %v, want a=1 and b=2", got)
	}
	got["a"] = "mutated"
	if task.Headers()["a"] != "1" {
		t.Errorf("task headers were mutated through the enqueued message: %v", task.Headers())
	}
	if _, ok := task.Headers()["b"]; ok {
		t.Errorf("Header option leaked into the task: %v", task.Headers())
	}
}

func TestComposeOptionsTaskID(t *testing.T) {
	o, err := composeOptions()
	if err != nil || o.taskID == "" {
		t.Fatalf("default task ID: %q, %v", o.taskID, err)
	}
	o2, _ := composeOptions()
	if o.taskID == o2.taskID {
		t.Error("default task IDs must be unique")
	}
	o3, err := composeOptions(TaskID("fixed"))
	if err != nil || o3.taskID != "fixed" {
		t.Errorf("TaskID option: %q, %v", o3.taskID, err)
	}
}
