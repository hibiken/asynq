// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestProcessorSuccess(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("gen_thumbnail", nil)
	m3 := h.NewTaskMessage("reindex", nil)
	m4 := h.NewTaskMessage("sync", nil)

	t1 := NewTask(m1.Type, m1.Payload)
	t2 := NewTask(m2.Type, m2.Payload)
	t3 := NewTask(m3.Type, m3.Payload)
	t4 := NewTask(m4.Type, m4.Payload)

	tests := []struct {
		enqueued      []*base.TaskMessage // initial default queue state
		incoming      []*base.TaskMessage // tasks to be enqueued during run
		wait          time.Duration       // wait duration between starting and stopping processor for this test case
		wantProcessed []*Task             // tasks to be processed at the end
	}{
		{
			enqueued:      []*base.TaskMessage{m1},
			incoming:      []*base.TaskMessage{m2, m3, m4},
			wait:          time.Second,
			wantProcessed: []*Task{t1, t2, t3, t4},
		},
		{
			enqueued:      []*base.TaskMessage{},
			incoming:      []*base.TaskMessage{m1},
			wait:          time.Second,
			wantProcessed: []*Task{t1},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)                       // clean up db before each test case.
		h.SeedDefaultQueue(t, r, tc.enqueued) // initialize default queue.

		// instantiate a new processor
		var mu sync.Mutex
		var processed []*Task
		handler := func(task *Task) error {
			mu.Lock()
			defer mu.Unlock()
			processed = append(processed, task)
			return nil
		}
		p := newProcessor(rdbClient, 10, defaultQueueConfig, defaultDelayFunc)
		p.handler = HandlerFunc(handler)
		p.dequeueTimeout = time.Second // short time out for test purpose

		p.start()
		for _, msg := range tc.incoming {
			err := rdbClient.Enqueue(msg)
			if err != nil {
				p.terminate()
				t.Fatal(err)
			}
		}
		time.Sleep(tc.wait)
		p.terminate()

		if diff := cmp.Diff(tc.wantProcessed, processed, sortTaskOpt, cmp.AllowUnexported(Payload{})); diff != "" {
			t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
		}

		if l := r.LLen(base.InProgressQueue).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", base.InProgressQueue, l)
		}
	}
}

func TestProcessorRetry(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := h.NewTaskMessage("send_email", nil)
	m1.Retried = m1.Retry // m1 has reached its max retry count
	m2 := h.NewTaskMessage("gen_thumbnail", nil)
	m3 := h.NewTaskMessage("reindex", nil)
	m4 := h.NewTaskMessage("sync", nil)

	errMsg := "something went wrong"
	// r* is m* after retry
	r1 := *m1
	r1.ErrorMsg = errMsg
	r2 := *m2
	r2.ErrorMsg = errMsg
	r2.Retried = m2.Retried + 1
	r3 := *m3
	r3.ErrorMsg = errMsg
	r3.Retried = m3.Retried + 1
	r4 := *m4
	r4.ErrorMsg = errMsg
	r4.Retried = m4.Retried + 1

	now := time.Now()

	tests := []struct {
		enqueued  []*base.TaskMessage // initial default queue state
		incoming  []*base.TaskMessage // tasks to be enqueued during run
		delay     time.Duration       // retry delay duration
		wait      time.Duration       // wait duration between starting and stopping processor for this test case
		wantRetry []h.ZSetEntry       // tasks in retry queue at the end
		wantDead  []*base.TaskMessage // tasks in dead queue at the end
	}{
		{
			enqueued: []*base.TaskMessage{m1, m2},
			incoming: []*base.TaskMessage{m3, m4},
			delay:    time.Minute,
			wait:     time.Second,
			wantRetry: []h.ZSetEntry{
				{Msg: &r2, Score: now.Add(time.Minute).Unix()},
				{Msg: &r3, Score: now.Add(time.Minute).Unix()},
				{Msg: &r4, Score: now.Add(time.Minute).Unix()},
			},
			wantDead: []*base.TaskMessage{&r1},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)                       // clean up db before each test case.
		h.SeedDefaultQueue(t, r, tc.enqueued) // initialize default queue.

		// instantiate a new processor
		delayFunc := func(n int, e error, t *Task) time.Duration {
			return tc.delay
		}
		handler := func(task *Task) error {
			return fmt.Errorf(errMsg)
		}
		p := newProcessor(rdbClient, 10, defaultQueueConfig, delayFunc)
		p.handler = HandlerFunc(handler)
		p.dequeueTimeout = time.Second // short time out for test purpose

		p.start()
		for _, msg := range tc.incoming {
			err := rdbClient.Enqueue(msg)
			if err != nil {
				p.terminate()
				t.Fatal(err)
			}
		}
		time.Sleep(tc.wait)
		p.terminate()

		gotRetry := h.GetRetryEntries(t, r)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q after running processor; (-want, +got)\n%s", base.RetryQueue, diff)
		}

		gotDead := h.GetDeadMessages(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running processor; (-want, +got)\n%s", base.DeadQueue, diff)
		}

		if l := r.LLen(base.InProgressQueue).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", base.InProgressQueue, l)
		}
	}
}

func TestPerform(t *testing.T) {
	tests := []struct {
		desc    string
		handler HandlerFunc
		task    *Task
		wantErr bool
	}{
		{
			desc: "handler returns nil",
			handler: func(t *Task) error {
				return nil
			},
			task:    NewTask("gen_thumbnail", map[string]interface{}{"src": "some/img/path"}),
			wantErr: false,
		},
		{
			desc: "handler returns error",
			handler: func(t *Task) error {
				return fmt.Errorf("something went wrong")
			},
			task:    NewTask("gen_thumbnail", map[string]interface{}{"src": "some/img/path"}),
			wantErr: true,
		},
		{
			desc: "handler panics",
			handler: func(t *Task) error {
				panic("something went terribly wrong")
			},
			task:    NewTask("gen_thumbnail", map[string]interface{}{"src": "some/img/path"}),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		got := perform(tc.handler, tc.task)
		if !tc.wantErr && got != nil {
			t.Errorf("%s: perform() = %v, want nil", tc.desc, got)
			continue
		}
		if tc.wantErr && got == nil {
			t.Errorf("%s: perform() = nil, want non-nil error", tc.desc)
			continue
		}
	}
}
