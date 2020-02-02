// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
		h.FlushDB(t, r)                        // clean up db before each test case.
		h.SeedEnqueuedQueue(t, r, tc.enqueued) // initialize default queue.

		// instantiate a new processor
		var mu sync.Mutex
		var processed []*Task
		handler := func(task *Task) error {
			mu.Lock()
			defer mu.Unlock()
			processed = append(processed, task)
			return nil
		}
		pi := base.NewProcessInfo("localhost", 1234, 10, defaultQueueConfig, false)
		p := newProcessor(rdbClient, pi, defaultDelayFunc, nil)
		p.handler = HandlerFunc(handler)

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
				{Msg: &r2, Score: float64(now.Add(time.Minute).Unix())},
				{Msg: &r3, Score: float64(now.Add(time.Minute).Unix())},
				{Msg: &r4, Score: float64(now.Add(time.Minute).Unix())},
			},
			wantDead: []*base.TaskMessage{&r1},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)                        // clean up db before each test case.
		h.SeedEnqueuedQueue(t, r, tc.enqueued) // initialize default queue.

		// instantiate a new processor
		delayFunc := func(n int, e error, t *Task) time.Duration {
			return tc.delay
		}
		handler := func(task *Task) error {
			return fmt.Errorf(errMsg)
		}
		pi := base.NewProcessInfo("localhost", 1234, 10, defaultQueueConfig, false)
		p := newProcessor(rdbClient, pi, delayFunc, nil)
		p.handler = HandlerFunc(handler)

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

		cmpOpt := cmpopts.EquateApprox(0, float64(time.Second)) // allow up to second difference in zset score
		gotRetry := h.GetRetryEntries(t, r)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortZSetEntryOpt, cmpOpt); diff != "" {
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

func TestProcessorQueues(t *testing.T) {
	sortOpt := cmp.Transformer("SortStrings", func(in []string) []string {
		out := append([]string(nil), in...) // Copy input to avoid mutating it
		sort.Strings(out)
		return out
	})

	tests := []struct {
		queueCfg map[string]uint
		want     []string
	}{
		{
			queueCfg: map[string]uint{
				"high":    6,
				"default": 3,
				"low":     1,
			},
			want: []string{"high", "default", "low"},
		},
		{
			queueCfg: map[string]uint{
				"default": 1,
			},
			want: []string{"default"},
		},
	}

	for _, tc := range tests {
		pi := base.NewProcessInfo("localhost", 1234, 10, tc.queueCfg, false)
		p := newProcessor(nil, pi, defaultDelayFunc, nil)
		got := p.queues()
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("with queue config: %v\n(*processor).queues() = %v, want %v\n(-want,+got):\n%s",
				tc.queueCfg, got, tc.want, diff)
		}
	}
}

func TestProcessorWithStrictPriority(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("send_email", nil)
	m3 := h.NewTaskMessage("send_email", nil)
	m4 := h.NewTaskMessage("gen_thumbnail", nil)
	m5 := h.NewTaskMessage("gen_thumbnail", nil)
	m6 := h.NewTaskMessage("sync", nil)
	m7 := h.NewTaskMessage("sync", nil)

	t1 := NewTask(m1.Type, m1.Payload)
	t2 := NewTask(m2.Type, m2.Payload)
	t3 := NewTask(m3.Type, m3.Payload)
	t4 := NewTask(m4.Type, m4.Payload)
	t5 := NewTask(m5.Type, m5.Payload)
	t6 := NewTask(m6.Type, m6.Payload)
	t7 := NewTask(m7.Type, m7.Payload)

	tests := []struct {
		enqueued      map[string][]*base.TaskMessage // initial queues state
		wait          time.Duration                  // wait duration between starting and stopping processor for this test case
		wantProcessed []*Task                        // tasks to be processed at the end
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m4, m5},
				"critical":            {m1, m2, m3},
				"low":                 {m6, m7},
			},
			wait:          time.Second,
			wantProcessed: []*Task{t1, t2, t3, t4, t5, t6, t7},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r, msgs, qname)
		}

		// instantiate a new processor
		var mu sync.Mutex
		var processed []*Task
		handler := func(task *Task) error {
			mu.Lock()
			defer mu.Unlock()
			processed = append(processed, task)
			return nil
		}
		queueCfg := map[string]uint{
			"critical":            3,
			base.DefaultQueueName: 2,
			"low":                 1,
		}
		// Note: Set concurrency to 1 to make sure tasks are processed one at a time.
		pi := base.NewProcessInfo("localhost", 1234, 1 /*concurrency */, queueCfg, true /* strict */)
		p := newProcessor(rdbClient, pi, defaultDelayFunc, nil)
		p.handler = HandlerFunc(handler)

		p.start()
		time.Sleep(tc.wait)
		p.terminate()

		if diff := cmp.Diff(tc.wantProcessed, processed, cmp.AllowUnexported(Payload{})); diff != "" {
			t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
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
