package asynq

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestProcessorSuccess(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("gen_thumbnail", "default", nil)
	m3 := randomTask("reindex", "default", nil)
	m4 := randomTask("sync", "default", nil)

	t1 := &Task{Type: m1.Type, Payload: m1.Payload}
	t2 := &Task{Type: m2.Type, Payload: m2.Payload}
	t3 := &Task{Type: m3.Type, Payload: m3.Payload}
	t4 := &Task{Type: m4.Type, Payload: m4.Payload}

	tests := []struct {
		initQueue     []*rdb.TaskMessage // initial default queue state
		incoming      []*rdb.TaskMessage // tasks to be enqueued during run
		wait          time.Duration      // wait duration between starting and stopping processor for this test case
		wantProcessed []*Task            // tasks to be processed at the end
	}{
		{
			initQueue:     []*rdb.TaskMessage{m1},
			incoming:      []*rdb.TaskMessage{m2, m3, m4},
			wait:          time.Second,
			wantProcessed: []*Task{t1, t2, t3, t4},
		},
		{
			initQueue:     []*rdb.TaskMessage{},
			incoming:      []*rdb.TaskMessage{m1},
			wait:          time.Second,
			wantProcessed: []*Task{t1},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// instantiate a new processor
		var mu sync.Mutex
		var processed []*Task
		var h HandlerFunc
		h = func(task *Task) error {
			mu.Lock()
			defer mu.Unlock()
			processed = append(processed, task)
			return nil
		}
		p := newProcessor(rdbClient, 10, h)
		p.dequeueTimeout = time.Second // short time out for test purpose
		// initialize default queue.
		for _, msg := range tc.initQueue {
			err := rdbClient.Enqueue(msg)
			if err != nil {
				t.Fatal(err)
			}
		}

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

		if diff := cmp.Diff(tc.wantProcessed, processed, sortTaskOpt); diff != "" {
			t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
		}

		if l := r.LLen(inProgressQ).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", inProgressQ, l)
		}
	}
}

func TestProcessorRetry(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := randomTask("send_email", "default", nil)
	m1.Retried = m1.Retry // m1 has reached its max retry count
	m2 := randomTask("gen_thumbnail", "default", nil)
	m3 := randomTask("reindex", "default", nil)
	m4 := randomTask("sync", "default", nil)

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

	tests := []struct {
		initQueue []*rdb.TaskMessage // initial default queue state
		incoming  []*rdb.TaskMessage // tasks to be enqueued during run
		wait      time.Duration      // wait duration between starting and stopping processor for this test case
		wantRetry []*rdb.TaskMessage // tasks in retry queue at the end
		wantDead  []*rdb.TaskMessage // tasks in dead queue at the end
	}{
		{
			initQueue: []*rdb.TaskMessage{m1, m2},
			incoming:  []*rdb.TaskMessage{m3, m4},
			wait:      time.Second,
			wantRetry: []*rdb.TaskMessage{&r2, &r3, &r4},
			wantDead:  []*rdb.TaskMessage{&r1},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// instantiate a new processor
		var h HandlerFunc
		h = func(task *Task) error {
			return fmt.Errorf(errMsg)
		}
		p := newProcessor(rdbClient, 10, h)
		p.dequeueTimeout = time.Second // short time out for test purpose
		// initialize default queue.
		for _, msg := range tc.initQueue {
			err := rdbClient.Enqueue(msg)
			if err != nil {
				t.Fatal(err)
			}
		}

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

		gotRetryRaw := r.ZRange(retryQ, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running processor; (-want, +got)\n%s", retryQ, diff)
		}

		gotDeadRaw := r.ZRange(deadQ, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, gotDeadRaw)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running processor; (-want, +got)\n%s", deadQ, diff)
		}

		if l := r.LLen(inProgressQ).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", inProgressQ, l)
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
			task:    &Task{Type: "gen_thumbnail", Payload: map[string]interface{}{"src": "some/img/path"}},
			wantErr: false,
		},
		{
			desc: "handler returns error",
			handler: func(t *Task) error {
				return fmt.Errorf("something went wrong")
			},
			task:    &Task{Type: "gen_thumbnail", Payload: map[string]interface{}{"src": "some/img/path"}},
			wantErr: true,
		},
		{
			desc: "handler panics",
			handler: func(t *Task) error {
				panic("something went terribly wrong")
			},
			task:    &Task{Type: "gen_thumbnail", Payload: map[string]interface{}{"src": "some/img/path"}},
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
