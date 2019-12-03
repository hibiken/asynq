package asynq

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestProcessorSuccess(t *testing.T) {
	r := setup(t)

	m1 := randomTask("send_email", "default", nil)
	m2 := randomTask("gen_thumbnail", "default", nil)
	m3 := randomTask("reindex", "default", nil)
	m4 := randomTask("sync", "default", nil)

	t1 := &Task{Type: m1.Type, Payload: m1.Payload}
	t2 := &Task{Type: m2.Type, Payload: m2.Payload}
	t3 := &Task{Type: m3.Type, Payload: m3.Payload}
	t4 := &Task{Type: m4.Type, Payload: m4.Payload}

	tests := []struct {
		initQueue     []*taskMessage // initial default queue state
		incoming      []*taskMessage // tasks to be enqueued during run
		wait          time.Duration  // wait duration between starting and stopping processor for this test case
		wantProcessed []*Task        // tasks to be processed at the end
	}{
		{
			initQueue:     []*taskMessage{m1},
			incoming:      []*taskMessage{m2, m3, m4},
			wait:          time.Second,
			wantProcessed: []*Task{t1, t2, t3, t4},
		},
		{
			initQueue:     []*taskMessage{},
			incoming:      []*taskMessage{m1},
			wait:          time.Second,
			wantProcessed: []*Task{t1},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
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
		p := newProcessor(r, 10, h)
		p.dequeueTimeout = time.Second // short time out for test purpose
		// initialize default queue.
		for _, msg := range tc.initQueue {
			err := r.enqueue(msg)
			if err != nil {
				t.Fatal(err)
			}
		}

		p.start()

		for _, msg := range tc.incoming {
			err := r.enqueue(msg)
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

		if l := r.client.LLen(inProgress).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", inProgress, l)
		}
	}
}

func TestProcessorRetry(t *testing.T) {
	r := setup(t)

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
		initQueue []*taskMessage // initial default queue state
		incoming  []*taskMessage // tasks to be enqueued during run
		wait      time.Duration  // wait duration between starting and stopping processor for this test case
		wantRetry []*taskMessage // tasks in retry queue at the end
		wantDead  []*taskMessage // tasks in dead queue at the end
	}{
		{
			initQueue: []*taskMessage{m1, m2},
			incoming:  []*taskMessage{m3, m4},
			wait:      time.Second,
			wantRetry: []*taskMessage{&r2, &r3, &r4},
			wantDead:  []*taskMessage{&r1},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// instantiate a new processor
		var h HandlerFunc
		h = func(task *Task) error {
			return fmt.Errorf(errMsg)
		}
		p := newProcessor(r, 10, h)
		p.dequeueTimeout = time.Second // short time out for test purpose
		// initialize default queue.
		for _, msg := range tc.initQueue {
			err := r.enqueue(msg)
			if err != nil {
				t.Fatal(err)
			}
		}

		p.start()
		for _, msg := range tc.incoming {
			err := r.enqueue(msg)
			if err != nil {
				p.terminate()
				t.Fatal(err)
			}
		}
		time.Sleep(tc.wait)
		p.terminate()

		gotRetryRaw := r.client.ZRange(retry, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running processor; (-want, +got)\n%s", retry, diff)
		}

		gotDeadRaw := r.client.ZRange(dead, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, gotDeadRaw)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running processor; (-want, +got)\n%s", dead, diff)
		}

		if l := r.client.LLen(inProgress).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", inProgress, l)
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
