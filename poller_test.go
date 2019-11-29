package asynq

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestPoller(t *testing.T) {
	type scheduledTask struct {
		msg       *taskMessage
		processAt time.Time
	}
	r := setup(t)
	const pollInterval = time.Second
	p := newPoller(r, pollInterval, []string{scheduled, retry})
	t1 := randomTask("gen_thumbnail", "default", nil)
	t2 := randomTask("send_email", "default", nil)
	t3 := randomTask("reindex", "default", nil)
	t4 := randomTask("sync", "default", nil)

	tests := []struct {
		initScheduled []scheduledTask // scheduled queue initial state
		initRetry     []scheduledTask // retry queue initial state
		initQueue     []*taskMessage  // default queue initial state
		wait          time.Duration   // wait duration before checking for final state
		wantScheduled []*taskMessage  // schedule queue final state
		wantRetry     []*taskMessage  // retry queue final state
		wantQueue     []*taskMessage  // default queue final state
	}{
		{
			initScheduled: []scheduledTask{
				{t1, time.Now().Add(time.Hour)},
				{t2, time.Now().Add(-2 * time.Second)},
			},
			initRetry: []scheduledTask{
				{t3, time.Now().Add(-500 * time.Millisecond)},
			},
			initQueue:     []*taskMessage{t4},
			wait:          pollInterval * 2,
			wantScheduled: []*taskMessage{t1},
			wantRetry:     []*taskMessage{},
			wantQueue:     []*taskMessage{t2, t3, t4},
		},
		{
			initScheduled: []scheduledTask{
				{t1, time.Now()},
				{t2, time.Now().Add(-2 * time.Second)},
				{t3, time.Now().Add(-500 * time.Millisecond)},
			},
			initRetry:     []scheduledTask{},
			initQueue:     []*taskMessage{t4},
			wait:          pollInterval * 2,
			wantScheduled: []*taskMessage{},
			wantRetry:     []*taskMessage{},
			wantQueue:     []*taskMessage{t1, t2, t3, t4},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize scheduled queue
		for _, st := range tc.initScheduled {
			err := r.schedule(scheduled, st.processAt, st.msg)
			if err != nil {
				t.Fatal(err)
			}
		}
		// initialize retry queue
		for _, st := range tc.initRetry {
			err := r.schedule(retry, st.processAt, st.msg)
			if err != nil {
				t.Fatal(err)
			}
		}
		// initialize default queue
		for _, msg := range tc.initQueue {
			err := r.enqueue(msg)
			if err != nil {
				t.Fatal(err)
			}
		}

		p.start()
		time.Sleep(tc.wait)
		p.terminate()

		gotScheduledRaw := r.client.ZRange(scheduled, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running poller: (-want, +got)\n%s", scheduled, diff)
		}

		gotRetryRaw := r.client.ZRange(retry, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running poller: (-want, +got)\n%s", retry, diff)
		}

		gotQueueRaw := r.client.LRange(defaultQueue, 0, -1).Val()
		gotQueue := mustUnmarshalSlice(t, gotQueueRaw)
		if diff := cmp.Diff(tc.wantQueue, gotQueue, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running poller: (-want, +got)\n%s", defaultQueue, diff)
		}
	}
}
