package asynq

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestPoller(t *testing.T) {
	type scheduledTask struct {
		msg       *rdb.TaskMessage
		processAt time.Time
	}
	r := setup(t)
	rdbClient := rdb.NewRDB(r)
	const pollInterval = time.Second
	p := newPoller(rdbClient, pollInterval, []string{rdb.Scheduled, rdb.Retry})
	t1 := randomTask("gen_thumbnail", "default", nil)
	t2 := randomTask("send_email", "default", nil)
	t3 := randomTask("reindex", "default", nil)
	t4 := randomTask("sync", "default", nil)

	tests := []struct {
		initScheduled []scheduledTask    // scheduled queue initial state
		initRetry     []scheduledTask    // retry queue initial state
		initQueue     []*rdb.TaskMessage // default queue initial state
		wait          time.Duration      // wait duration before checking for final state
		wantScheduled []*rdb.TaskMessage // schedule queue final state
		wantRetry     []*rdb.TaskMessage // retry queue final state
		wantQueue     []*rdb.TaskMessage // default queue final state
	}{
		{
			initScheduled: []scheduledTask{
				{t1, time.Now().Add(time.Hour)},
				{t2, time.Now().Add(-2 * time.Second)},
			},
			initRetry: []scheduledTask{
				{t3, time.Now().Add(-500 * time.Millisecond)},
			},
			initQueue:     []*rdb.TaskMessage{t4},
			wait:          pollInterval * 2,
			wantScheduled: []*rdb.TaskMessage{t1},
			wantRetry:     []*rdb.TaskMessage{},
			wantQueue:     []*rdb.TaskMessage{t2, t3, t4},
		},
		{
			initScheduled: []scheduledTask{
				{t1, time.Now()},
				{t2, time.Now().Add(-2 * time.Second)},
				{t3, time.Now().Add(-500 * time.Millisecond)},
			},
			initRetry:     []scheduledTask{},
			initQueue:     []*rdb.TaskMessage{t4},
			wait:          pollInterval * 2,
			wantScheduled: []*rdb.TaskMessage{},
			wantRetry:     []*rdb.TaskMessage{},
			wantQueue:     []*rdb.TaskMessage{t1, t2, t3, t4},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize scheduled queue
		for _, st := range tc.initScheduled {
			err := rdbClient.Schedule(rdb.Scheduled, st.processAt, st.msg)
			if err != nil {
				t.Fatal(err)
			}
		}
		// initialize retry queue
		for _, st := range tc.initRetry {
			err := rdbClient.Schedule(rdb.Retry, st.processAt, st.msg)
			if err != nil {
				t.Fatal(err)
			}
		}
		// initialize default queue
		for _, msg := range tc.initQueue {
			err := rdbClient.Enqueue(msg)
			if err != nil {
				t.Fatal(err)
			}
		}

		p.start()
		time.Sleep(tc.wait)
		p.terminate()

		gotScheduledRaw := r.ZRange(rdb.Scheduled, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running poller: (-want, +got)\n%s", rdb.Scheduled, diff)
		}

		gotRetryRaw := r.ZRange(rdb.Retry, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running poller: (-want, +got)\n%s", rdb.Retry, diff)
		}

		gotQueueRaw := r.LRange(rdb.DefaultQueue, 0, -1).Val()
		gotQueue := mustUnmarshalSlice(t, gotQueueRaw)
		if diff := cmp.Diff(tc.wantQueue, gotQueue, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running poller: (-want, +got)\n%s", rdb.DefaultQueue, diff)
		}
	}
}
