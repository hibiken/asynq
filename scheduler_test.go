package asynq

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestScheduler(t *testing.T) {
	type scheduledTask struct {
		msg       *base.TaskMessage
		processAt time.Time
	}
	r := setup(t)
	rdbClient := rdb.NewRDB(r)
	const pollInterval = time.Second
	s := newScheduler(rdbClient, pollInterval)
	t1 := randomTask("gen_thumbnail", "default", nil)
	t2 := randomTask("send_email", "default", nil)
	t3 := randomTask("reindex", "default", nil)
	t4 := randomTask("sync", "default", nil)

	tests := []struct {
		initScheduled []scheduledTask     // scheduled queue initial state
		initRetry     []scheduledTask     // retry queue initial state
		initQueue     []*base.TaskMessage // default queue initial state
		wait          time.Duration       // wait duration before checking for final state
		wantScheduled []*base.TaskMessage // schedule queue final state
		wantRetry     []*base.TaskMessage // retry queue final state
		wantQueue     []*base.TaskMessage // default queue final state
	}{
		{
			initScheduled: []scheduledTask{
				{t1, time.Now().Add(time.Hour)},
				{t2, time.Now().Add(-2 * time.Second)},
			},
			initRetry: []scheduledTask{
				{t3, time.Now().Add(-500 * time.Millisecond)},
			},
			initQueue:     []*base.TaskMessage{t4},
			wait:          pollInterval * 2,
			wantScheduled: []*base.TaskMessage{t1},
			wantRetry:     []*base.TaskMessage{},
			wantQueue:     []*base.TaskMessage{t2, t3, t4},
		},
		{
			initScheduled: []scheduledTask{
				{t1, time.Now()},
				{t2, time.Now().Add(-2 * time.Second)},
				{t3, time.Now().Add(-500 * time.Millisecond)},
			},
			initRetry:     []scheduledTask{},
			initQueue:     []*base.TaskMessage{t4},
			wait:          pollInterval * 2,
			wantScheduled: []*base.TaskMessage{},
			wantRetry:     []*base.TaskMessage{},
			wantQueue:     []*base.TaskMessage{t1, t2, t3, t4},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// initialize scheduled queue
		for _, st := range tc.initScheduled {
			err := rdbClient.Schedule(st.msg, st.processAt)
			if err != nil {
				t.Fatal(err)
			}
		}
		// initialize retry queue
		for _, st := range tc.initRetry {
			err := r.ZAdd(base.RetryQueue, &redis.Z{
				Member: mustMarshal(t, st.msg),
				Score:  float64(st.processAt.Unix()),
			}).Err()
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

		s.start()
		time.Sleep(tc.wait)
		s.terminate()

		gotScheduledRaw := r.ZRange(base.ScheduledQueue, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running scheduler: (-want, +got)\n%s", base.ScheduledQueue, diff)
		}

		gotRetryRaw := r.ZRange(base.RetryQueue, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running scheduler: (-want, +got)\n%s", base.RetryQueue, diff)
		}

		gotQueueRaw := r.LRange(base.DefaultQueue, 0, -1).Val()
		gotQueue := mustUnmarshalSlice(t, gotQueueRaw)
		if diff := cmp.Diff(tc.wantQueue, gotQueue, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running scheduler: (-want, +got)\n%s", base.DefaultQueue, diff)
		}
	}
}
