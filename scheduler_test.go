// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestScheduler(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)
	const pollInterval = time.Second
	s := newScheduler(rdbClient, pollInterval)
	t1 := h.NewTaskMessage("gen_thumbnail", nil)
	t2 := h.NewTaskMessage("send_email", nil)
	t3 := h.NewTaskMessage("reindex", nil)
	t4 := h.NewTaskMessage("sync", nil)
	now := time.Now()

	tests := []struct {
		initScheduled []h.ZSetEntry       // scheduled queue initial state
		initRetry     []h.ZSetEntry       // retry queue initial state
		initQueue     []*base.TaskMessage // default queue initial state
		wait          time.Duration       // wait duration before checking for final state
		wantScheduled []*base.TaskMessage // schedule queue final state
		wantRetry     []*base.TaskMessage // retry queue final state
		wantQueue     []*base.TaskMessage // default queue final state
	}{
		{
			initScheduled: []h.ZSetEntry{
				{Msg: t1, Score: now.Add(time.Hour).Unix()},
				{Msg: t2, Score: now.Add(-2 * time.Second).Unix()},
			},
			initRetry: []h.ZSetEntry{
				{Msg: t3, Score: time.Now().Add(-500 * time.Millisecond).Unix()},
			},
			initQueue:     []*base.TaskMessage{t4},
			wait:          pollInterval * 2,
			wantScheduled: []*base.TaskMessage{t1},
			wantRetry:     []*base.TaskMessage{},
			wantQueue:     []*base.TaskMessage{t2, t3, t4},
		},
		{
			initScheduled: []h.ZSetEntry{
				{Msg: t1, Score: now.Unix()},
				{Msg: t2, Score: now.Add(-2 * time.Second).Unix()},
				{Msg: t3, Score: now.Add(-500 * time.Millisecond).Unix()},
			},
			initRetry:     []h.ZSetEntry{},
			initQueue:     []*base.TaskMessage{t4},
			wait:          pollInterval * 2,
			wantScheduled: []*base.TaskMessage{},
			wantRetry:     []*base.TaskMessage{},
			wantQueue:     []*base.TaskMessage{t1, t2, t3, t4},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)                              // clean up db before each test case.
		h.SeedScheduledQueue(t, r, tc.initScheduled) // initialize scheduled queue
		h.SeedRetryQueue(t, r, tc.initRetry)         // initialize retry queue
		h.SeedDefaultQueue(t, r, tc.initQueue)       // initialize default queue

		s.start()
		time.Sleep(tc.wait)
		s.terminate()

		gotScheduled := h.GetScheduledMessages(t, r)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running scheduler: (-want, +got)\n%s", base.ScheduledQueue, diff)
		}

		gotRetry := h.GetRetryMessages(t, r)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running scheduler: (-want, +got)\n%s", base.RetryQueue, diff)
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r)
		if diff := cmp.Diff(tc.wantQueue, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running scheduler: (-want, +got)\n%s", base.DefaultQueue, diff)
		}
	}
}
