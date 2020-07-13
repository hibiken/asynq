// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestRecoverer(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	t1 := h.NewTaskMessage("task1", nil)
	t2 := h.NewTaskMessage("task2", nil)
	t3 := h.NewTaskMessageWithQueue("task3", nil, "critical")
	t4 := h.NewTaskMessage("task4", nil)
	t4.Retried = t4.Retry // t4 has reached its max retry count

	now := time.Now()
	oneHourFromNow := now.Add(1 * time.Hour)
	fiveMinutesFromNow := now.Add(5 * time.Minute)
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	oneHourAgo := now.Add(-1 * time.Hour)

	tests := []struct {
		desc           string
		inProgress     []*base.TaskMessage
		deadlines      []base.Z
		retry          []base.Z
		dead           []base.Z
		wantInProgress []*base.TaskMessage
		wantDeadlines  []base.Z
		wantRetry      []*base.TaskMessage
		wantDead       []*base.TaskMessage
	}{
		{
			desc:       "with one task in-progress",
			inProgress: []*base.TaskMessage{t1},
			deadlines: []base.Z{
				{Message: t1, Score: fiveMinutesAgo.Unix()},
			},
			retry:          []base.Z{},
			dead:           []base.Z{},
			wantInProgress: []*base.TaskMessage{},
			wantDeadlines:  []base.Z{},
			wantRetry: []*base.TaskMessage{
				h.TaskMessageAfterRetry(*t1, "deadline exceeded"),
			},
			wantDead: []*base.TaskMessage{},
		},
		{
			desc:       "with a task with max-retry reached",
			inProgress: []*base.TaskMessage{t4},
			deadlines: []base.Z{
				{Message: t4, Score: fiveMinutesAgo.Unix()},
			},
			retry:          []base.Z{},
			dead:           []base.Z{},
			wantInProgress: []*base.TaskMessage{},
			wantDeadlines:  []base.Z{},
			wantRetry:      []*base.TaskMessage{},
			wantDead:       []*base.TaskMessage{h.TaskMessageWithError(*t4, "deadline exceeded")},
		},
		{
			desc:       "with multiple tasks in-progress, and one expired",
			inProgress: []*base.TaskMessage{t1, t2, t3},
			deadlines: []base.Z{
				{Message: t1, Score: oneHourAgo.Unix()},
				{Message: t2, Score: fiveMinutesFromNow.Unix()},
				{Message: t3, Score: oneHourFromNow.Unix()},
			},
			retry:          []base.Z{},
			dead:           []base.Z{},
			wantInProgress: []*base.TaskMessage{t2, t3},
			wantDeadlines: []base.Z{
				{Message: t2, Score: fiveMinutesFromNow.Unix()},
				{Message: t3, Score: oneHourFromNow.Unix()},
			},
			wantRetry: []*base.TaskMessage{
				h.TaskMessageAfterRetry(*t1, "deadline exceeded"),
			},
			wantDead: []*base.TaskMessage{},
		},
		{
			desc:       "with multiple expired tasks in-progress",
			inProgress: []*base.TaskMessage{t1, t2, t3},
			deadlines: []base.Z{
				{Message: t1, Score: oneHourAgo.Unix()},
				{Message: t2, Score: fiveMinutesAgo.Unix()},
				{Message: t3, Score: oneHourFromNow.Unix()},
			},
			retry:          []base.Z{},
			dead:           []base.Z{},
			wantInProgress: []*base.TaskMessage{t3},
			wantDeadlines: []base.Z{
				{Message: t3, Score: oneHourFromNow.Unix()},
			},
			wantRetry: []*base.TaskMessage{
				h.TaskMessageAfterRetry(*t1, "deadline exceeded"),
				h.TaskMessageAfterRetry(*t2, "deadline exceeded"),
			},
			wantDead: []*base.TaskMessage{},
		},
		{
			desc:           "with empty in-progress queue",
			inProgress:     []*base.TaskMessage{},
			deadlines:      []base.Z{},
			retry:          []base.Z{},
			dead:           []base.Z{},
			wantInProgress: []*base.TaskMessage{},
			wantDeadlines:  []base.Z{},
			wantRetry:      []*base.TaskMessage{},
			wantDead:       []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedInProgressQueue(t, r, tc.inProgress)
		h.SeedDeadlines(t, r, tc.deadlines)
		h.SeedRetryQueue(t, r, tc.retry)
		h.SeedDeadQueue(t, r, tc.dead)

		recoverer := newRecoverer(recovererParams{
			logger:         testLogger,
			broker:         rdbClient,
			interval:       1 * time.Second,
			retryDelayFunc: func(n int, err error, task *Task) time.Duration { return 30 * time.Second },
		})

		var wg sync.WaitGroup
		recoverer.start(&wg)
		time.Sleep(2 * time.Second)
		recoverer.terminate()

		gotInProgress := h.GetInProgressMessages(t, r)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want,+got)\n%s", tc.desc, base.InProgressQueue, diff)
		}
		gotDeadlines := h.GetDeadlinesEntries(t, r)
		if diff := cmp.Diff(tc.wantDeadlines, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want,+got)\n%s", tc.desc, base.KeyDeadlines, diff)
		}
		gotRetry := h.GetRetryMessages(t, r)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q: (-want, +got)\n%s", tc.desc, base.RetryQueue, diff)
		}
		gotDead := h.GetDeadMessages(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q: (-want, +got)\n%s", tc.desc, base.DeadQueue, diff)
		}
	}
}
