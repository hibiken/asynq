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

	t1 := h.NewTaskMessageWithQueue("task1", nil, "default")
	t2 := h.NewTaskMessageWithQueue("task2", nil, "default")
	t3 := h.NewTaskMessageWithQueue("task3", nil, "critical")
	t4 := h.NewTaskMessageWithQueue("task4", nil, "default")
	t4.Retried = t4.Retry // t4 has reached its max retry count

	now := time.Now()
	oneHourFromNow := now.Add(1 * time.Hour)
	fiveMinutesFromNow := now.Add(5 * time.Minute)
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	oneHourAgo := now.Add(-1 * time.Hour)

	tests := []struct {
		desc           string
		inProgress     map[string][]*base.TaskMessage
		deadlines      map[string][]base.Z
		retry          map[string][]base.Z
		dead           map[string][]base.Z
		wantInProgress map[string][]*base.TaskMessage
		wantDeadlines  map[string][]base.Z
		wantRetry      map[string][]*base.TaskMessage
		wantDead       map[string][]*base.TaskMessage
	}{
		{
			desc: "with one task in-progress",
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: fiveMinutesAgo.Unix()}},
			},
			retry: map[string][]base.Z{
				"default": {},
			},
			dead: map[string][]base.Z{
				"default": {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {h.TaskMessageAfterRetry(*t1, "deadline exceeded")},
			},
			wantDead: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			desc: "with a task with max-retry reached",
			inProgress: map[string][]*base.TaskMessage{
				"default":  {t4},
				"critical": {},
			},
			deadlines: map[string][]base.Z{
				"default":  {{Message: t4, Score: fiveMinutesAgo.Unix()}},
				"critical": {},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			dead: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
			wantDead: map[string][]*base.TaskMessage{
				"default":  {h.TaskMessageWithError(*t4, "deadline exceeded")},
				"critical": {},
			},
		},
		{
			desc: "with multiple tasks in-progress, and one expired",
			inProgress: map[string][]*base.TaskMessage{
				"default":  {t1, t2},
				"critical": {t3},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: oneHourAgo.Unix()},
					{Message: t2, Score: fiveMinutesFromNow.Unix()},
				},
				"critical": {
					{Message: t3, Score: oneHourFromNow.Unix()},
				},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			dead: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {t2},
				"critical": {t3},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {{Message: t2, Score: fiveMinutesFromNow.Unix()}},
				"critical": {{Message: t3, Score: oneHourFromNow.Unix()}},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {h.TaskMessageAfterRetry(*t1, "deadline exceeded")},
				"critical": {},
			},
			wantDead: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
		},
		{
			desc: "with multiple expired tasks in-progress",
			inProgress: map[string][]*base.TaskMessage{
				"default":  {t1, t2},
				"critical": {t3},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: oneHourAgo.Unix()},
					{Message: t2, Score: oneHourFromNow.Unix()},
				},
				"critical": {
					{Message: t3, Score: fiveMinutesAgo.Unix()},
				},
			},
			retry: map[string][]base.Z{
				"default": {},
				"cricial": {},
			},
			dead: map[string][]base.Z{
				"default": {},
				"cricial": {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {t2},
				"critical": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: oneHourFromNow.Unix()}},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {h.TaskMessageAfterRetry(*t1, "deadline exceeded")},
				"critical": {h.TaskMessageAfterRetry(*t3, "deadline exceeded")},
			},
			wantDead: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
		},
		{
			desc: "with empty in-progress queue",
			inProgress: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
			deadlines: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			dead: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
			wantDead: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllInProgressQueues(t, r, tc.inProgress)
		h.SeedAllDeadlines(t, r, tc.deadlines)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllDeadQueues(t, r, tc.dead)

		recoverer := newRecoverer(recovererParams{
			logger:         testLogger,
			broker:         rdbClient,
			queues:         []string{"default", "critical"},
			interval:       1 * time.Second,
			retryDelayFunc: func(n int, err error, task *Task) time.Duration { return 30 * time.Second },
		})

		var wg sync.WaitGroup
		recoverer.start(&wg)
		time.Sleep(2 * time.Second)
		recoverer.terminate()

		for qname, want := range tc.wantInProgress {
			gotInProgress := h.GetInProgressMessages(t, r, qname)
			if diff := cmp.Diff(want, gotInProgress, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want,+got)\n%s", tc.desc, base.InProgressKey(qname), diff)
			}
		}
		for qname, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want,+got)\n%s", tc.desc, base.DeadlinesKey(qname), diff)
			}
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryMessages(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q: (-want, +got)\n%s", tc.desc, base.RetryKey(qname), diff)
			}
		}
		for qname, want := range tc.wantDead {
			gotDead := h.GetDeadMessages(t, r, qname)
			if diff := cmp.Diff(want, gotDead, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q: (-want, +got)\n%s", tc.desc, base.DeadKey(qname), diff)
			}
		}
	}
}
