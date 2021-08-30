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
	defer r.Close()
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
		desc          string
		inProgress    map[string][]*base.TaskMessage
		deadlines     map[string][]base.Z
		retry         map[string][]base.Z
		archived      map[string][]base.Z
		wantActive    map[string][]*base.TaskMessage
		wantDeadlines map[string][]base.Z
		wantRetry     map[string][]*base.TaskMessage
		wantArchived  map[string][]*base.TaskMessage
	}{
		{
			desc: "with one active task",
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: fiveMinutesAgo.Unix()}},
			},
			retry: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantArchived: map[string][]*base.TaskMessage{
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
			archived: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			wantActive: map[string][]*base.TaskMessage{
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
			wantArchived: map[string][]*base.TaskMessage{
				"default":  {t4},
				"critical": {},
			},
		},
		{
			desc: "with multiple active tasks, and one expired",
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
			archived: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {t2},
				"critical": {t3},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {{Message: t2, Score: fiveMinutesFromNow.Unix()}},
				"critical": {{Message: t3, Score: oneHourFromNow.Unix()}},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
			},
			wantArchived: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
		},
		{
			desc: "with multiple expired active tasks",
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
			archived: map[string][]base.Z{
				"default": {},
				"cricial": {},
			},
			wantActive: map[string][]*base.TaskMessage{
				"default":  {t2},
				"critical": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: oneHourFromNow.Unix()}},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t3},
			},
			wantArchived: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
		},
		{
			desc: "with empty active queue",
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
			archived: map[string][]base.Z{
				"default":  {},
				"critical": {},
			},
			wantActive: map[string][]*base.TaskMessage{
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
			wantArchived: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllActiveQueues(t, r, tc.inProgress)
		h.SeedAllDeadlines(t, r, tc.deadlines)
		h.SeedAllRetryQueues(t, r, tc.retry)
		h.SeedAllArchivedQueues(t, r, tc.archived)

		recoverer := newRecoverer(recovererParams{
			logger:         testLogger,
			broker:         rdbClient,
			queues:         []string{"default", "critical"},
			interval:       1 * time.Second,
			retryDelayFunc: func(n int, err error, task *Task) time.Duration { return 30 * time.Second },
			isFailureFunc:  defaultIsFailureFunc,
		})

		var wg sync.WaitGroup
		recoverer.start(&wg)
		runTime := time.Now() // time when recoverer is running
		time.Sleep(2 * time.Second)
		recoverer.shutdown()

		for qname, want := range tc.wantActive {
			gotActive := h.GetActiveMessages(t, r, qname)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want,+got)\n%s", tc.desc, base.ActiveKey(qname), diff)
			}
		}
		for qname, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want,+got)\n%s", tc.desc, base.DeadlinesKey(qname), diff)
			}
		}
		cmpOpt := h.EquateInt64Approx(2) // allow up to two-second difference in `LastFailedAt`
		for qname, msgs := range tc.wantRetry {
			gotRetry := h.GetRetryMessages(t, r, qname)
			var wantRetry []*base.TaskMessage // Note: construct message here since `LastFailedAt` is relative to each test run
			for _, msg := range msgs {
				wantRetry = append(wantRetry, h.TaskMessageAfterRetry(*msg, "context deadline exceeded", runTime))
			}
			if diff := cmp.Diff(wantRetry, gotRetry, h.SortMsgOpt, cmpOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q: (-want, +got)\n%s", tc.desc, base.RetryKey(qname), diff)
			}
		}
		for qname, msgs := range tc.wantArchived {
			gotArchived := h.GetArchivedMessages(t, r, qname)
			var wantArchived []*base.TaskMessage
			for _, msg := range msgs {
				wantArchived = append(wantArchived, h.TaskMessageWithError(*msg, "context deadline exceeded", runTime))
			}
			if diff := cmp.Diff(wantArchived, gotArchived, h.SortMsgOpt, cmpOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q: (-want, +got)\n%s", tc.desc, base.ArchivedKey(qname), diff)
			}
		}
	}
}
