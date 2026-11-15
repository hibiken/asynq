// Copyright 2021 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

func newCompletedTask(qname, tasktype string, payload []byte, completedAt time.Time) *base.TaskMessage {
	msg := h.NewTaskMessageWithQueue(tasktype, payload, qname)
	msg.CompletedAt = completedAt.Unix()
	return msg
}

func TestJanitor(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	const interval = 1 * time.Second
	const batchSize = 100
	janitor := newJanitor(janitorParams{
		logger:    testLogger,
		broker:    rdbClient,
		queues:    []string{"default", "custom"},
		interval:  interval,
		batchSize: batchSize,
	})

	now := time.Now()
	hourAgo := now.Add(-1 * time.Hour)
	minuteAgo := now.Add(-1 * time.Minute)
	halfHourAgo := now.Add(-30 * time.Minute)
	halfHourFromNow := now.Add(30 * time.Minute)
	fiveMinFromNow := now.Add(5 * time.Minute)
	msg1 := newCompletedTask("default", "task1", nil, hourAgo)
	msg2 := newCompletedTask("default", "task2", nil, minuteAgo)
	msg3 := newCompletedTask("custom", "task3", nil, hourAgo)
	msg4 := newCompletedTask("custom", "task4", nil, minuteAgo)

	tests := []struct {
		completed     map[string][]base.Z // initial completed sets
		wantCompleted map[string][]base.Z // expected completed sets after janitor runs
	}{
		{
			completed: map[string][]base.Z{
				"default": {
					{Message: msg1, Score: halfHourAgo.Unix()},
					{Message: msg2, Score: fiveMinFromNow.Unix()},
				},
				"custom": {
					{Message: msg3, Score: halfHourFromNow.Unix()},
					{Message: msg4, Score: minuteAgo.Unix()},
				},
			},
			wantCompleted: map[string][]base.Z{
				"default": {
					{Message: msg2, Score: fiveMinFromNow.Unix()},
				},
				"custom": {
					{Message: msg3, Score: halfHourFromNow.Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllCompletedQueues(t, r, tc.completed)

		var wg sync.WaitGroup
		janitor.start(&wg)
		time.Sleep(2 * interval) // make sure to let janitor run at least one time
		janitor.shutdown()

		for qname, want := range tc.wantCompleted {
			got := h.GetCompletedEntries(t, r, qname)
			if diff := cmp.Diff(want, got, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("diff found in %q after running janitor: (-want, +got)\n%s", base.CompletedKey(qname), diff)
			}
		}
	}
}

func TestJanitorTrimsArchive(t *testing.T) {
	r := setup(t)
	defer r.Close()

	const maxArchiveSize = 2
	rdbClient := rdb.NewRDB(r, rdb.RDBConfig{MaxArchiveSize: maxArchiveSize})

	now := time.Now()
	oldest := h.NewTaskMessageWithQueue("old", nil, "default")
	middle := h.NewTaskMessageWithQueue("middle", nil, "default")
	newest := h.NewTaskMessageWithQueue("newest", nil, "default")
	h.SeedAllArchivedQueues(t, r, map[string][]base.Z{
		"default": {
			{Message: oldest, Score: now.Add(-3 * time.Hour).Unix()},
			{Message: middle, Score: now.Add(-2 * time.Hour).Unix()},
			{Message: newest, Score: now.Add(-time.Hour).Unix()},
		},
	})

	janitor := newJanitor(janitorParams{
		logger:    testLogger,
		broker:    rdbClient,
		queues:    []string{"default"},
		interval:  time.Hour,
		batchSize: 1,
	})
	janitor.exec()

	got := h.GetArchivedEntries(t, r, "default")
	if len(got) != maxArchiveSize {
		t.Fatalf("archived set size = %d, want %d", len(got), maxArchiveSize)
	}
	for _, entry := range got {
		if entry.Message.ID == oldest.ID {
			t.Errorf("oldest task %s still in archive", oldest.ID)
		}
	}
	if n := r.Exists(context.Background(), base.TaskKey("default", oldest.ID)).Val(); n != 0 {
		t.Errorf("trimmed task %s still has a task hash", oldest.ID)
	}
}
