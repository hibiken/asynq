// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
	"github.com/hibiken/asynq/internal/timeutil"
)

// This is the actual mechanism behind issue #1144 and the seven strand
// bugs fixed in internal/rdb: ExtendLease used ZADD's XX option, which
// silently does nothing for a task with no existing lease entry, and the
// caller here never checked for that — it just advanced the worker's
// local lease deadline regardless, so a worker could believe its lease
// was healthy indefinitely while Redis had no matching entry at all.
//
// This test reproduces the precondition directly: a task is active with
// no lease entry (as if something else — the recoverer, most plausibly —
// already reclaimed it) while a worker still has it as "started" and
// still believes its local lease is valid. One heartbeat tick should
// NOT advance that worker's local lease deadline, since Redis has
// nothing to extend.
func TestHeartbeaterDoesNotExtendLeaseForMissingEntry(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	now := time.Now()
	clock := timeutil.NewSimulatedClock(now)
	rdbClient.SetClock(clock)

	msg := h.NewTaskMessageWithQueue("scenario_task", nil, "default")
	origDeadline := now.Add(10 * time.Second)
	lease := h.NewLeaseWithClock(origDeadline, clock)

	// Task is active, but deliberately has no lease entry seeded — the
	// exact "active with no lease" state the other repro tests cover
	// from the rdb side. This test covers where that state actually
	// gets acted on: the heartbeat that's still trying to extend it.
	h.SeedActiveQueue(t, r, []*base.TaskMessage{msg}, "default")

	srvState := &serverState{}
	startingCh := make(chan *workerInfo)
	finishedCh := make(chan *base.TaskMessage)
	hb := newHeartbeater(heartbeaterParams{
		logger:         testLogger,
		broker:         rdbClient,
		interval:       50 * time.Millisecond,
		concurrency:    10,
		queues:         map[string]int{"default": 1},
		strictPriority: false,
		state:          srvState,
		starting:       startingCh,
		finished:       finishedCh,
	})
	hb.clock = clock

	srvState.mu.Lock()
	srvState.value = srvStateActive
	srvState.mu.Unlock()

	var wg sync.WaitGroup
	hb.start(&wg)
	defer hb.shutdown()

	startingCh <- &workerInfo{msg: msg, started: now, deadline: now.Add(2 * time.Minute), lease: lease}

	// Give it a few ticks — this must hold on every tick, not just the
	// first, since the bug was that this kept happening indefinitely.
	time.Sleep(250 * time.Millisecond)

	if got := lease.Deadline(); got != origDeadline {
		t.Errorf("worker's local lease deadline = %v, want unchanged at %v — "+
			"heartbeat should not have been able to extend a lease Redis has no entry for",
			got, origDeadline)
	}

	if leaseEntries := h.GetLeaseEntries(t, r, "default"); len(leaseEntries) != 0 {
		t.Errorf("lease entries in redis = %d, want 0 — nothing should have been created for this task", len(leaseEntries))
	}
}
