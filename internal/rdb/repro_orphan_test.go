package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	h "github.com/hibiken/asynq/internal/testutil"
)

// Reproduction: what happens when Retry/Archive is called on a task that is
// in the ACTIVE list but has NO corresponding lease entry (the exact state
// issue #1144 describes: "tasks marked ACTIVE but lease record missing").
//
// This does not try to prove *how* the lease entry first goes missing in a
// live system (that would need a timing-based fuzz test against the real
// heartbeat/recoverer goroutines). It proves what the code does *once* that
// state exists, which is the part that determines whether the task is
// recoverable or lost.
func TestOrphanedActiveTask_NoLeaseEntry(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(r *RDB, msg *base.TaskMessage) error
	}{
		{
			name: "Retry",
			call: func(r *RDB, msg *base.TaskMessage) error {
				return r.Retry(context.Background(), msg, time.Now().Add(time.Minute), "boom", true)
			},
		},
		{
			name: "Archive",
			call: func(r *RDB, msg *base.TaskMessage) error {
				return r.Archive(context.Background(), msg, "boom")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()

			msg := h.NewTaskMessage("scenario_task", nil)

			// Precondition: task is ACTIVE, but has NO lease entry.
			// (SeedActiveQueue puts it in the active list; we deliberately
			// do not call SeedLease, and confirm the lease set is empty.)
			h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, msg.Queue)

			leaseBefore := h.GetLeaseEntries(t, r.client, msg.Queue)
			if len(leaseBefore) != 0 {
				t.Fatalf("precondition failed: expected 0 lease entries, got %d", len(leaseBefore))
			}
			activeBefore := h.GetActiveMessages(t, r.client, msg.Queue)
			if len(activeBefore) != 1 {
				t.Fatalf("precondition failed: expected 1 active message, got %d", len(activeBefore))
			}

			// Act: call Retry or Archive on the orphaned-but-active task,
			// exactly as the recoverer would.
			err := tc.call(r, msg)

			// Observe: where did the task actually end up?
			activeAfter := h.GetActiveMessages(t, r.client, msg.Queue)
			retryAfter := h.GetRetryMessages(t, r.client, msg.Queue)
			archivedAfter := h.GetArchivedMessages(t, r.client, msg.Queue)
			rawHash := r.client.HGetAll(context.Background(), base.TaskKey(msg.Queue, msg.ID)).Val()

			t.Logf("call=%s error=%v", tc.name, err)
			t.Logf("active list after:   %d entries", len(activeAfter))
			t.Logf("retry list after:    %d entries", len(retryAfter))
			t.Logf("archived list after: %d entries", len(archivedAfter))
			t.Logf("raw task hash still in redis: %v (state=%q)", len(rawHash) > 0, rawHash["state"])

			// Post-fix expectation: the call succeeds, and the task lands
			// in exactly the list it was supposed to (retry or archived),
			// even though it had no lease entry to begin with.
			if err != nil {
				t.Errorf("got error %v, want nil — missing lease entry should no longer abort recovery", err)
			}
			if len(activeAfter) != 0 {
				t.Errorf("task still in active list after %s, want 0", tc.name)
			}
			total := len(retryAfter) + len(archivedAfter)
			if total != 1 {
				t.Errorf("task landed in retry(%d)+archived(%d) = %d lists, want exactly 1 — "+
					"anything else means it's still getting lost", len(retryAfter), len(archivedAfter), total)
			} else {
				t.Logf("PASS: task correctly recovered into %s despite missing lease entry", tc.name)
			}
		})
	}
}
