package rdb

import (
	"context"
	"testing"

	"github.com/hibiken/asynq/internal/base"
	h "github.com/hibiken/asynq/internal/testutil"
)

// Same precondition as TestOrphanedActiveTask_NoLeaseEntry (see that file):
// a task is ACTIVE with no lease entry. This time via Done(), which is only
// ever called from processor.go on a worker's own successful completion —
// not from the recoverer, so #1158's active-list reconciliation never runs
// ahead of this path at all.
func TestDone_NoLeaseEntry(t *testing.T) {
	for _, tc := range []struct {
		name   string
		unique bool
	}{
		{"plain", false},
		{"unique", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()

			msg := h.NewTaskMessage("scenario_task", nil)
			if tc.unique {
				msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
				// Check the actual result, not just the error — SetNX
				// returns false (no error) if the key already existed,
				// which would otherwise let this test proceed without
				// ever having genuinely acquired the lock it claims to.
				ok, err := r.client.SetNX(context.Background(), msg.UniqueKey, msg.ID, 0).Result()
				if err != nil {
					t.Fatalf("failed to seed uniqueness lock: %v", err)
				}
				if !ok {
					t.Fatalf("precondition failed: uniqueness lock was not actually acquired")
				}
			}

			h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, msg.Queue)
			if entries := h.GetLeaseEntries(t, r.client, msg.Queue); len(entries) != 0 {
				t.Fatalf("precondition failed: expected 0 lease entries, got %d", len(entries))
			}

			err := r.Done(context.Background(), msg)

			activeAfter := h.GetActiveMessages(t, r.client, msg.Queue)
			hashExists := r.client.Exists(context.Background(), base.TaskKey(msg.Queue, msg.ID)).Val() > 0
			processedTotal := r.client.Get(context.Background(), base.ProcessedTotalKey(msg.Queue)).Val()

			t.Logf("unique=%v error=%v active_after=%d hash_still_exists=%v processed_total=%q",
				tc.unique, err, len(activeAfter), hashExists, processedTotal)

			if err != nil {
				t.Errorf("got error %v, want nil", err)
			}
			if len(activeAfter) != 0 {
				t.Errorf("task still in active list, want 0")
			}
			if hashExists {
				t.Errorf("task hash was never deleted — leaked key, invisible to any list-based scan")
			}
			if processedTotal != "1" {
				t.Errorf("processed-total stat = %q, want \"1\" — completion wasn't counted", processedTotal)
			}
			if tc.unique {
				lockExists := r.client.Exists(context.Background(), msg.UniqueKey).Val() > 0
				t.Logf("uniqueness lock still held: %v", lockExists)
				if lockExists {
					t.Errorf("uniqueness lock was never released — blocks re-enqueue of this unique task " +
						"until the lock's own TTL expires, even though it already completed")
				}
			}
		})
	}
}
