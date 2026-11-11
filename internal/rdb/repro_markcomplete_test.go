package rdb

import (
	"context"
	"testing"

	"github.com/hibiken/asynq/internal/base"
	h "github.com/hibiken/asynq/internal/testutil"
)

// Same precondition as the other repro_*_test.go files: task ACTIVE, no
// lease entry. MarkAsComplete is the Retention-aware completion path
// (used instead of Done when msg.Retention > 0) — a fourth, separate
// script pair with the identical LREM-then-hard-abort shape, missed in
// the original pass and caught in review, not found independently.
func TestMarkAsComplete_NoLeaseEntry(t *testing.T) {
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
			msg.Retention = 3600
			if tc.unique {
				msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
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

			err := r.MarkAsComplete(context.Background(), msg)

			activeAfter := h.GetActiveMessages(t, r.client, msg.Queue)
			completedScore := r.client.ZScore(context.Background(), base.CompletedKey(msg.Queue), msg.ID)

			t.Logf("unique=%v error=%v active_after=%d in_completed=%v",
				tc.unique, err, len(activeAfter), completedScore.Err() == nil)

			if err != nil {
				t.Errorf("got error %v, want nil", err)
			}
			if len(activeAfter) != 0 {
				t.Errorf("task still in active list, want 0")
			}
			if completedScore.Err() != nil {
				t.Errorf("task not in completed set: %v — same strand-without-a-destination failure as "+
					"Retry/Archive/Done, just via the Retention path", completedScore.Err())
			}

			if tc.unique {
				lockExists := r.client.Exists(context.Background(), msg.UniqueKey).Val() > 0
				if lockExists {
					t.Errorf("uniqueness lock was never released")
				}
			}
		})
	}
}
