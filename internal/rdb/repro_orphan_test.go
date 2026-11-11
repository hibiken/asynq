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
			h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, msg.Queue)

			if leaseBefore := h.GetLeaseEntries(t, r.client, msg.Queue); len(leaseBefore) != 0 {
				t.Fatalf("precondition failed: expected 0 lease entries, got %d", len(leaseBefore))
			}
			if activeBefore := h.GetActiveMessages(t, r.client, msg.Queue); len(activeBefore) != 1 {
				t.Fatalf("precondition failed: expected 1 active message, got %d", len(activeBefore))
			}

			err := tc.call(r, msg)

			activeAfter := h.GetActiveMessages(t, r.client, msg.Queue)
			retryAfter := h.GetRetryMessages(t, r.client, msg.Queue)
			archivedAfter := h.GetArchivedMessages(t, r.client, msg.Queue)

			t.Logf("call=%s error=%v active=%d retry=%d archived=%d",
				tc.name, err, len(activeAfter), len(retryAfter), len(archivedAfter))

			if err != nil {
				t.Errorf("got error %v, want nil", err)
			}
			if len(activeAfter) != 0 {
				t.Errorf("task still in active list, want 0")
			}

			// Assert the specific destination, not just "landed somewhere" —
			// Retry must not archive, Archive must not retry.
			var wantRetry, wantArchived int
			if tc.name == "Retry" {
				wantRetry, wantArchived = 1, 0
			} else {
				wantRetry, wantArchived = 0, 1
			}
			if len(retryAfter) != wantRetry || len(archivedAfter) != wantArchived {
				t.Errorf("%s landed in retry=%d archived=%d, want retry=%d archived=%d",
					tc.name, len(retryAfter), len(archivedAfter), wantRetry, wantArchived)
			}
		})
	}
}
