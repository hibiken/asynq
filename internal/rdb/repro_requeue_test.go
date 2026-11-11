package rdb

import (
	"context"
	"testing"

	"github.com/hibiken/asynq/internal/base"
	h "github.com/hibiken/asynq/internal/testutil"
)

// Requeue is the graceful-shutdown path: processor.go pushes an in-flight
// task back to pending when the server stops before the task finishes.
// Same LREM-then-hard-abort-on-ZREM shape as the other four, so same
// precondition: task ACTIVE, no lease entry.
func TestRequeue_NoLeaseEntry(t *testing.T) {
	r := setup(t)
	defer r.Close()

	msg := h.NewTaskMessage("scenario_task", nil)
	h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, msg.Queue)
	if entries := h.GetLeaseEntries(t, r.client, msg.Queue); len(entries) != 0 {
		t.Fatalf("precondition failed: expected 0 lease entries, got %d", len(entries))
	}

	err := r.Requeue(context.Background(), msg)

	activeAfter := h.GetActiveMessages(t, r.client, msg.Queue)
	pendingAfter := h.GetPendingMessages(t, r.client, msg.Queue)

	t.Logf("error=%v active_after=%d pending_after=%d", err, len(activeAfter), len(pendingAfter))

	if err != nil {
		t.Errorf("got error %v, want nil", err)
	}
	if len(activeAfter) != 0 {
		t.Errorf("task still in active list, want 0")
	}
	if len(pendingAfter) != 1 {
		t.Errorf("task in pending=%d, want 1 — same strand-with-no-destination failure as the others, "+
			"just on the shutdown-requeue path", len(pendingAfter))
	}
}
