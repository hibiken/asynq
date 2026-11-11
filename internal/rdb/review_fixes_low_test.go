// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	h "github.com/hibiken/asynq/internal/testutil"
)

// Regression: a scheduled task that carries a group must be aggregated when it
// becomes due, not silently dropped into the pending list.
func TestScheduledGroupTaskIsAggregatedWhenDue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	m := h.NewTaskMessage("grouped", nil)
	m.GroupKey = "g1"
	if err := r.Schedule(ctx, m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.ForwardIfReady(m.Queue); err != nil {
		t.Fatal(err)
	}
	info, err := r.GetTaskInfo(m.Queue, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != base.TaskStateAggregating {
		t.Errorf("state = %v, want aggregating", info.State)
	}
	groups, err := r.ListGroups(m.Queue)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0] != "g1" {
		t.Errorf("groups = %v, want [g1]", groups)
	}
}

func TestScheduleUniqueGroupTaskIsAggregatedWhenDue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	m := h.NewTaskMessage("grouped-unique", []byte("p"))
	m.GroupKey = "g2"
	m.UniqueKey = base.UniqueKey(m.Queue, m.Type, m.Payload)
	if err := r.ScheduleUnique(ctx, m, time.Now().Add(-time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := r.ForwardIfReady(m.Queue); err != nil {
		t.Fatal(err)
	}
	info, err := r.GetTaskInfo(m.Queue, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != base.TaskStateAggregating {
		t.Errorf("state = %v, want aggregating", info.State)
	}
}

// A task without a group keeps going to pending.
func TestScheduledPlainTaskStillGoesPending(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m := h.NewTaskMessage("plain", nil)
	if err := r.Schedule(context.Background(), m, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := r.ForwardIfReady(m.Queue); err != nil {
		t.Fatal(err)
	}
	info, err := r.GetTaskInfo(m.Queue, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != base.TaskStatePending {
		t.Errorf("state = %v, want pending", info.State)
	}
}

// Regression: another process removing a queue invalidates only its own cache.
// The cache must expire so that this process re-registers the queue.
func TestQueueRegistrationSurvivesRemoteRemoveQueue(t *testing.T) {
	r1 := setup(t)
	defer r1.Close()
	r2 := NewRDB(r1.client)
	ctx := context.Background()

	old := queuePublishTTL
	queuePublishTTL = 50 * time.Millisecond
	defer func() { queuePublishTTL = old }()

	m1 := h.NewTaskMessage("a", nil)
	if err := r1.Enqueue(ctx, m1); err != nil {
		t.Fatal(err)
	}
	// Empty the queue, then remove it through the *other* instance.
	if _, _, err := r1.Dequeue(m1.Queue); err != nil {
		t.Fatal(err)
	}
	if err := r1.Done(ctx, m1); err != nil {
		t.Fatal(err)
	}
	if err := r2.RemoveQueue(m1.Queue, false); err != nil {
		t.Fatalf("RemoveQueue: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	m2 := h.NewTaskMessage("b", nil)
	if err := r1.Enqueue(ctx, m2); err != nil {
		t.Fatal(err)
	}
	ok, err := r1.client.SIsMember(ctx, base.AllQueues, m1.Queue).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("queue is missing from the queue set after enqueue (stale queuesPublished cache)")
	}
}

// Regression: errors.E was called with a format string and arguments, which it
// does not support; the real cause was dropped.
func TestEnqueueUniqueEncodeErrorKeepsCause(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m := h.NewTaskMessage("bad\xff", nil)
	m.UniqueKey = "k"
	err := r.EnqueueUnique(context.Background(), m, time.Minute)
	if err == nil {
		t.Fatal("expected an encode error")
	}
	if !strings.Contains(err.Error(), "cannot encode") || !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("error lost its cause: %v", err)
	}
}

func TestUniqueKeyNilAndEmptyPayloadAreEqual(t *testing.T) {
	a := base.UniqueKey("q", "t", nil)
	b := base.UniqueKey("q", "t", []byte{})
	if a != b {
		t.Errorf("nil payload key %q != empty payload key %q", a, b)
	}
	if base.UniqueKey("q", "t", []byte("x")) == a {
		t.Error("non-empty payload must differ")
	}
}
