// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	h "github.com/hibiken/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

type recordingProber struct {
	mu  sync.Mutex
	got []map[string]any
	ch  chan struct{}
}

func newRecordingProber() *recordingProber {
	return &recordingProber{ch: make(chan struct{}, 64)}
}

func (p *recordingProber) Changed(out map[string]any) {
	p.mu.Lock()
	p.got = append(p.got, out)
	p.mu.Unlock()
	p.ch <- struct{}{}
}

func (p *recordingProber) Result(state base.TaskState, raw *base.TaskInfo) (string, any) {
	return "task", raw.Message.ID
}

func (p *recordingProber) wait(t *testing.T, n int) []map[string]any {
	t.Helper()
	for i := range n {
		select {
		case <-p.ch:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for prober event %d/%d", i+1, n)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]map[string]any(nil), p.got...)
}

// Regression: SetTaskProber must return immediately and keep delivering events.
func TestSetTaskProberDoesNotBlock(t *testing.T) {
	r := setup(t)
	defer r.Close()

	p := newRecordingProber()
	returned := make(chan struct{})
	go func() {
		r.SetTaskProber(p)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("SetTaskProber blocked the caller")
	}

	msg := h.NewTaskMessage("send_email", nil)
	if err := r.Enqueue(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	got := p.wait(t, 1)
	if got[0]["id"] != msg.ID || got[0]["state"] != "pending" {
		t.Errorf("unexpected event: %v", got[0])
	}
}

// Regression: a malformed state-changed payload must not panic the subscriber.
func TestSetTaskProberSurvivesMalformedPayload(t *testing.T) {
	r := setup(t)
	defer r.Close()

	p := newRecordingProber()
	r.SetTaskProber(p)

	ctx := context.Background()
	for _, payload := range []string{"not-json", `{"state":1}`, `{"state":"x"}`} {
		if err := r.client.Publish(ctx, "state-changed", payload).Err(); err != nil {
			t.Fatal(err)
		}
	}
	got := p.wait(t, 3)
	for i, ev := range got {
		if _, ok := ev["err"]; !ok {
			t.Errorf("event %d should carry an err field: %v", i, ev)
		}
	}

	// subscriber must still be alive afterwards.
	msg := h.NewTaskMessage("send_email", nil)
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatal(err)
	}
	p.wait(t, 1)
}

// Regression: BatchEnqueue must publish state events like Enqueue/Schedule do.
func TestBatchEnqueuePublishesStateEvents(t *testing.T) {
	r := setup(t)
	defer r.Close()

	p := newRecordingProber()
	r.SetTaskProber(p)

	m1 := h.NewTaskMessage("a", nil)
	m2 := h.NewTaskMessage("b", nil)
	n, err := r.BatchEnqueue(context.Background(), []base.BatchEnqueueItem{
		{Msg: m1},
		{Msg: m2, ProcessAt: time.Now().Add(time.Hour)},
	})
	if err != nil || n != 2 {
		t.Fatalf("BatchEnqueue = %d, %v; want 2, nil", n, err)
	}
	got := p.wait(t, 2)
	states := map[string]string{}
	for _, ev := range got {
		states[ev["id"].(string)] = ev["state"].(string)
	}
	if states[m1.ID] != "pending" || states[m2.ID] != "scheduled" {
		t.Errorf("unexpected states: %v", states)
	}

	// duplicates are skipped and must not emit events.
	n, err = r.BatchEnqueue(context.Background(), []base.BatchEnqueueItem{{Msg: m1}})
	if err != nil || n != 0 {
		t.Fatalf("duplicate BatchEnqueue = %d, %v; want 0, nil", n, err)
	}
	select {
	case <-p.ch:
		t.Error("event emitted for skipped duplicate")
	case <-time.After(300 * time.Millisecond):
	}
}

// Regression: when a unique-task script fails because the task ID already
// exists, it must release the uniqueness lock it just acquired.
func TestUniqueLockReleasedOnTaskIDConflict(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	existing := h.NewTaskMessage("existing", nil)
	if err := r.Enqueue(ctx, existing); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		desc string
		call func(msg *base.TaskMessage) error
	}{
		{"EnqueueUnique", func(m *base.TaskMessage) error { return r.EnqueueUnique(ctx, m, time.Minute) }},
		{"ScheduleUnique", func(m *base.TaskMessage) error {
			return r.ScheduleUnique(ctx, m, time.Now().Add(time.Hour), time.Minute)
		}},
		{"AddToGroupUnique", func(m *base.TaskMessage) error { return r.AddToGroupUnique(ctx, m, "g", time.Minute) }},
	}
	for i, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			uniqueKey := base.UniqueKey(base.DefaultQueueName, "conflict", []byte{byte(i)})
			dup := h.NewTaskMessage("conflict", []byte{byte(i)})
			dup.ID = existing.ID // collides with the already stored task
			dup.UniqueKey = uniqueKey

			err := tc.call(dup)
			if !errors.Is(err, errors.ErrTaskIdConflict) {
				t.Fatalf("got %v, want ErrTaskIdConflict", err)
			}
			if n := r.client.Exists(ctx, uniqueKey).Val(); n != 0 {
				t.Errorf("unique lock %q leaked after task-id conflict", uniqueKey)
			}

			// the same unique task with a fresh ID must now be accepted.
			ok := h.NewTaskMessage("conflict", []byte{byte(i)})
			ok.UniqueKey = uniqueKey
			if err := tc.call(ok); err != nil {
				t.Errorf("retry with fresh ID failed: %v", err)
			}
		})
	}
}

// Regression: a BatchEnqueue that aborts before reaching Redis must not leave
// queues marked as published in the in-memory cache.
func TestBatchEnqueueEncodeFailureKeepsQueueCacheConsistent(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	good := h.NewTaskMessageWithQueue("good", nil, "q-good")
	bad := h.NewTaskMessageWithQueue("bad", nil, "q-bad")
	bad.Headers = map[string]string{"k": string([]byte{0xff, 0xfe})} // invalid UTF-8: proto string encoding fails

	if _, err := r.BatchEnqueue(ctx, []base.BatchEnqueueItem{{Msg: good}, {Msg: bad}}); err == nil {
		t.Skip("message with invalid UTF-8 header unexpectedly encoded; cannot trigger the failure path")
	}
	if _, found := r.queuesPublished.Load("q-good"); found {
		t.Error("queue cache marks q-good as published although nothing was sent to redis")
	}

	// A later successful enqueue must register the queue in asynq:queues.
	n, err := r.BatchEnqueue(ctx, []base.BatchEnqueueItem{{Msg: good}})
	if err != nil || n != 1 {
		t.Fatalf("BatchEnqueue = %d, %v; want 1, nil", n, err)
	}
	if ok := r.client.SIsMember(ctx, base.AllQueues, "q-good").Val(); !ok {
		t.Error("q-good missing from asynq:queues after successful BatchEnqueue")
	}
}

// seedExpiredLeases creates n active tasks in qname whose leases expired long ago.
func seedExpiredLeases(t *testing.T, r *RDB, qname string, n int) []*base.TaskMessage {
	t.Helper()
	ctx := context.Background()
	msgs := make([]*base.TaskMessage, n)
	pipe := r.client.Pipeline()
	for i := 0; i < n; i++ {
		m := h.NewTaskMessageWithQueue("t", nil, qname)
		msgs[i] = m
		encoded, err := base.EncodeMessage(m)
		if err != nil {
			t.Fatal(err)
		}
		pipe.HSet(ctx, base.TaskKey(qname, m.ID), "msg", encoded, "state", "active")
		pipe.LPush(ctx, base.ActiveKey(qname), m.ID)
		pipe.ZAdd(ctx, base.LeaseKey(qname), redis.Z{Score: float64(time.Now().Add(-time.Hour).Unix()), Member: m.ID})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	return msgs
}

// Regression: ListLeaseExpired reads in bounded batches and must still return
// every expired task, each exactly once, when there are more than one batch.
func TestListLeaseExpiredSpansMultipleBatches(t *testing.T) {
	r := setup(t)
	defer r.Close()

	n := 2*leaseExpiredBatchSize + 37
	want := seedExpiredLeases(t, r, "default", n)
	other := seedExpiredLeases(t, r, "other", 3)

	got, err := r.ListLeaseExpired(time.Now().Add(-30*time.Second), "default", "other")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n+len(other) {
		t.Fatalf("got %d messages, want %d", len(got), n+len(other))
	}
	seen := map[string]int{}
	for _, m := range got {
		seen[m.ID]++
	}
	for _, m := range append(want, other...) {
		if seen[m.ID] != 1 {
			t.Fatalf("task %s returned %d times, want exactly once", m.ID, seen[m.ID])
		}
	}
}

// Regression: one corrupt message must not hide the recoverable ones.
func TestListLeaseExpiredReturnsPartialResults(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	good := seedExpiredLeases(t, r, "default", 3)
	// a lease entry whose stored message cannot be decoded
	r.client.HSet(ctx, base.TaskKey("default", "corrupt"), "msg", "not-a-protobuf-\xff\xff")
	r.client.ZAdd(ctx, base.LeaseKey("default"), redis.Z{Score: float64(time.Now().Add(-time.Hour).Unix()), Member: "corrupt"})
	good = append(good, seedExpiredLeases(t, r, "other", 2)...)

	got, err := r.ListLeaseExpired(time.Now().Add(-30*time.Second), "default", "other")
	if err == nil {
		t.Fatal("expected an error describing the corrupt message")
	}
	if len(got) != len(good) {
		t.Errorf("got %d messages despite the failure, want %d", len(got), len(good))
	}
}

// Regression: state events are now published by the scripts. Each producer must
// emit exactly one event with the right state, and a rejected call none.
func TestProducerScriptsPublishStateEvents(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	sub := r.client.Subscribe(ctx, stateChangedChannel)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	ch := sub.Channel()

	// 其他套件可能同時對同一個 redis 發佈事件，只關注本測試產生的任務 ID。
	mine := map[string]bool{}
	recv := func(wait time.Duration) (map[string]any, bool) {
		deadline := time.After(wait)
		for {
			select {
			case m := <-ch:
				var ev map[string]any
				if err := json.Unmarshal([]byte(m.Payload), &ev); err != nil {
					t.Fatal(err)
				}
				if id, _ := ev["id"].(string); mine[id] {
					return ev, true
				}
			case <-deadline:
				return nil, false
			}
		}
	}
	next := func() map[string]any {
		t.Helper()
		ev, ok := recv(2 * time.Second)
		if !ok {
			t.Fatal("timed out waiting for a state event")
		}
		return ev
	}
	noMore := func() {
		t.Helper()
		if ev, ok := recv(200 * time.Millisecond); ok {
			t.Fatalf("unexpected extra event: %v", ev)
		}
	}
	uniq := func(name string) *base.TaskMessage {
		m := h.NewTaskMessage(name, []byte(name))
		m.UniqueKey = base.UniqueKey(m.Queue, m.Type, m.Payload)
		return m
	}
	soon := time.Now().Add(time.Hour)

	tests := []struct {
		desc  string
		state base.TaskState
		call  func(m *base.TaskMessage) error
		mk    func(string) *base.TaskMessage
	}{
		{"Enqueue", base.TaskStatePending, func(m *base.TaskMessage) error { return r.Enqueue(ctx, m) }, func(n string) *base.TaskMessage { return h.NewTaskMessage(n, nil) }},
		{"EnqueueUnique", base.TaskStatePending, func(m *base.TaskMessage) error { return r.EnqueueUnique(ctx, m, time.Minute) }, uniq},
		{"Schedule", base.TaskStateScheduled, func(m *base.TaskMessage) error { return r.Schedule(ctx, m, soon) }, func(n string) *base.TaskMessage { return h.NewTaskMessage(n, nil) }},
		{"ScheduleUnique", base.TaskStateScheduled, func(m *base.TaskMessage) error { return r.ScheduleUnique(ctx, m, soon, time.Minute) }, uniq},
		{"AddToGroup", base.TaskStateAggregating, func(m *base.TaskMessage) error { return r.AddToGroup(ctx, m, "g") }, func(n string) *base.TaskMessage { return h.NewTaskMessage(n, nil) }},
		{"AddToGroupUnique", base.TaskStateAggregating, func(m *base.TaskMessage) error { return r.AddToGroupUnique(ctx, m, "g", time.Minute) }, uniq},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			m := tc.mk(tc.desc)
			mine[m.ID] = true
			if err := tc.call(m); err != nil {
				t.Fatal(err)
			}
			ev := next()
			if ev["id"] != m.ID || int(ev["state"].(float64)) != int(tc.state) {
				t.Errorf("event = %v, want id=%s state=%v", ev, m.ID, tc.state)
			}
			noMore()

			// the same ID again is rejected and must not produce an event.
			dup := tc.mk(tc.desc + "-dup")
			dup.ID = m.ID
			if err := tc.call(dup); err == nil {
				t.Fatal("duplicate call should have failed")
			}
			noMore()
		})
	}
}

// Regression: finishing a task removes only that task from a long active list
// and leaves the order of the others untouched.
func TestDoneRemovesOnlyTargetFromActiveList(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	msgs := seedExpiredLeases(t, r, "default", 50)
	for _, i := range []int{49, 0, 25} { // head, tail, middle
		if err := r.Done(ctx, msgs[i]); err != nil {
			t.Fatalf("Done(%d): %v", i, err)
		}
	}
	ids := r.client.LRange(ctx, base.ActiveKey("default"), 0, -1).Val()
	if len(ids) != 47 {
		t.Fatalf("active list has %d entries, want 47", len(ids))
	}
	want := make([]string, 0, 47)
	for i := 48; i >= 1; i-- { // LPUSH order: newest first
		if i != 25 {
			want = append(want, msgs[i].ID)
		}
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("order changed at %d: got %s want %s", i, ids[i], want[i])
		}
	}
	// a task that is not in the list is still reported as not found.
	if err := r.Done(ctx, msgs[25]); err == nil {
		t.Error("Done on an already finished task should fail")
	}
}

// seedArchived fills the archive of the default queue with n entries that all
// fall outside the retention window and returns their IDs.
func seedArchived(t *testing.T, r *RDB, qname string, n int, score int64) []string {
	t.Helper()
	ctx := context.Background()
	ids := make([]string, n)
	pipe := r.client.Pipeline()
	for i := range ids {
		m := h.NewTaskMessageWithQueue("old", nil, qname)
		ids[i] = m.ID
		encoded, err := base.EncodeMessage(m)
		if err != nil {
			t.Fatal(err)
		}
		pipe.HSet(ctx, base.TaskKey(qname, m.ID), "msg", encoded, "state", "archived")
		pipe.ZAdd(ctx, base.ArchivedKey(qname), redis.Z{Score: float64(score), Member: m.ID})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	return ids
}

// Regression: trimming a large expired archive set must not hit Lua's
// "too many results to unpack" limit, which would make every Archive fail.
func TestArchiveTrimsLargeExpiredSet(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	old := seedArchived(t, r, "default", 9000, time.Now().AddDate(0, 0, -archivedExpirationInDays-5).Unix())
	msg := h.NewTaskMessage("fresh", nil)
	h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, "default")
	h.SeedLease(t, r.client, []base.Z{{Message: msg, Score: time.Now().Add(time.Minute).Unix()}}, "default")

	if err := r.Archive(ctx, msg, "boom"); err != nil {
		t.Fatalf("Archive failed: %v", err)
	}
	if n := r.client.ZCard(ctx, base.ArchivedKey("default")).Val(); n != 1 {
		t.Errorf("archive set has %d entries after trimming, want 1", n)
	}
	for _, id := range old[:5] {
		if r.client.Exists(ctx, base.TaskKey("default", id)).Val() != 0 {
			t.Errorf("task hash of trimmed entry %s was not deleted", id)
		}
	}
}

// Regression: Inspector bulk-archive operations trim the archive set. The task
// hashes of the trimmed entries must be deleted too, otherwise they stay in
// redis forever (no TTL) with their full payload.
func TestInspectorArchiveTrimDeletesTaskHashes(t *testing.T) {
	expiredScore := time.Now().AddDate(0, 0, -archivedExpirationInDays-5).Unix()

	tests := []struct {
		desc string
		run  func(t *testing.T, r *RDB)
	}{
		{"ArchiveTask", func(t *testing.T, r *RDB) {
			m := h.NewTaskMessage("p", nil)
			h.SeedPendingQueue(t, r.client, []*base.TaskMessage{m}, "default")
			if err := r.ArchiveTask("default", m.ID); err != nil {
				t.Fatal(err)
			}
		}},
		{"ArchiveAllPendingTasks", func(t *testing.T, r *RDB) {
			h.SeedPendingQueue(t, r.client, []*base.TaskMessage{h.NewTaskMessage("p", nil)}, "default")
			if _, err := r.ArchiveAllPendingTasks("default"); err != nil {
				t.Fatal(err)
			}
		}},
		{"ArchiveAllRetryTasks", func(t *testing.T, r *RDB) {
			m := h.NewTaskMessage("r", nil)
			h.SeedRetryQueue(t, r.client, []base.Z{{Message: m, Score: time.Now().Add(time.Hour).Unix()}}, "default")
			if _, err := r.ArchiveAllRetryTasks("default"); err != nil {
				t.Fatal(err)
			}
		}},
		{"ArchiveAllScheduledTasks", func(t *testing.T, r *RDB) {
			m := h.NewTaskMessage("s", nil)
			h.SeedScheduledQueue(t, r.client, []base.Z{{Message: m, Score: time.Now().Add(time.Hour).Unix()}}, "default")
			if _, err := r.ArchiveAllScheduledTasks("default"); err != nil {
				t.Fatal(err)
			}
		}},
		{"ArchiveAllAggregatingTasks", func(t *testing.T, r *RDB) {
			m := h.NewTaskMessage("a", nil)
			m.GroupKey = "g"
			h.SeedGroup(t, r.client, []base.Z{{Message: m, Score: time.Now().Unix()}}, "default", "g")
			if _, err := r.ArchiveAllAggregatingTasks("default", "g"); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			ctx := context.Background()
			// make sure the queue is known to the inspector
			r.client.SAdd(ctx, base.AllQueues, "default")

			old := seedArchived(t, r, "default", 20, expiredScore)
			tc.run(t, r)

			if n := r.client.ZCard(ctx, base.ArchivedKey("default")).Val(); n != 1 {
				t.Fatalf("archive set has %d entries, want 1 (expired entries should be trimmed)", n)
			}
			leaked := 0
			for _, id := range old {
				if r.client.Exists(ctx, base.TaskKey("default", id)).Val() != 0 {
					leaked++
				}
			}
			if leaked != 0 {
				t.Errorf("%d/%d task hashes of trimmed archive entries were leaked", leaked, len(old))
			}
		})
	}
}

// Regression: the size-based trim (not only the age-based one) must delete the
// task hashes too.
func TestInspectorArchiveSizeTrimDeletesTaskHashes(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	r.client.SAdd(ctx, base.AllQueues, "default")

	old := seedArchived(t, r, "default", maxArchiveSize+50, time.Now().Add(-time.Hour).Unix())
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{h.NewTaskMessage("p", nil)}, "default")
	if _, err := r.ArchiveAllPendingTasks("default"); err != nil {
		t.Fatal(err)
	}
	remaining := int(r.client.ZCard(ctx, base.ArchivedKey("default")).Val())
	if remaining >= maxArchiveSize+50 {
		t.Fatalf("archive set was not trimmed: %d entries", remaining)
	}
	pipe := r.client.Pipeline()
	for _, id := range old {
		pipe.Exists(ctx, base.TaskKey("default", id))
	}
	cmds, _ := pipe.Exec(ctx)
	hashes := 0
	for _, c := range cmds {
		hashes += int(c.(*redis.IntCmd).Val())
	}
	// Every archive entry that is still listed keeps its hash; no hash may exist without an entry.
	if hashes > remaining {
		t.Errorf("%d task hashes exist but only %d archive entries remain: %d leaked", hashes, remaining, hashes-remaining)
	}
}
