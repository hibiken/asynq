// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/rs/xid"
)

func TestCurrentStats(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessage("gen_thumbnail", map[string]interface{}{"src": "some/path/to/img"})
	m4 := h.NewTaskMessage("sync", nil)
	now := time.Now()

	tests := []struct {
		enqueued   []*base.TaskMessage
		inProgress []*base.TaskMessage
		scheduled  []h.ZSetEntry
		retry      []h.ZSetEntry
		dead       []h.ZSetEntry
		processed  int
		failed     int
		want       *Stats
	}{
		{
			enqueued:   []*base.TaskMessage{m1},
			inProgress: []*base.TaskMessage{m2},
			scheduled: []h.ZSetEntry{
				{Msg: m3, Score: now.Add(time.Hour).Unix()},
				{Msg: m4, Score: now.Unix()}},
			retry:     []h.ZSetEntry{},
			dead:      []h.ZSetEntry{},
			processed: 120,
			failed:    2,
			want: &Stats{
				Enqueued:   1,
				InProgress: 1,
				Scheduled:  2,
				Retry:      0,
				Dead:       0,
				Processed:  120,
				Failed:     2,
				Timestamp:  now,
			},
		},
		{
			enqueued:   []*base.TaskMessage{},
			inProgress: []*base.TaskMessage{},
			scheduled: []h.ZSetEntry{
				{Msg: m3, Score: now.Unix()},
				{Msg: m4, Score: now.Unix()}},
			retry: []h.ZSetEntry{
				{Msg: m1, Score: now.Add(time.Minute).Unix()}},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: now.Add(-time.Hour).Unix()}},
			processed: 90,
			failed:    10,
			want: &Stats{
				Enqueued:   0,
				InProgress: 0,
				Scheduled:  2,
				Retry:      1,
				Dead:       1,
				Processed:  90,
				Failed:     10,
				Timestamp:  now,
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedEnqueuedQueue(t, r.client, tc.enqueued)
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		h.SeedScheduledQueue(t, r.client, tc.scheduled)
		h.SeedRetryQueue(t, r.client, tc.retry)
		h.SeedDeadQueue(t, r.client, tc.dead)
		processedKey := base.ProcessedKey(now)
		failedKey := base.FailureKey(now)
		r.client.Set(processedKey, tc.processed, 0)
		r.client.Set(failedKey, tc.failed, 0)

		got, err := r.CurrentStats()
		if err != nil {
			t.Errorf("r.CurrentStats() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}

		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
			t.Errorf("r.CurrentStats() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestCurrentStatsWithoutData(t *testing.T) {
	r := setup(t)

	want := &Stats{
		Enqueued:   0,
		InProgress: 0,
		Scheduled:  0,
		Retry:      0,
		Dead:       0,
		Processed:  0,
		Failed:     0,
		Timestamp:  time.Now(),
	}

	got, err := r.CurrentStats()
	if err != nil {
		t.Fatalf("r.CurrentStats() = %v, %v, want %+v, nil", got, err, want)
	}

	if diff := cmp.Diff(want, got, timeCmpOpt); diff != "" {
		t.Errorf("r.CurrentStats() = %v, %v, want %+v, nil; (-want, +got)\n%s", got, err, want, diff)
	}
}

func TestHistoricalStats(t *testing.T) {
	r := setup(t)
	now := time.Now().UTC()

	tests := []struct {
		n int // number of days
	}{
		{90},
		{7},
		{0},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		// populate last n days data
		for i := 0; i < tc.n; i++ {
			ts := now.Add(-time.Duration(i) * 24 * time.Hour)
			processedKey := base.ProcessedKey(ts)
			failedKey := base.FailureKey(ts)
			r.client.Set(processedKey, (i+1)*1000, 0)
			r.client.Set(failedKey, (i+1)*10, 0)
		}

		got, err := r.HistoricalStats(tc.n)
		if err != nil {
			t.Errorf("RDB.HistoricalStats(%v) returned error: %v", tc.n, err)
			continue
		}

		if len(got) != tc.n {
			t.Errorf("RDB.HistorycalStats(%v) returned %d daily stats, want %d", tc.n, len(got), tc.n)
			continue
		}

		for i := 0; i < tc.n; i++ {
			want := &DailyStats{
				Processed: (i + 1) * 1000,
				Failed:    (i + 1) * 10,
				Time:      now.Add(-time.Duration(i) * 24 * time.Hour),
			}
			if diff := cmp.Diff(want, got[i], timeCmpOpt); diff != "" {
				t.Errorf("RDB.HistoricalStats %d days ago data; got %+v, want %+v; (-want,+got):\n%s", i, got[i], want, diff)
			}
		}
	}

}

func TestRedisInfo(t *testing.T) {
	r := setup(t)

	info, err := r.RedisInfo()
	if err != nil {
		t.Fatalf("RDB.RedisInfo() returned error: %v", err)
	}

	wantKeys := []string{
		"redis_version",
		"uptime_in_days",
		"connected_clients",
		"used_memory_human",
		"used_memory_peak_human",
		"used_memory_peak_perc",
	}

	for _, key := range wantKeys {
		if _, ok := info[key]; !ok {
			t.Errorf("RDB.RedisInfo() = %v is missing entry for %q", info, key)
		}
	}
}

func TestListEnqueued(t *testing.T) {
	r := setup(t)

	m1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := &EnqueuedTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload}
	t2 := &EnqueuedTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload}
	tests := []struct {
		enqueued []*base.TaskMessage
		want     []*EnqueuedTask
	}{
		{
			enqueued: []*base.TaskMessage{m1, m2},
			want:     []*EnqueuedTask{t1, t2},
		},
		{
			enqueued: []*base.TaskMessage{},
			want:     []*EnqueuedTask{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedEnqueuedQueue(t, r.client, tc.enqueued)

		got, err := r.ListEnqueued()
		if err != nil {
			t.Errorf("r.ListEnqueued() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*EnqueuedTask) []*EnqueuedTask {
			out := append([]*EnqueuedTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("r.ListEnqueued() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListInProgress(t *testing.T) {
	r := setup(t)

	m1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := &InProgressTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload}
	t2 := &InProgressTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload}
	tests := []struct {
		inProgress []*base.TaskMessage
		want       []*InProgressTask
	}{
		{
			inProgress: []*base.TaskMessage{m1, m2},
			want:       []*InProgressTask{t1, t2},
		},
		{
			inProgress: []*base.TaskMessage{},
			want:       []*InProgressTask{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedInProgressQueue(t, r.client, tc.inProgress)

		got, err := r.ListInProgress()
		if err != nil {
			t.Errorf("r.ListInProgress() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*InProgressTask) []*InProgressTask {
			out := append([]*InProgressTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("r.ListInProgress() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListScheduled(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := h.NewTaskMessage("reindex", nil)
	p1 := time.Now().Add(30 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	t1 := &ScheduledTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload, ProcessAt: p1, Score: p1.Unix()}
	t2 := &ScheduledTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload, ProcessAt: p2, Score: p2.Unix()}

	tests := []struct {
		scheduled []h.ZSetEntry
		want      []*ScheduledTask
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: p1.Unix()},
				{Msg: m2, Score: p2.Unix()},
			},
			want: []*ScheduledTask{t1, t2},
		},
		{
			scheduled: []h.ZSetEntry{},
			want:      []*ScheduledTask{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedScheduledQueue(t, r.client, tc.scheduled)

		got, err := r.ListScheduled()
		if err != nil {
			t.Errorf("r.ListScheduled() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*ScheduledTask) []*ScheduledTask {
			out := append([]*ScheduledTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListScheduled() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListRetry(t *testing.T) {
	r := setup(t)
	m1 := &base.TaskMessage{
		ID:       xid.New(),
		Type:     "send_email",
		Queue:    "default",
		Payload:  map[string]interface{}{"subject": "hello"},
		ErrorMsg: "email server not responding",
		Retry:    25,
		Retried:  10,
	}
	m2 := &base.TaskMessage{
		ID:       xid.New(),
		Type:     "reindex",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "search engine not responding",
		Retry:    25,
		Retried:  2,
	}
	p1 := time.Now().Add(5 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	t1 := &RetryTask{
		ID:        m1.ID,
		Type:      m1.Type,
		Payload:   m1.Payload,
		ProcessAt: p1,
		ErrorMsg:  m1.ErrorMsg,
		Retried:   m1.Retried,
		Retry:     m1.Retry,
		Score:     p1.Unix(),
	}
	t2 := &RetryTask{
		ID:        m2.ID,
		Type:      m2.Type,
		Payload:   m2.Payload,
		ProcessAt: p2,
		ErrorMsg:  m2.ErrorMsg,
		Retried:   m2.Retried,
		Retry:     m2.Retry,
		Score:     p2.Unix(),
	}

	tests := []struct {
		retry []h.ZSetEntry
		want  []*RetryTask
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: p1.Unix()},
				{Msg: m2, Score: p2.Unix()},
			},
			want: []*RetryTask{t1, t2},
		},
		{
			retry: []h.ZSetEntry{},
			want:  []*RetryTask{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedRetryQueue(t, r.client, tc.retry)

		got, err := r.ListRetry()
		if err != nil {
			t.Errorf("r.ListRetry() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*RetryTask) []*RetryTask {
			out := append([]*RetryTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})

		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListRetry() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListDead(t *testing.T) {
	r := setup(t)
	m1 := &base.TaskMessage{
		ID:       xid.New(),
		Type:     "send_email",
		Queue:    "default",
		Payload:  map[string]interface{}{"subject": "hello"},
		ErrorMsg: "email server not responding",
	}
	m2 := &base.TaskMessage{
		ID:       xid.New(),
		Type:     "reindex",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "search engine not responding",
	}
	f1 := time.Now().Add(-5 * time.Minute)
	f2 := time.Now().Add(-24 * time.Hour)
	t1 := &DeadTask{
		ID:           m1.ID,
		Type:         m1.Type,
		Payload:      m1.Payload,
		LastFailedAt: f1,
		ErrorMsg:     m1.ErrorMsg,
		Score:        f1.Unix(),
	}
	t2 := &DeadTask{
		ID:           m2.ID,
		Type:         m2.Type,
		Payload:      m2.Payload,
		LastFailedAt: f2,
		ErrorMsg:     m2.ErrorMsg,
		Score:        f2.Unix(),
	}

	tests := []struct {
		dead []h.ZSetEntry
		want []*DeadTask
	}{
		{
			dead: []h.ZSetEntry{
				{Msg: m1, Score: f1.Unix()},
				{Msg: m2, Score: f2.Unix()},
			},
			want: []*DeadTask{t1, t2},
		},
		{
			dead: []h.ZSetEntry{},
			want: []*DeadTask{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedDeadQueue(t, r.client, tc.dead)

		got, err := r.ListDead()
		if err != nil {
			t.Errorf("r.ListDead() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*DeadTask) []*DeadTask {
			out := append([]*DeadTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListDead() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

var timeCmpOpt = cmpopts.EquateApproxTime(time.Second)

func TestEnqueueDeadTask(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		dead         []h.ZSetEntry
		score        int64
		id           xid.ID
		want         error // expected return value from calling EnqueueDeadTask
		wantDead     []*base.TaskMessage
		wantEnqueued []*base.TaskMessage
	}{
		{
			dead: []h.ZSetEntry{
				{Msg: t1, Score: s1},
				{Msg: t2, Score: s2},
			},
			score:        s2,
			id:           t2.ID,
			want:         nil,
			wantDead:     []*base.TaskMessage{t1},
			wantEnqueued: []*base.TaskMessage{t2},
		},
		{
			dead: []h.ZSetEntry{
				{Msg: t1, Score: s1},
				{Msg: t2, Score: s2},
			},
			score:        123,
			id:           t2.ID,
			want:         ErrTaskNotFound,
			wantDead:     []*base.TaskMessage{t1, t2},
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedDeadQueue(t, r.client, tc.dead)

		got := r.EnqueueDeadTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueDeadTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DefaultQueue, diff)
		}

		gotDead := h.GetDeadMessages(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.DeadQueue, diff)
		}
	}
}

func TestEnqueueRetryTask(t *testing.T) {
	r := setup(t)

	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()
	tests := []struct {
		retry        []h.ZSetEntry
		score        int64
		id           xid.ID
		want         error // expected return value from calling EnqueueRetryTask
		wantRetry    []*base.TaskMessage
		wantEnqueued []*base.TaskMessage
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: t1, Score: s1},
				{Msg: t2, Score: s2},
			},
			score:        s2,
			id:           t2.ID,
			want:         nil,
			wantRetry:    []*base.TaskMessage{t1},
			wantEnqueued: []*base.TaskMessage{t2},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: t1, Score: s1},
				{Msg: t2, Score: s2},
			},
			score:        123,
			id:           t2.ID,
			want:         ErrTaskNotFound,
			wantRetry:    []*base.TaskMessage{t1, t2},
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)                  // clean up db before each test case
		h.SeedRetryQueue(t, r.client, tc.retry) // initialize retry queue

		got := r.EnqueueRetryTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueRetryTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DefaultQueue, diff)
		}

		gotRetry := h.GetRetryMessages(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}

func TestEnqueueScheduledTask(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		scheduled     []h.ZSetEntry
		score         int64
		id            xid.ID
		want          error // expected return value from calling EnqueueScheduledTask
		wantScheduled []*base.TaskMessage
		wantEnqueued  []*base.TaskMessage
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: s1},
				{Msg: t2, Score: s2},
			},
			score:         s2,
			id:            t2.ID,
			want:          nil,
			wantScheduled: []*base.TaskMessage{t1},
			wantEnqueued:  []*base.TaskMessage{t2},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: s1},
				{Msg: t2, Score: s2},
			},
			score:         123,
			id:            t2.ID,
			want:          ErrTaskNotFound,
			wantScheduled: []*base.TaskMessage{t1, t2},
			wantEnqueued:  []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedScheduledQueue(t, r.client, tc.scheduled)

		got := r.EnqueueScheduledTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueRetryTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DefaultQueue, diff)
		}

		gotScheduled := h.GetScheduledMessages(t, r.client)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.ScheduledQueue, diff)
		}
	}
}

func TestEnqueueAllScheduledTasks(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("reindex", nil)

	tests := []struct {
		desc         string
		scheduled    []h.ZSetEntry
		want         int64
		wantEnqueued []*base.TaskMessage
	}{
		{
			desc: "with tasks in scheduled queue",
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: time.Now().Add(time.Hour).Unix()},
				{Msg: t2, Score: time.Now().Add(time.Hour).Unix()},
				{Msg: t3, Score: time.Now().Add(time.Hour).Unix()},
			},
			want:         3,
			wantEnqueued: []*base.TaskMessage{t1, t2, t3},
		},
		{
			desc:         "with empty scheduled queue",
			scheduled:    []h.ZSetEntry{},
			want:         0,
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedScheduledQueue(t, r.client, tc.scheduled)

		got, err := r.EnqueueAllScheduledTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllScheduledTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllScheduledTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.DefaultQueue, diff)
		}
	}
}

func TestEnqueueAllRetryTasks(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("reindex", nil)

	tests := []struct {
		description  string
		retry        []h.ZSetEntry
		want         int64
		wantEnqueued []*base.TaskMessage
	}{
		{
			description: "with tasks in retry queue",
			retry: []h.ZSetEntry{
				{Msg: t1, Score: time.Now().Add(time.Hour).Unix()},
				{Msg: t2, Score: time.Now().Add(time.Hour).Unix()},
				{Msg: t3, Score: time.Now().Add(time.Hour).Unix()},
			},
			want:         3,
			wantEnqueued: []*base.TaskMessage{t1, t2, t3},
		},
		{
			description:  "with empty retry queue",
			retry:        []h.ZSetEntry{},
			want:         0,
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedRetryQueue(t, r.client, tc.retry)

		got, err := r.EnqueueAllRetryTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllRetryTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllRetryTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.description, base.DefaultQueue, diff)
		}
	}
}

func TestEnqueueAllDeadTasks(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("reindex", nil)

	tests := []struct {
		desc         string
		dead         []h.ZSetEntry
		want         int64
		wantEnqueued []*base.TaskMessage
	}{
		{
			desc: "with tasks in dead queue",
			dead: []h.ZSetEntry{
				{Msg: t1, Score: time.Now().Add(-time.Minute).Unix()},
				{Msg: t2, Score: time.Now().Add(-time.Minute).Unix()},
				{Msg: t3, Score: time.Now().Add(-time.Minute).Unix()},
			},
			want:         3,
			wantEnqueued: []*base.TaskMessage{t1, t2, t3},
		},
		{
			desc:         "with empty dead queue",
			dead:         []h.ZSetEntry{},
			want:         0,
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedDeadQueue(t, r.client, tc.dead)

		got, err := r.EnqueueAllDeadTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllDeadTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllDeadTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
		}

		gotEnqueued := h.GetEnqueuedMessages(t, r.client)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.DefaultQueue, diff)
		}
	}
}

func TestKillRetryTask(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := time.Now().Add(time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		retry     []h.ZSetEntry
		dead      []h.ZSetEntry
		id        xid.ID
		score     int64
		want      error
		wantRetry []h.ZSetEntry
		wantDead  []h.ZSetEntry
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			dead:  []h.ZSetEntry{},
			id:    m1.ID,
			score: t1.Unix(),
			want:  nil,
			wantRetry: []h.ZSetEntry{
				{Msg: m2, Score: t2.Unix()},
			},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Unix()},
			},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
			},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: t2.Unix()},
			},
			id:    m2.ID,
			score: t2.Unix(),
			want:  ErrTaskNotFound,
			wantRetry: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
			},
			wantDead: []h.ZSetEntry{
				{Msg: m2, Score: t2.Unix()},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedRetryQueue(t, r.client, tc.retry)
		h.SeedDeadQueue(t, r.client, tc.dead)

		got := r.KillRetryTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("(*RDB).KillRetryTask(%v, %v) = %v, want %v",
				tc.id, tc.score, got, tc.want)
			continue
		}

		gotRetry := h.GetRetryEntries(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.RetryQueue, diff)
		}

		gotDead := h.GetDeadEntries(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.DeadQueue, diff)
		}
	}
}

func TestKillScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := time.Now().Add(time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []h.ZSetEntry
		dead          []h.ZSetEntry
		id            xid.ID
		score         int64
		want          error
		wantScheduled []h.ZSetEntry
		wantDead      []h.ZSetEntry
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			dead:  []h.ZSetEntry{},
			id:    m1.ID,
			score: t1.Unix(),
			want:  nil,
			wantScheduled: []h.ZSetEntry{
				{Msg: m2, Score: t2.Unix()},
			},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Unix()},
			},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
			},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: t2.Unix()},
			},
			id:    m2.ID,
			score: t2.Unix(),
			want:  ErrTaskNotFound,
			wantScheduled: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
			},
			wantDead: []h.ZSetEntry{
				{Msg: m2, Score: t2.Unix()},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedScheduledQueue(t, r.client, tc.scheduled)
		h.SeedDeadQueue(t, r.client, tc.dead)

		got := r.KillScheduledTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("(*RDB).KillScheduledTask(%v, %v) = %v, want %v",
				tc.id, tc.score, got, tc.want)
			continue
		}

		gotScheduled := h.GetScheduledEntries(t, r.client)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.SortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.ScheduledQueue, diff)
		}

		gotDead := h.GetDeadEntries(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.DeadQueue, diff)
		}
	}
}

func TestKillAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := time.Now().Add(time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		retry     []h.ZSetEntry
		dead      []h.ZSetEntry
		want      int64
		wantRetry []h.ZSetEntry
		wantDead  []h.ZSetEntry
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			dead:      []h.ZSetEntry{},
			want:      2,
			wantRetry: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Unix()},
				{Msg: m2, Score: time.Now().Unix()},
			},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
			},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: t2.Unix()},
			},
			want:      1,
			wantRetry: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
		},
		{
			retry: []h.ZSetEntry{},
			dead: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			want:      0,
			wantRetry: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedRetryQueue(t, r.client, tc.retry)
		h.SeedDeadQueue(t, r.client, tc.dead)

		got, err := r.KillAllRetryTasks()
		if got != tc.want || err != nil {
			t.Errorf("(*RDB).KillAllRetryTasks() = %v, %v; want %v, nil",
				got, err, tc.want)
			continue
		}

		gotRetry := h.GetRetryEntries(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.RetryQueue, diff)
		}

		gotDead := h.GetDeadEntries(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.DeadQueue, diff)
		}
	}
}

func TestKillAllScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := time.Now().Add(time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []h.ZSetEntry
		dead          []h.ZSetEntry
		want          int64
		wantScheduled []h.ZSetEntry
		wantDead      []h.ZSetEntry
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			dead:          []h.ZSetEntry{},
			want:          2,
			wantScheduled: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Unix()},
				{Msg: m2, Score: time.Now().Unix()},
			},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
			},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: t2.Unix()},
			},
			want:          1,
			wantScheduled: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
		},
		{
			scheduled: []h.ZSetEntry{},
			dead: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			want:          0,
			wantScheduled: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedScheduledQueue(t, r.client, tc.scheduled)
		h.SeedDeadQueue(t, r.client, tc.dead)

		got, err := r.KillAllScheduledTasks()
		if got != tc.want || err != nil {
			t.Errorf("(*RDB).KillAllScheduledTasks() = %v, %v; want %v, nil",
				got, err, tc.want)
			continue
		}

		gotScheduled := h.GetScheduledEntries(t, r.client)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.SortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.ScheduledQueue, diff)
		}

		gotDead := h.GetDeadEntries(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.DeadQueue, diff)
		}
	}
}

func TestDeleteDeadTask(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := time.Now().Add(-5 * time.Minute)
	t2 := time.Now().Add(-time.Hour)

	tests := []struct {
		dead     []h.ZSetEntry
		id       xid.ID
		score    int64
		want     error
		wantDead []*base.TaskMessage
	}{
		{
			dead: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			id:       m1.ID,
			score:    t1.Unix(),
			want:     nil,
			wantDead: []*base.TaskMessage{m2},
		},
		{
			dead: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			id:       m1.ID,
			score:    t2.Unix(), // id and score mismatch
			want:     ErrTaskNotFound,
			wantDead: []*base.TaskMessage{m1, m2},
		},
		{
			dead:     []h.ZSetEntry{},
			id:       m1.ID,
			score:    t1.Unix(),
			want:     ErrTaskNotFound,
			wantDead: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedDeadQueue(t, r.client, tc.dead)

		got := r.DeleteDeadTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteDeadTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotDead := h.GetDeadMessages(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadQueue, diff)
		}
	}
}

func TestDeleteRetryTask(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		retry     []h.ZSetEntry
		id        xid.ID
		score     int64
		want      error
		wantRetry []*base.TaskMessage
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			id:        m1.ID,
			score:     t1.Unix(),
			want:      nil,
			wantRetry: []*base.TaskMessage{m2},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
			},
			id:        m2.ID,
			score:     t2.Unix(),
			want:      ErrTaskNotFound,
			wantRetry: []*base.TaskMessage{m1},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedRetryQueue(t, r.client, tc.retry)

		got := r.DeleteRetryTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteRetryTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotRetry := h.GetRetryMessages(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}

func TestDeleteScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []h.ZSetEntry
		id            xid.ID
		score         int64
		want          error
		wantScheduled []*base.TaskMessage
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
				{Msg: m2, Score: t2.Unix()},
			},
			id:            m1.ID,
			score:         t1.Unix(),
			want:          nil,
			wantScheduled: []*base.TaskMessage{m2},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: t1.Unix()},
			},
			id:            m2.ID,
			score:         t2.Unix(),
			want:          ErrTaskNotFound,
			wantScheduled: []*base.TaskMessage{m1},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedScheduledQueue(t, r.client, tc.scheduled)

		got := r.DeleteScheduledTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteScheduledTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotScheduled := h.GetScheduledMessages(t, r.client)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledQueue, diff)
		}
	}
}

func TestDeleteAllDeadTasks(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessage("gen_thumbnail", nil)

	tests := []struct {
		dead     []h.ZSetEntry
		wantDead []*base.TaskMessage
	}{
		{
			dead: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Unix()},
				{Msg: m2, Score: time.Now().Unix()},
				{Msg: m3, Score: time.Now().Unix()},
			},
			wantDead: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedDeadQueue(t, r.client, tc.dead)

		err := r.DeleteAllDeadTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotDead := h.GetDeadMessages(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadQueue, diff)
		}
	}
}

func TestDeleteAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessage("gen_thumbnail", nil)

	tests := []struct {
		retry     []h.ZSetEntry
		wantRetry []*base.TaskMessage
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Unix()},
				{Msg: m2, Score: time.Now().Unix()},
				{Msg: m3, Score: time.Now().Unix()},
			},
			wantRetry: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedRetryQueue(t, r.client, tc.retry)

		err := r.DeleteAllRetryTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotRetry := h.GetRetryMessages(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}

func TestDeleteAllScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessage("gen_thumbnail", nil)

	tests := []struct {
		scheduled     []h.ZSetEntry
		wantScheduled []*base.TaskMessage
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: time.Now().Add(time.Minute).Unix()},
				{Msg: m2, Score: time.Now().Add(time.Minute).Unix()},
				{Msg: m3, Score: time.Now().Add(time.Minute).Unix()},
			},
			wantScheduled: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedScheduledQueue(t, r.client, tc.scheduled)

		err := r.DeleteAllScheduledTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotScheduled := h.GetScheduledMessages(t, r.client)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledQueue, diff)
		}
	}
}
