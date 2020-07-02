// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

func TestCurrentStats(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessage("gen_thumbnail", map[string]interface{}{"src": "some/path/to/img"})
	m4 := h.NewTaskMessage("sync", nil)
	m5 := h.NewTaskMessage("important_notification", nil)
	m5.Queue = "critical"
	m6 := h.NewTaskMessage("minor_notification", nil)
	m6.Queue = "low"
	now := time.Now()

	tests := []struct {
		enqueued   map[string][]*base.TaskMessage
		inProgress []*base.TaskMessage
		scheduled  []h.ZSetEntry
		retry      []h.ZSetEntry
		dead       []h.ZSetEntry
		processed  int
		failed     int
		allQueues  []interface{}
		paused     []string
		want       *Stats
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1},
				"critical":            {m5},
				"low":                 {m6},
			},
			inProgress: []*base.TaskMessage{m2},
			scheduled: []h.ZSetEntry{
				{Msg: m3, Score: float64(now.Add(time.Hour).Unix())},
				{Msg: m4, Score: float64(now.Unix())}},
			retry:     []h.ZSetEntry{},
			dead:      []h.ZSetEntry{},
			processed: 120,
			failed:    2,
			allQueues: []interface{}{base.DefaultQueue, base.QueueKey("critical"), base.QueueKey("low")},
			paused:    []string{},
			want: &Stats{
				Enqueued:   3,
				InProgress: 1,
				Scheduled:  2,
				Retry:      0,
				Dead:       0,
				Processed:  120,
				Failed:     2,
				Timestamp:  now,
				// Queues should be sorted by name.
				Queues: []*Queue{
					{Name: "critical", Paused: false, Size: 1},
					{Name: "default", Paused: false, Size: 1},
					{Name: "low", Paused: false, Size: 1},
				},
			},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
			inProgress: []*base.TaskMessage{},
			scheduled: []h.ZSetEntry{
				{Msg: m3, Score: float64(now.Unix())},
				{Msg: m4, Score: float64(now.Unix())}},
			retry: []h.ZSetEntry{
				{Msg: m1, Score: float64(now.Add(time.Minute).Unix())}},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: float64(now.Add(-time.Hour).Unix())}},
			processed: 90,
			failed:    10,
			allQueues: []interface{}{base.DefaultQueue},
			paused:    []string{},
			want: &Stats{
				Enqueued:   0,
				InProgress: 0,
				Scheduled:  2,
				Retry:      1,
				Dead:       1,
				Processed:  90,
				Failed:     10,
				Timestamp:  now,
				Queues: []*Queue{
					{Name: base.DefaultQueueName, Paused: false, Size: 0},
				},
			},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1},
				"critical":            {m5},
				"low":                 {m6},
			},
			inProgress: []*base.TaskMessage{m2},
			scheduled: []h.ZSetEntry{
				{Msg: m3, Score: float64(now.Add(time.Hour).Unix())},
				{Msg: m4, Score: float64(now.Unix())}},
			retry:     []h.ZSetEntry{},
			dead:      []h.ZSetEntry{},
			processed: 120,
			failed:    2,
			allQueues: []interface{}{base.DefaultQueue, base.QueueKey("critical"), base.QueueKey("low")},
			paused:    []string{"critical", "low"},
			want: &Stats{
				Enqueued:   3,
				InProgress: 1,
				Scheduled:  2,
				Retry:      0,
				Dead:       0,
				Processed:  120,
				Failed:     2,
				Timestamp:  now,
				Queues: []*Queue{
					{Name: "critical", Paused: true, Size: 1},
					{Name: "default", Paused: false, Size: 1},
					{Name: "low", Paused: true, Size: 1},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		for _, qname := range tc.paused {
			if err := r.Pause(qname); err != nil {
				t.Fatal(err)
			}
		}
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r.client, msgs, qname)
		}
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		h.SeedScheduledQueue(t, r.client, tc.scheduled)
		h.SeedRetryQueue(t, r.client, tc.retry)
		h.SeedDeadQueue(t, r.client, tc.dead)
		processedKey := base.ProcessedKey(now)
		failedKey := base.FailureKey(now)
		r.client.Set(processedKey, tc.processed, 0)
		r.client.Set(failedKey, tc.failed, 0)
		r.client.SAdd(base.AllQueues, tc.allQueues...)

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
		Queues:     make([]*Queue, 0),
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
	m3 := h.NewTaskMessageWithQueue("important_notification", nil, "critical")
	m4 := h.NewTaskMessageWithQueue("minor_notification", nil, "low")
	t1 := &EnqueuedTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload, Queue: m1.Queue}
	t2 := &EnqueuedTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload, Queue: m2.Queue}
	t3 := &EnqueuedTask{ID: m3.ID, Type: m3.Type, Payload: m3.Payload, Queue: m3.Queue}
	tests := []struct {
		enqueued map[string][]*base.TaskMessage
		qname    string
		want     []*EnqueuedTask
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1, m2},
			},
			qname: base.DefaultQueueName,
			want:  []*EnqueuedTask{t1, t2},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
			qname: base.DefaultQueueName,
			want:  []*EnqueuedTask{},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1, m2},
				"critical":            {m3},
				"low":                 {m4},
			},
			qname: base.DefaultQueueName,
			want:  []*EnqueuedTask{t1, t2},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1, m2},
				"critical":            {m3},
				"low":                 {m4},
			},
			qname: "critical",
			want:  []*EnqueuedTask{t3},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r.client, msgs, qname)
		}

		got, err := r.ListEnqueued(tc.qname, Pagination{Size: 20, Page: 0})
		op := fmt.Sprintf("r.ListEnqueued(%q, Pagination{Size: 20, Page: 0})", tc.qname)
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.want)
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
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s", op, got, err, tc.want, diff)
			continue
		}
	}
}
func TestListEnqueuedPagination(t *testing.T) {
	r := setup(t)
	var msgs []*base.TaskMessage
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		msgs = append(msgs, msg)
	}
	// create 100 tasks in default queue
	h.SeedEnqueuedQueue(t, r.client, msgs)

	msgs = []*base.TaskMessage(nil) // empty list
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("custom %d", i), nil)
		msgs = append(msgs, msg)
	}
	// create 100 tasks in custom queue
	h.SeedEnqueuedQueue(t, r.client, msgs, "custom")

	tests := []struct {
		desc      string
		qname     string
		page      int
		size      int
		wantSize  int
		wantFirst string
		wantLast  string
	}{
		{"first page", "default", 0, 20, 20, "task 0", "task 19"},
		{"second page", "default", 1, 20, 20, "task 20", "task 39"},
		{"different page size", "default", 2, 30, 30, "task 60", "task 89"},
		{"last page", "default", 3, 30, 10, "task 90", "task 99"},
		{"out of range", "default", 4, 30, 0, "", ""},
		{"second page with custom queue", "custom", 1, 20, 20, "custom 20", "custom 39"},
	}

	for _, tc := range tests {
		got, err := r.ListEnqueued(tc.qname, Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListEnqueued(%q, Pagination{Size: %d, Page: %d})", tc.qname, tc.size, tc.page)
		if err != nil {
			t.Errorf("%s; %s returned error %v", tc.desc, op, err)
			continue
		}

		if len(got) != tc.wantSize {
			t.Errorf("%s; %s returned a list of size %d, want %d", tc.desc, op, len(got), tc.wantSize)
			continue
		}

		if tc.wantSize == 0 {
			continue
		}

		first := got[0]
		if first.Type != tc.wantFirst {
			t.Errorf("%s; %s returned a list with first message %q, want %q",
				tc.desc, op, first.Type, tc.wantFirst)
		}

		last := got[len(got)-1]
		if last.Type != tc.wantLast {
			t.Errorf("%s; %s returned a list with the last message %q, want %q",
				tc.desc, op, last.Type, tc.wantLast)
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

		got, err := r.ListInProgress(Pagination{Size: 20, Page: 0})
		op := "r.ListInProgress(Pagination{Size: 20, Page: 0})"
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.want)
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
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s", op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListInProgressPagination(t *testing.T) {
	r := setup(t)
	var msgs []*base.TaskMessage
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		msgs = append(msgs, msg)
	}
	h.SeedInProgressQueue(t, r.client, msgs)

	tests := []struct {
		desc      string
		page      int
		size      int
		wantSize  int
		wantFirst string
		wantLast  string
	}{
		{"first page", 0, 20, 20, "task 0", "task 19"},
		{"second page", 1, 20, 20, "task 20", "task 39"},
		{"different page size", 2, 30, 30, "task 60", "task 89"},
		{"last page", 3, 30, 10, "task 90", "task 99"},
		{"out of range", 4, 30, 0, "", ""},
	}

	for _, tc := range tests {
		got, err := r.ListInProgress(Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListInProgress(Pagination{Size: %d, Page: %d})", tc.size, tc.page)
		if err != nil {
			t.Errorf("%s; %s returned error %v", tc.desc, op, err)
			continue
		}

		if len(got) != tc.wantSize {
			t.Errorf("%s; %s returned list of size %d, want %d", tc.desc, op, len(got), tc.wantSize)
			continue
		}

		if tc.wantSize == 0 {
			continue
		}

		first := got[0]
		if first.Type != tc.wantFirst {
			t.Errorf("%s; %s returned a list with first message %q, want %q",
				tc.desc, op, first.Type, tc.wantFirst)
		}

		last := got[len(got)-1]
		if last.Type != tc.wantLast {
			t.Errorf("%s; %s returned a list with the last message %q, want %q",
				tc.desc, op, last.Type, tc.wantLast)
		}
	}
}

func TestListScheduled(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := h.NewTaskMessage("reindex", nil)
	p1 := time.Now().Add(30 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	t1 := &ScheduledTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload, ProcessAt: p1, Score: p1.Unix(), Queue: m1.Queue}
	t2 := &ScheduledTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload, ProcessAt: p2, Score: p2.Unix(), Queue: m2.Queue}

	tests := []struct {
		scheduled []h.ZSetEntry
		want      []*ScheduledTask
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: float64(p1.Unix())},
				{Msg: m2, Score: float64(p2.Unix())},
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

		got, err := r.ListScheduled(Pagination{Size: 20, Page: 0})
		op := "r.ListScheduled(Pagination{Size: 20, Page: 0})"
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.want)
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
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s", op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListScheduledPagination(t *testing.T) {
	r := setup(t)
	// create 100 tasks with an increasing number of wait time.
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		if err := r.Schedule(msg, time.Now().Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		desc      string
		page      int
		size      int
		wantSize  int
		wantFirst string
		wantLast  string
	}{
		{"first page", 0, 20, 20, "task 0", "task 19"},
		{"second page", 1, 20, 20, "task 20", "task 39"},
		{"different page size", 2, 30, 30, "task 60", "task 89"},
		{"last page", 3, 30, 10, "task 90", "task 99"},
		{"out of range", 4, 30, 0, "", ""},
	}

	for _, tc := range tests {
		got, err := r.ListScheduled(Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListScheduled(Pagination{Size: %d, Page: %d})", tc.size, tc.page)
		if err != nil {
			t.Errorf("%s; %s returned error %v", tc.desc, op, err)
			continue
		}

		if len(got) != tc.wantSize {
			t.Errorf("%s; %s returned list of size %d, want %d", tc.desc, op, len(got), tc.wantSize)
			continue
		}

		if tc.wantSize == 0 {
			continue
		}

		first := got[0]
		if first.Type != tc.wantFirst {
			t.Errorf("%s; %s returned a list with first message %q, want %q",
				tc.desc, op, first.Type, tc.wantFirst)
		}

		last := got[len(got)-1]
		if last.Type != tc.wantLast {
			t.Errorf("%s; %s returned a list with the last message %q, want %q",
				tc.desc, op, last.Type, tc.wantLast)
		}
	}
}

func TestListRetry(t *testing.T) {
	r := setup(t)
	m1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "send_email",
		Queue:    "default",
		Payload:  map[string]interface{}{"subject": "hello"},
		ErrorMsg: "email server not responding",
		Retry:    25,
		Retried:  10,
	}
	m2 := &base.TaskMessage{
		ID:       uuid.New(),
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
		Queue:     m1.Queue,
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
		Queue:     m1.Queue,
	}

	tests := []struct {
		retry []h.ZSetEntry
		want  []*RetryTask
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: float64(p1.Unix())},
				{Msg: m2, Score: float64(p2.Unix())},
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

		got, err := r.ListRetry(Pagination{Size: 20, Page: 0})
		op := "r.ListRetry(Pagination{Size: 20, Page: 0})"
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.want)
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
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s", op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListRetryPagination(t *testing.T) {
	r := setup(t)
	// create 100 tasks with an increasing number of wait time.
	now := time.Now()
	var seed []h.ZSetEntry
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		processAt := now.Add(time.Duration(i) * time.Second)
		seed = append(seed, h.ZSetEntry{Msg: msg, Score: float64(processAt.Unix())})
	}
	h.SeedRetryQueue(t, r.client, seed)

	tests := []struct {
		desc      string
		page      int
		size      int
		wantSize  int
		wantFirst string
		wantLast  string
	}{
		{"first page", 0, 20, 20, "task 0", "task 19"},
		{"second page", 1, 20, 20, "task 20", "task 39"},
		{"different page size", 2, 30, 30, "task 60", "task 89"},
		{"last page", 3, 30, 10, "task 90", "task 99"},
		{"out of range", 4, 30, 0, "", ""},
	}

	for _, tc := range tests {
		got, err := r.ListRetry(Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListRetry(Pagination{Size: %d, Page: %d})", tc.size, tc.page)
		if err != nil {
			t.Errorf("%s; %s returned error %v", tc.desc, op, err)
			continue
		}

		if len(got) != tc.wantSize {
			t.Errorf("%s; %s returned list of size %d, want %d", tc.desc, op, len(got), tc.wantSize)
			continue
		}

		if tc.wantSize == 0 {
			continue
		}

		first := got[0]
		if first.Type != tc.wantFirst {
			t.Errorf("%s; %s returned a list with first message %q, want %q",
				tc.desc, op, first.Type, tc.wantFirst)
		}

		last := got[len(got)-1]
		if last.Type != tc.wantLast {
			t.Errorf("%s; %s returned a list with the last message %q, want %q",
				tc.desc, op, last.Type, tc.wantLast)
		}
	}
}

func TestListDead(t *testing.T) {
	r := setup(t)
	m1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "send_email",
		Queue:    "default",
		Payload:  map[string]interface{}{"subject": "hello"},
		ErrorMsg: "email server not responding",
	}
	m2 := &base.TaskMessage{
		ID:       uuid.New(),
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
		Queue:        m1.Queue,
	}
	t2 := &DeadTask{
		ID:           m2.ID,
		Type:         m2.Type,
		Payload:      m2.Payload,
		LastFailedAt: f2,
		ErrorMsg:     m2.ErrorMsg,
		Score:        f2.Unix(),
		Queue:        m2.Queue,
	}

	tests := []struct {
		dead []h.ZSetEntry
		want []*DeadTask
	}{
		{
			dead: []h.ZSetEntry{
				{Msg: m1, Score: float64(f1.Unix())},
				{Msg: m2, Score: float64(f2.Unix())},
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

		got, err := r.ListDead(Pagination{Size: 20, Page: 0})
		op := "r.ListDead(Pagination{Size: 20, Page: 0})"
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.want)
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
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s", op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListDeadPagination(t *testing.T) {
	r := setup(t)
	var entries []h.ZSetEntry
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		entries = append(entries, h.ZSetEntry{Msg: msg, Score: float64(i)})
	}
	h.SeedDeadQueue(t, r.client, entries)

	tests := []struct {
		desc      string
		page      int
		size      int
		wantSize  int
		wantFirst string
		wantLast  string
	}{
		{"first page", 0, 20, 20, "task 0", "task 19"},
		{"second page", 1, 20, 20, "task 20", "task 39"},
		{"different page size", 2, 30, 30, "task 60", "task 89"},
		{"last page", 3, 30, 10, "task 90", "task 99"},
		{"out of range", 4, 30, 0, "", ""},
	}

	for _, tc := range tests {
		got, err := r.ListDead(Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListDead(Pagination{Size: %d, Page: %d})", tc.size, tc.page)
		if err != nil {
			t.Errorf("%s; %s returned error %v", tc.desc, op, err)
			continue
		}

		if len(got) != tc.wantSize {
			t.Errorf("%s; %s returned list of size %d, want %d", tc.desc, op, len(got), tc.wantSize)
			continue
		}

		if tc.wantSize == 0 {
			continue
		}

		first := got[0]
		if first.Type != tc.wantFirst {
			t.Errorf("%s; %s returned a list with first message %q, want %q",
				tc.desc, op, first.Type, tc.wantFirst)
		}

		last := got[len(got)-1]
		if last.Type != tc.wantLast {
			t.Errorf("%s; %s returned a list with the last message %q, want %q",
				tc.desc, op, last.Type, tc.wantLast)
		}
	}
}

var timeCmpOpt = cmpopts.EquateApproxTime(time.Second)

func TestEnqueueDeadTask(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("send_notification", nil)
	t3.Queue = "critical"
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		dead         []h.ZSetEntry
		score        int64
		id           uuid.UUID
		want         error // expected return value from calling EnqueueDeadTask
		wantDead     []*base.TaskMessage
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			dead: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
			},
			score:    s2,
			id:       t2.ID,
			want:     nil,
			wantDead: []*base.TaskMessage{t1},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t2},
			},
		},
		{
			dead: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
			},
			score:    123,
			id:       t2.ID,
			want:     ErrTaskNotFound,
			wantDead: []*base.TaskMessage{t1, t2},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			dead: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
				{Msg: t3, Score: float64(s1)},
			},
			score:    s1,
			id:       t3.ID,
			want:     nil,
			wantDead: []*base.TaskMessage{t1, t2},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
				"critical":            {t3},
			},
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

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
			}
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
	t3 := h.NewTaskMessage("send_notification", nil)
	t3.Queue = "low"
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()
	tests := []struct {
		retry        []h.ZSetEntry
		score        int64
		id           uuid.UUID
		want         error // expected return value from calling EnqueueRetryTask
		wantRetry    []*base.TaskMessage
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
			},
			score:     s2,
			id:        t2.ID,
			want:      nil,
			wantRetry: []*base.TaskMessage{t1},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t2},
			},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
			},
			score:     123,
			id:        t2.ID,
			want:      ErrTaskNotFound,
			wantRetry: []*base.TaskMessage{t1, t2},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
				{Msg: t3, Score: float64(s2)},
			},
			score:     s2,
			id:        t3.ID,
			want:      nil,
			wantRetry: []*base.TaskMessage{t1, t2},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
				"low":                 {t3},
			},
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

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
			}
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
	t3 := h.NewTaskMessage("send_notification", nil)
	t3.Queue = "notifications"
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		scheduled     []h.ZSetEntry
		score         int64
		id            uuid.UUID
		want          error // expected return value from calling EnqueueScheduledTask
		wantScheduled []*base.TaskMessage
		wantEnqueued  map[string][]*base.TaskMessage
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
			},
			score:         s2,
			id:            t2.ID,
			want:          nil,
			wantScheduled: []*base.TaskMessage{t1},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t2},
			},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
			},
			score:         123,
			id:            t2.ID,
			want:          ErrTaskNotFound,
			wantScheduled: []*base.TaskMessage{t1, t2},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(s1)},
				{Msg: t2, Score: float64(s2)},
				{Msg: t3, Score: float64(s1)},
			},
			score:         s1,
			id:            t3.ID,
			want:          nil,
			wantScheduled: []*base.TaskMessage{t1, t2},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
				"notifications":       {t3},
			},
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

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
			}
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
	t4 := h.NewTaskMessage("important_notification", nil)
	t4.Queue = "critical"
	t5 := h.NewTaskMessage("minor_notification", nil)
	t5.Queue = "low"

	tests := []struct {
		desc         string
		scheduled    []h.ZSetEntry
		want         int64
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in scheduled queue",
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t2, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t3, Score: float64(time.Now().Add(time.Hour).Unix())},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			desc:      "with empty scheduled queue",
			scheduled: []h.ZSetEntry{},
			want:      0,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			desc: "with custom queues",
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t2, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t3, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t4, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t5, Score: float64(time.Now().Add(time.Hour).Unix())},
			},
			want: 5,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
				"critical":            {t4},
				"low":                 {t5},
			},
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

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.QueueKey(qname), diff)
			}
		}
	}
}

func TestEnqueueAllRetryTasks(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("reindex", nil)
	t4 := h.NewTaskMessage("important_notification", nil)
	t4.Queue = "critical"
	t5 := h.NewTaskMessage("minor_notification", nil)
	t5.Queue = "low"

	tests := []struct {
		desc         string
		retry        []h.ZSetEntry
		want         int64
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in retry queue",
			retry: []h.ZSetEntry{
				{Msg: t1, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t2, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t3, Score: float64(time.Now().Add(time.Hour).Unix())},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			desc:  "with empty retry queue",
			retry: []h.ZSetEntry{},
			want:  0,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			desc: "with custom queues",
			retry: []h.ZSetEntry{
				{Msg: t1, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t2, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t3, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t4, Score: float64(time.Now().Add(time.Hour).Unix())},
				{Msg: t5, Score: float64(time.Now().Add(time.Hour).Unix())},
			},
			want: 5,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
				"critical":            {t4},
				"low":                 {t5},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedRetryQueue(t, r.client, tc.retry)

		got, err := r.EnqueueAllRetryTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllRetryTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllRetryTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
		}

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.QueueKey(qname), diff)
			}
		}
	}
}

func TestEnqueueAllDeadTasks(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("reindex", nil)
	t4 := h.NewTaskMessage("important_notification", nil)
	t4.Queue = "critical"
	t5 := h.NewTaskMessage("minor_notification", nil)
	t5.Queue = "low"

	tests := []struct {
		desc         string
		dead         []h.ZSetEntry
		want         int64
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in dead queue",
			dead: []h.ZSetEntry{
				{Msg: t1, Score: float64(time.Now().Add(-time.Minute).Unix())},
				{Msg: t2, Score: float64(time.Now().Add(-time.Minute).Unix())},
				{Msg: t3, Score: float64(time.Now().Add(-time.Minute).Unix())},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			desc: "with empty dead queue",
			dead: []h.ZSetEntry{},
			want: 0,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			desc: "with custom queues",
			dead: []h.ZSetEntry{
				{Msg: t1, Score: float64(time.Now().Add(-time.Minute).Unix())},
				{Msg: t2, Score: float64(time.Now().Add(-time.Minute).Unix())},
				{Msg: t3, Score: float64(time.Now().Add(-time.Minute).Unix())},
				{Msg: t4, Score: float64(time.Now().Add(-time.Minute).Unix())},
				{Msg: t5, Score: float64(time.Now().Add(-time.Minute).Unix())},
			},
			want: 5,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
				"critical":            {t4},
				"low":                 {t5},
			},
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

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.QueueKey(qname), diff)
			}
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
		id        uuid.UUID
		score     int64
		want      error
		wantRetry []h.ZSetEntry
		wantDead  []h.ZSetEntry
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			dead:  []h.ZSetEntry{},
			id:    m1.ID,
			score: t1.Unix(),
			want:  nil,
			wantRetry: []h.ZSetEntry{
				{Msg: m2, Score: float64(t2.Unix())},
			},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: float64(time.Now().Unix())},
			},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
			},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: float64(t2.Unix())},
			},
			id:    m2.ID,
			score: t2.Unix(),
			want:  ErrTaskNotFound,
			wantRetry: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
			},
			wantDead: []h.ZSetEntry{
				{Msg: m2, Score: float64(t2.Unix())},
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
		id            uuid.UUID
		score         int64
		want          error
		wantScheduled []h.ZSetEntry
		wantDead      []h.ZSetEntry
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			dead:  []h.ZSetEntry{},
			id:    m1.ID,
			score: t1.Unix(),
			want:  nil,
			wantScheduled: []h.ZSetEntry{
				{Msg: m2, Score: float64(t2.Unix())},
			},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: float64(time.Now().Unix())},
			},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
			},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: float64(t2.Unix())},
			},
			id:    m2.ID,
			score: t2.Unix(),
			want:  ErrTaskNotFound,
			wantScheduled: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
			},
			wantDead: []h.ZSetEntry{
				{Msg: m2, Score: float64(t2.Unix())},
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
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			dead:      []h.ZSetEntry{},
			want:      2,
			wantRetry: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: float64(time.Now().Unix())},
				{Msg: m2, Score: float64(time.Now().Unix())},
			},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
			},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: float64(t2.Unix())},
			},
			want:      1,
			wantRetry: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: float64(time.Now().Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
		},
		{
			retry: []h.ZSetEntry{},
			dead: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			want:      0,
			wantRetry: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
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
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			dead:          []h.ZSetEntry{},
			want:          2,
			wantScheduled: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: float64(time.Now().Unix())},
				{Msg: m2, Score: float64(time.Now().Unix())},
			},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
			},
			dead: []h.ZSetEntry{
				{Msg: m2, Score: float64(t2.Unix())},
			},
			want:          1,
			wantScheduled: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: float64(time.Now().Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
		},
		{
			scheduled: []h.ZSetEntry{},
			dead: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			want:          0,
			wantScheduled: []h.ZSetEntry{},
			wantDead: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
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
		id       uuid.UUID
		score    int64
		want     error
		wantDead []*base.TaskMessage
	}{
		{
			dead: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			id:       m1.ID,
			score:    t1.Unix(),
			want:     nil,
			wantDead: []*base.TaskMessage{m2},
		},
		{
			dead: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
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
		id        uuid.UUID
		score     int64
		want      error
		wantRetry []*base.TaskMessage
	}{
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			id:        m1.ID,
			score:     t1.Unix(),
			want:      nil,
			wantRetry: []*base.TaskMessage{m2},
		},
		{
			retry: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
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
		id            uuid.UUID
		score         int64
		want          error
		wantScheduled []*base.TaskMessage
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
				{Msg: m2, Score: float64(t2.Unix())},
			},
			id:            m1.ID,
			score:         t1.Unix(),
			want:          nil,
			wantScheduled: []*base.TaskMessage{m2},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: m1, Score: float64(t1.Unix())},
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
				{Msg: m1, Score: float64(time.Now().Unix())},
				{Msg: m2, Score: float64(time.Now().Unix())},
				{Msg: m3, Score: float64(time.Now().Unix())},
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
				{Msg: m1, Score: float64(time.Now().Unix())},
				{Msg: m2, Score: float64(time.Now().Unix())},
				{Msg: m3, Score: float64(time.Now().Unix())},
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
				{Msg: m1, Score: float64(time.Now().Add(time.Minute).Unix())},
				{Msg: m2, Score: float64(time.Now().Add(time.Minute).Unix())},
				{Msg: m3, Score: float64(time.Now().Add(time.Minute).Unix())},
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

func TestRemoveQueue(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessage("gen_thumbnail", nil)

	tests := []struct {
		enqueued     map[string][]*base.TaskMessage
		qname        string // queue to remove
		force        bool
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m2, m3},
				"low":      {},
			},
			qname: "low",
			force: false,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m2, m3},
			},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m2, m3},
				"low":      {},
			},
			qname: "critical",
			force: true, // allow removing non-empty queue
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {m1},
				"low":     {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r.client, msgs, qname)
		}

		err := r.RemoveQueue(tc.qname, tc.force)
		if err != nil {
			t.Errorf("(*RDB).RemoveQueue(%q) = %v, want nil", tc.qname, err)
			continue
		}

		qkey := base.QueueKey(tc.qname)
		if r.client.SIsMember(base.AllQueues, qkey).Val() {
			t.Errorf("%q is a member of %q", qkey, base.AllQueues)
		}

		if r.client.LLen(qkey).Val() != 0 {
			t.Errorf("queue %q is not empty", qkey)
		}

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got):\n%s", base.QueueKey(qname), diff)
			}
		}
	}
}

func TestRemoveQueueError(t *testing.T) {
	r := setup(t)
	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessage("gen_thumbnail", nil)

	tests := []struct {
		desc     string
		enqueued map[string][]*base.TaskMessage
		qname    string // queue to remove
		force    bool
	}{
		{
			desc: "removing non-existent queue",
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m2, m3},
				"low":      {},
			},
			qname: "nonexistent",
			force: false,
		},
		{
			desc: "removing non-empty queue",
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m2, m3},
				"low":      {},
			},
			qname: "critical",
			force: false,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r.client, msgs, qname)
		}

		got := r.RemoveQueue(tc.qname, tc.force)
		if got == nil {
			t.Errorf("%s;(*RDB).RemoveQueue(%q) = nil, want error", tc.desc, tc.qname)
			continue
		}

		// Make sure that nothing changed
		for qname, want := range tc.enqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("%s;mismatch found in %q; (-want,+got):\n%s", tc.desc, base.QueueKey(qname), diff)
			}
		}
	}
}

func TestListServers(t *testing.T) {
	r := setup(t)

	started1 := time.Now().Add(-time.Hour)
	info1 := &base.ServerInfo{
		Host:              "do.droplet1",
		PID:               1234,
		ServerID:          "server123",
		Concurrency:       10,
		Queues:            map[string]int{"default": 1},
		Status:            "running",
		Started:           started1,
		ActiveWorkerCount: 0,
	}

	started2 := time.Now().Add(-2 * time.Hour)
	info2 := &base.ServerInfo{
		Host:              "do.droplet2",
		PID:               9876,
		ServerID:          "server456",
		Concurrency:       20,
		Queues:            map[string]int{"email": 1},
		Status:            "stopped",
		Started:           started2,
		ActiveWorkerCount: 1,
	}

	tests := []struct {
		data []*base.ServerInfo
	}{
		{
			data: []*base.ServerInfo{},
		},
		{
			data: []*base.ServerInfo{info1},
		},
		{
			data: []*base.ServerInfo{info1, info2},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		for _, info := range tc.data {
			if err := r.WriteServerState(info, []*base.WorkerInfo{}, 5*time.Second); err != nil {
				t.Fatal(err)
			}
		}

		got, err := r.ListServers()
		if err != nil {
			t.Errorf("r.ListServers returned an error: %v", err)
		}
		if diff := cmp.Diff(tc.data, got, h.SortServerInfoOpt); diff != "" {
			t.Errorf("r.ListServers returned %v, want %v; (-want,+got)\n%s",
				got, tc.data, diff)
		}
	}
}

func TestListWorkers(t *testing.T) {
	r := setup(t)

	var (
		host = "127.0.0.1"
		pid  = 4567

		m1 = h.NewTaskMessage("send_email", map[string]interface{}{"user_id": "abc123"})
		m2 = h.NewTaskMessage("gen_thumbnail", map[string]interface{}{"path": "some/path/to/image/file"})
		m3 = h.NewTaskMessage("reindex", map[string]interface{}{})
	)

	tests := []struct {
		data []*base.WorkerInfo
	}{
		{
			data: []*base.WorkerInfo{
				{Host: host, PID: pid, ID: m1.ID.String(), Type: m1.Type, Queue: m1.Queue, Payload: m1.Payload, Started: time.Now().Add(-1 * time.Second)},
				{Host: host, PID: pid, ID: m2.ID.String(), Type: m2.Type, Queue: m2.Queue, Payload: m2.Payload, Started: time.Now().Add(-5 * time.Second)},
				{Host: host, PID: pid, ID: m3.ID.String(), Type: m3.Type, Queue: m3.Queue, Payload: m3.Payload, Started: time.Now().Add(-30 * time.Second)},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		err := r.WriteServerState(&base.ServerInfo{}, tc.data, time.Minute)
		if err != nil {
			t.Errorf("could not write server state to redis: %v", err)
			continue
		}

		got, err := r.ListWorkers()
		if err != nil {
			t.Errorf("(*RDB).ListWorkers() returned an error: %v", err)
			continue
		}

		if diff := cmp.Diff(tc.data, got, h.SortWorkerInfoOpt); diff != "" {
			t.Errorf("(*RDB).ListWorkers() = %v, want = %v; (-want,+got)\n%s", got, tc.data, diff)
		}
	}
}

func TestPause(t *testing.T) {
	r := setup(t)

	tests := []struct {
		initial []string // initial keys in the paused set
		qname   string   // name of the queue to pause
		want    []string // expected keys in the paused set
	}{
		{[]string{}, "default", []string{"asynq:queues:default"}},
		{[]string{"asynq:queues:default"}, "critical", []string{"asynq:queues:default", "asynq:queues:critical"}},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		// Set up initial state.
		for _, qkey := range tc.initial {
			if err := r.client.SAdd(base.PausedQueues, qkey).Err(); err != nil {
				t.Fatal(err)
			}
		}

		err := r.Pause(tc.qname)
		if err != nil {
			t.Errorf("Pause(%q) returned error: %v", tc.qname, err)
		}

		got, err := r.client.SMembers(base.PausedQueues).Result()
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(tc.want, got, h.SortStringSliceOpt); diff != "" {
			t.Errorf("%q has members %v, want %v; (-want,+got)\n%s",
				base.PausedQueues, got, tc.want, diff)
		}
	}
}

func TestPauseError(t *testing.T) {
	r := setup(t)

	tests := []struct {
		desc    string   // test case description
		initial []string // initial keys in the paused set
		qname   string   // name of the queue to pause
		want    []string // expected keys in the paused set
	}{
		{"queue already paused", []string{"asynq:queues:default"}, "default", []string{"asynq:queues:default"}},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		// Set up initial state.
		for _, qkey := range tc.initial {
			if err := r.client.SAdd(base.PausedQueues, qkey).Err(); err != nil {
				t.Fatal(err)
			}
		}

		err := r.Pause(tc.qname)
		if err == nil {
			t.Errorf("%s; Pause(%q) returned nil: want error", tc.desc, tc.qname)
		}

		got, err := r.client.SMembers(base.PausedQueues).Result()
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(tc.want, got, h.SortStringSliceOpt); diff != "" {
			t.Errorf("%s; %q has members %v, want %v; (-want,+got)\n%s",
				tc.desc, base.PausedQueues, got, tc.want, diff)
		}
	}
}

func TestUnpause(t *testing.T) {
	r := setup(t)

	tests := []struct {
		initial []string // initial keys in the paused set
		qname   string   // name of the queue to unpause
		want    []string // expected keys in the paused set
	}{
		{[]string{"asynq:queues:default"}, "default", []string{}},
		{[]string{"asynq:queues:default", "asynq:queues:low"}, "low", []string{"asynq:queues:default"}},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		// Set up initial state.
		for _, qkey := range tc.initial {
			if err := r.client.SAdd(base.PausedQueues, qkey).Err(); err != nil {
				t.Fatal(err)
			}
		}

		err := r.Unpause(tc.qname)
		if err != nil {
			t.Errorf("Unpause(%q) returned error: %v", tc.qname, err)
		}

		got, err := r.client.SMembers(base.PausedQueues).Result()
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(tc.want, got, h.SortStringSliceOpt); diff != "" {
			t.Errorf("%q has members %v, want %v; (-want,+got)\n%s",
				base.PausedQueues, got, tc.want, diff)
		}
	}
}

func TestUnpauseError(t *testing.T) {
	r := setup(t)

	tests := []struct {
		desc    string   // test case description
		initial []string // initial keys in the paused set
		qname   string   // name of the queue to unpause
		want    []string // expected keys in the paused set
	}{
		{"set is empty", []string{}, "default", []string{}},
		{"queue is not in the set", []string{"asynq:queues:default"}, "low", []string{"asynq:queues:default"}},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		// Set up initial state.
		for _, qkey := range tc.initial {
			if err := r.client.SAdd(base.PausedQueues, qkey).Err(); err != nil {
				t.Fatal(err)
			}
		}

		err := r.Unpause(tc.qname)
		if err == nil {
			t.Errorf("%s; Unpause(%q) returned nil: want error", tc.desc, tc.qname)
		}

		got, err := r.client.SMembers(base.PausedQueues).Result()
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(tc.want, got, h.SortStringSliceOpt); diff != "" {
			t.Errorf("%s; %q has members %v, want %v; (-want,+got)\n%s",
				tc.desc, base.PausedQueues, got, tc.want, diff)
		}
	}
}
