// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"fmt"
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
		scheduled  []base.Z
		retry      []base.Z
		dead       []base.Z
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
			scheduled: []base.Z{
				{Message: m3, Score: now.Add(time.Hour).Unix()},
				{Message: m4, Score: now.Unix()}},
			retry:     []base.Z{},
			dead:      []base.Z{},
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
			scheduled: []base.Z{
				{Message: m3, Score: now.Unix()},
				{Message: m4, Score: now.Unix()}},
			retry: []base.Z{
				{Message: m1, Score: now.Add(time.Minute).Unix()}},
			dead: []base.Z{
				{Message: m2, Score: now.Add(-time.Hour).Unix()}},
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
			scheduled: []base.Z{
				{Message: m3, Score: now.Add(time.Hour).Unix()},
				{Message: m4, Score: now.Unix()}},
			retry:     []base.Z{},
			dead:      []base.Z{},
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

	tests := []struct {
		enqueued map[string][]*base.TaskMessage
		qname    string
		want     []*base.TaskMessage
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1, m2},
			},
			qname: base.DefaultQueueName,
			want:  []*base.TaskMessage{m1, m2},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: nil,
			},
			qname: base.DefaultQueueName,
			want:  []*base.TaskMessage(nil),
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1, m2},
				"critical":            {m3},
				"low":                 {m4},
			},
			qname: base.DefaultQueueName,
			want:  []*base.TaskMessage{m1, m2},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1, m2},
				"critical":            {m3},
				"low":                 {m4},
			},
			qname: "critical",
			want:  []*base.TaskMessage{m3},
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
		if diff := cmp.Diff(tc.want, got); diff != "" {
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

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)

	tests := []struct {
		inProgress []*base.TaskMessage
	}{
		{inProgress: []*base.TaskMessage{m1, m2}},
		{inProgress: []*base.TaskMessage(nil)},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedInProgressQueue(t, r.client, tc.inProgress)

		got, err := r.ListInProgress(Pagination{Size: 20, Page: 0})
		op := "r.ListInProgress(Pagination{Size: 20, Page: 0})"
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.inProgress)
			continue
		}
		if diff := cmp.Diff(tc.inProgress, got); diff != "" {
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s", op, got, err, tc.inProgress, diff)
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
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	p1 := time.Now().Add(30 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	p3 := time.Now().Add(5 * time.Minute)

	tests := []struct {
		scheduled []base.Z
		want      []base.Z
	}{
		{
			scheduled: []base.Z{
				{Message: m1, Score: p1.Unix()},
				{Message: m2, Score: p2.Unix()},
				{Message: m3, Score: p3.Unix()},
			},
			// should be sorted by score in ascending order
			want: []base.Z{
				{Message: m3, Score: p3.Unix()},
				{Message: m1, Score: p1.Unix()},
				{Message: m2, Score: p2.Unix()},
			},
		},
		{
			scheduled: []base.Z(nil),
			want:      []base.Z(nil),
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
		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
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

		first := got[0].Message
		if first.Type != tc.wantFirst {
			t.Errorf("%s; %s returned a list with first message %q, want %q",
				tc.desc, op, first.Type, tc.wantFirst)
		}

		last := got[len(got)-1].Message
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

	tests := []struct {
		retry []base.Z
		want  []base.Z
	}{
		{
			retry: []base.Z{
				{Message: m1, Score: p1.Unix()},
				{Message: m2, Score: p2.Unix()},
			},
			want: []base.Z{
				{Message: m1, Score: p1.Unix()},
				{Message: m2, Score: p2.Unix()},
			},
		},
		{
			retry: []base.Z(nil),
			want:  []base.Z(nil),
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
		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s",
				op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListRetryPagination(t *testing.T) {
	r := setup(t)
	// create 100 tasks with an increasing number of wait time.
	now := time.Now()
	var seed []base.Z
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		processAt := now.Add(time.Duration(i) * time.Second)
		seed = append(seed, base.Z{Message: msg, Score: processAt.Unix()})
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
		op := fmt.Sprintf("r.ListRetry(Pagination{Size: %d, Page: %d})",
			tc.size, tc.page)
		if err != nil {
			t.Errorf("%s; %s returned error %v", tc.desc, op, err)
			continue
		}

		if len(got) != tc.wantSize {
			t.Errorf("%s; %s returned list of size %d, want %d",
				tc.desc, op, len(got), tc.wantSize)
			continue
		}

		if tc.wantSize == 0 {
			continue
		}

		first := got[0].Message
		if first.Type != tc.wantFirst {
			t.Errorf("%s; %s returned a list with first message %q, want %q",
				tc.desc, op, first.Type, tc.wantFirst)
		}

		last := got[len(got)-1].Message
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

	tests := []struct {
		dead []base.Z
		want []base.Z
	}{
		{
			dead: []base.Z{
				{Message: m1, Score: f1.Unix()},
				{Message: m2, Score: f2.Unix()},
			},
			want: []base.Z{
				{Message: m2, Score: f2.Unix()}, // FIXME: shouldn't be sorted in the other order?
				{Message: m1, Score: f1.Unix()},
			},
		},
		{
			dead: []base.Z(nil),
			want: []base.Z(nil),
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
		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s",
				op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListDeadPagination(t *testing.T) {
	r := setup(t)
	var entries []base.Z
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		entries = append(entries, base.Z{Message: msg, Score: int64(i)})
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
		op := fmt.Sprintf("r.ListDead(Pagination{Size: %d, Page: %d})",
			tc.size, tc.page)
		if err != nil {
			t.Errorf("%s; %s returned error %v", tc.desc, op, err)
			continue
		}

		if len(got) != tc.wantSize {
			t.Errorf("%s; %s returned list of size %d, want %d",
				tc.desc, op, len(got), tc.wantSize)
			continue
		}

		if tc.wantSize == 0 {
			continue
		}

		first := got[0].Message
		if first.Type != tc.wantFirst {
			t.Errorf("%s; %s returned a list with first message %q, want %q",
				tc.desc, op, first.Type, tc.wantFirst)
		}

		last := got[len(got)-1].Message
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
		dead         []base.Z
		score        int64
		id           uuid.UUID
		want         error // expected return value from calling EnqueueDeadTask
		wantDead     []*base.TaskMessage
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			dead: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
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
			dead: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
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
			dead: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
				{Message: t3, Score: s1},
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
		retry        []base.Z
		score        int64
		id           uuid.UUID
		want         error // expected return value from calling EnqueueRetryTask
		wantRetry    []*base.TaskMessage
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			retry: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
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
			retry: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
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
			retry: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
				{Message: t3, Score: s2},
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
		scheduled     []base.Z
		score         int64
		id            uuid.UUID
		want          error // expected return value from calling EnqueueScheduledTask
		wantScheduled []*base.TaskMessage
		wantEnqueued  map[string][]*base.TaskMessage
	}{
		{
			scheduled: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
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
			scheduled: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
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
			scheduled: []base.Z{
				{Message: t1, Score: s1},
				{Message: t2, Score: s2},
				{Message: t3, Score: s1},
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
		scheduled    []base.Z
		want         int64
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in scheduled queue",
			scheduled: []base.Z{
				{Message: t1, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t2, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t3, Score: time.Now().Add(time.Hour).Unix()},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			desc:      "with empty scheduled queue",
			scheduled: []base.Z{},
			want:      0,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			desc: "with custom queues",
			scheduled: []base.Z{
				{Message: t1, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t2, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t3, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t4, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t5, Score: time.Now().Add(time.Hour).Unix()},
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
		retry        []base.Z
		want         int64
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in retry queue",
			retry: []base.Z{
				{Message: t1, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t2, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t3, Score: time.Now().Add(time.Hour).Unix()},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			desc:  "with empty retry queue",
			retry: []base.Z{},
			want:  0,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			desc: "with custom queues",
			retry: []base.Z{
				{Message: t1, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t2, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t3, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t4, Score: time.Now().Add(time.Hour).Unix()},
				{Message: t5, Score: time.Now().Add(time.Hour).Unix()},
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
		dead         []base.Z
		want         int64
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in dead queue",
			dead: []base.Z{
				{Message: t1, Score: time.Now().Add(-time.Minute).Unix()},
				{Message: t2, Score: time.Now().Add(-time.Minute).Unix()},
				{Message: t3, Score: time.Now().Add(-time.Minute).Unix()},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			desc: "with empty dead queue",
			dead: []base.Z{},
			want: 0,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
		},
		{
			desc: "with custom queues",
			dead: []base.Z{
				{Message: t1, Score: time.Now().Add(-time.Minute).Unix()},
				{Message: t2, Score: time.Now().Add(-time.Minute).Unix()},
				{Message: t3, Score: time.Now().Add(-time.Minute).Unix()},
				{Message: t4, Score: time.Now().Add(-time.Minute).Unix()},
				{Message: t5, Score: time.Now().Add(-time.Minute).Unix()},
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
		retry     []base.Z
		dead      []base.Z
		id        uuid.UUID
		score     int64
		want      error
		wantRetry []base.Z
		wantDead  []base.Z
	}{
		{
			retry: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			dead:  []base.Z{},
			id:    m1.ID,
			score: t1.Unix(),
			want:  nil,
			wantRetry: []base.Z{
				{Message: m2, Score: t2.Unix()},
			},
			wantDead: []base.Z{
				{Message: m1, Score: time.Now().Unix()},
			},
		},
		{
			retry: []base.Z{
				{Message: m1, Score: t1.Unix()},
			},
			dead: []base.Z{
				{Message: m2, Score: t2.Unix()},
			},
			id:    m2.ID,
			score: t2.Unix(),
			want:  ErrTaskNotFound,
			wantRetry: []base.Z{
				{Message: m1, Score: t1.Unix()},
			},
			wantDead: []base.Z{
				{Message: m2, Score: t2.Unix()},
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
		scheduled     []base.Z
		dead          []base.Z
		id            uuid.UUID
		score         int64
		want          error
		wantScheduled []base.Z
		wantDead      []base.Z
	}{
		{
			scheduled: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			dead:  []base.Z{},
			id:    m1.ID,
			score: t1.Unix(),
			want:  nil,
			wantScheduled: []base.Z{
				{Message: m2, Score: t2.Unix()},
			},
			wantDead: []base.Z{
				{Message: m1, Score: time.Now().Unix()},
			},
		},
		{
			scheduled: []base.Z{
				{Message: m1, Score: t1.Unix()},
			},
			dead: []base.Z{
				{Message: m2, Score: t2.Unix()},
			},
			id:    m2.ID,
			score: t2.Unix(),
			want:  ErrTaskNotFound,
			wantScheduled: []base.Z{
				{Message: m1, Score: t1.Unix()},
			},
			wantDead: []base.Z{
				{Message: m2, Score: t2.Unix()},
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
		retry     []base.Z
		dead      []base.Z
		want      int64
		wantRetry []base.Z
		wantDead  []base.Z
	}{
		{
			retry: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			dead:      []base.Z{},
			want:      2,
			wantRetry: []base.Z{},
			wantDead: []base.Z{
				{Message: m1, Score: time.Now().Unix()},
				{Message: m2, Score: time.Now().Unix()},
			},
		},
		{
			retry: []base.Z{
				{Message: m1, Score: t1.Unix()},
			},
			dead: []base.Z{
				{Message: m2, Score: t2.Unix()},
			},
			want:      1,
			wantRetry: []base.Z{},
			wantDead: []base.Z{
				{Message: m1, Score: time.Now().Unix()},
				{Message: m2, Score: t2.Unix()},
			},
		},
		{
			retry: []base.Z{},
			dead: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			want:      0,
			wantRetry: []base.Z{},
			wantDead: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
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
		scheduled     []base.Z
		dead          []base.Z
		want          int64
		wantScheduled []base.Z
		wantDead      []base.Z
	}{
		{
			scheduled: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			dead:          []base.Z{},
			want:          2,
			wantScheduled: []base.Z{},
			wantDead: []base.Z{
				{Message: m1, Score: time.Now().Unix()},
				{Message: m2, Score: time.Now().Unix()},
			},
		},
		{
			scheduled: []base.Z{
				{Message: m1, Score: t1.Unix()},
			},
			dead: []base.Z{
				{Message: m2, Score: t2.Unix()},
			},
			want:          1,
			wantScheduled: []base.Z{},
			wantDead: []base.Z{
				{Message: m1, Score: time.Now().Unix()},
				{Message: m2, Score: t2.Unix()},
			},
		},
		{
			scheduled: []base.Z{},
			dead: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			want:          0,
			wantScheduled: []base.Z{},
			wantDead: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
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
		dead     []base.Z
		id       uuid.UUID
		score    int64
		want     error
		wantDead []*base.TaskMessage
	}{
		{
			dead: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			id:       m1.ID,
			score:    t1.Unix(),
			want:     nil,
			wantDead: []*base.TaskMessage{m2},
		},
		{
			dead: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			id:       m1.ID,
			score:    t2.Unix(), // id and score mismatch
			want:     ErrTaskNotFound,
			wantDead: []*base.TaskMessage{m1, m2},
		},
		{
			dead:     []base.Z{},
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
		retry     []base.Z
		id        uuid.UUID
		score     int64
		want      error
		wantRetry []*base.TaskMessage
	}{
		{
			retry: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			id:        m1.ID,
			score:     t1.Unix(),
			want:      nil,
			wantRetry: []*base.TaskMessage{m2},
		},
		{
			retry: []base.Z{
				{Message: m1, Score: t1.Unix()},
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
		scheduled     []base.Z
		id            uuid.UUID
		score         int64
		want          error
		wantScheduled []*base.TaskMessage
	}{
		{
			scheduled: []base.Z{
				{Message: m1, Score: t1.Unix()},
				{Message: m2, Score: t2.Unix()},
			},
			id:            m1.ID,
			score:         t1.Unix(),
			want:          nil,
			wantScheduled: []*base.TaskMessage{m2},
		},
		{
			scheduled: []base.Z{
				{Message: m1, Score: t1.Unix()},
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
		dead     []base.Z
		want     int64
		wantDead []*base.TaskMessage
	}{
		{
			dead: []base.Z{
				{Message: m1, Score: time.Now().Unix()},
				{Message: m2, Score: time.Now().Unix()},
				{Message: m3, Score: time.Now().Unix()},
			},
			want:     3,
			wantDead: []*base.TaskMessage{},
		},
		{
			dead:     []base.Z{},
			want:     0,
			wantDead: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedDeadQueue(t, r.client, tc.dead)

		got, err := r.DeleteAllDeadTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeadTasks returned error: %v", err)
		}
		if got != tc.want {
			t.Errorf("r.DeleteAllDeadTasks() = %d, nil, want %d, nil", got, tc.want)
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
		retry     []base.Z
		want      int64
		wantRetry []*base.TaskMessage
	}{
		{
			retry: []base.Z{
				{Message: m1, Score: time.Now().Unix()},
				{Message: m2, Score: time.Now().Unix()},
				{Message: m3, Score: time.Now().Unix()},
			},
			want:      3,
			wantRetry: []*base.TaskMessage{},
		},
		{
			retry:     []base.Z{},
			want:      0,
			wantRetry: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedRetryQueue(t, r.client, tc.retry)

		got, err := r.DeleteAllRetryTasks()
		if err != nil {
			t.Errorf("r.DeleteAllRetryTasks returned error: %v", err)
		}
		if got != tc.want {
			t.Errorf("r.DeleteAllRetryTasks() = %d, nil, want %d, nil", got, tc.want)
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
		scheduled     []base.Z
		want          int64
		wantScheduled []*base.TaskMessage
	}{
		{
			scheduled: []base.Z{
				{Message: m1, Score: time.Now().Add(time.Minute).Unix()},
				{Message: m2, Score: time.Now().Add(time.Minute).Unix()},
				{Message: m3, Score: time.Now().Add(time.Minute).Unix()},
			},
			want:          3,
			wantScheduled: []*base.TaskMessage{},
		},
		{
			scheduled:     []base.Z{},
			want:          0,
			wantScheduled: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedScheduledQueue(t, r.client, tc.scheduled)

		got, err := r.DeleteAllScheduledTasks()
		if err != nil {
			t.Errorf("r.DeleteAllScheduledTasks returned error: %v", err)
		}
		if got != tc.want {
			t.Errorf("r.DeleteAllScheduledTasks() = %d, nil, want %d, nil", got, tc.want)
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
