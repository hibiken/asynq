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
	"github.com/hibiken/asynq/internal/errors"
)

func TestAllQueues(t *testing.T) {
	r := setup(t)
	defer r.Close()

	tests := []struct {
		queues []string
	}{
		{queues: []string{"default"}},
		{queues: []string{"custom1", "custom2"}},
		{queues: []string{"default", "custom1", "custom2"}},
		{queues: []string{}},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		for _, qname := range tc.queues {
			if err := r.client.SAdd(base.AllQueues, qname).Err(); err != nil {
				t.Fatalf("could not initialize all queue set: %v", err)
			}
		}
		got, err := r.AllQueues()
		if err != nil {
			t.Errorf("AllQueues() returned an error: %v", err)
			continue
		}
		if diff := cmp.Diff(tc.queues, got, h.SortStringSliceOpt); diff != "" {
			t.Errorf("AllQueues() = %v, want %v; (-want, +got)\n%s", got, tc.queues, diff)
		}
	}
}

func TestCurrentStats(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"subject": "hello"}))
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessage("gen_thumbnail", h.JSON(map[string]interface{}{"src": "some/path/to/img"}))
	m4 := h.NewTaskMessage("sync", nil)
	m5 := h.NewTaskMessageWithQueue("important_notification", nil, "critical")
	m6 := h.NewTaskMessageWithQueue("minor_notification", nil, "low")
	now := time.Now()

	tests := []struct {
		pending    map[string][]*base.TaskMessage
		inProgress map[string][]*base.TaskMessage
		scheduled  map[string][]base.Z
		retry      map[string][]base.Z
		archived   map[string][]base.Z
		processed  map[string]int
		failed     map[string]int
		paused     []string
		qname      string
		want       *Stats
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m5},
				"low":      {m6},
			},
			inProgress: map[string][]*base.TaskMessage{
				"default":  {m2},
				"critical": {},
				"low":      {},
			},
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m3, Score: now.Add(time.Hour).Unix()},
					{Message: m4, Score: now.Unix()},
				},
				"critical": {},
				"low":      {},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			archived: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			processed: map[string]int{
				"default":  120,
				"critical": 100,
				"low":      50,
			},
			failed: map[string]int{
				"default":  2,
				"critical": 0,
				"low":      1,
			},
			paused: []string{},
			qname:  "default",
			want: &Stats{
				Queue:     "default",
				Paused:    false,
				Size:      4,
				Pending:   1,
				Active:    1,
				Scheduled: 2,
				Retry:     0,
				Archived:  0,
				Processed: 120,
				Failed:    2,
				Timestamp: now,
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m5},
				"low":      {m6},
			},
			inProgress: map[string][]*base.TaskMessage{
				"default":  {m2},
				"critical": {},
				"low":      {},
			},
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m3, Score: now.Add(time.Hour).Unix()},
					{Message: m4, Score: now.Unix()},
				},
				"critical": {},
				"low":      {},
			},
			retry: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			archived: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			processed: map[string]int{
				"default":  120,
				"critical": 100,
				"low":      50,
			},
			failed: map[string]int{
				"default":  2,
				"critical": 0,
				"low":      1,
			},
			paused: []string{"critical", "low"},
			qname:  "critical",
			want: &Stats{
				Queue:     "critical",
				Paused:    true,
				Size:      1,
				Pending:   1,
				Active:    0,
				Scheduled: 0,
				Retry:     0,
				Archived:  0,
				Processed: 100,
				Failed:    0,
				Timestamp: now,
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
		h.SeedAllPendingQueues(t, r.client, tc.pending)
		h.SeedAllActiveQueues(t, r.client, tc.inProgress)
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)
		h.SeedAllRetryQueues(t, r.client, tc.retry)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)
		for qname, n := range tc.processed {
			processedKey := base.ProcessedKey(qname, now)
			r.client.Set(processedKey, n, 0)
		}
		for qname, n := range tc.failed {
			failedKey := base.FailedKey(qname, now)
			r.client.Set(failedKey, n, 0)
		}

		got, err := r.CurrentStats(tc.qname)
		if err != nil {
			t.Errorf("r.CurrentStats(%q) = %v, %v, want %v, nil", tc.qname, got, err, tc.want)
			continue
		}

		ignoreMemUsg := cmpopts.IgnoreFields(Stats{}, "MemoryUsage")
		if diff := cmp.Diff(tc.want, got, timeCmpOpt, ignoreMemUsg); diff != "" {
			t.Errorf("r.CurrentStats(%q) = %v, %v, want %v, nil; (-want, +got)\n%s", tc.qname, got, err, tc.want, diff)
			continue
		}
	}
}

func TestCurrentStatsWithNonExistentQueue(t *testing.T) {
	r := setup(t)
	defer r.Close()

	qname := "non-existent"
	got, err := r.CurrentStats(qname)
	if err == nil {
		t.Fatalf("r.CurrentStats(%q) = %v, %v, want nil, %v", qname, got, err, &QueueNotFoundError{qname})
	}
}

func TestHistoricalStats(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now().UTC()

	tests := []struct {
		qname string // queue of interest
		n     int    // number of days
	}{
		{"default", 90},
		{"custom", 7},
		{"default", 1},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		r.client.SAdd(base.AllQueues, tc.qname)
		// populate last n days data
		for i := 0; i < tc.n; i++ {
			ts := now.Add(-time.Duration(i) * 24 * time.Hour)
			processedKey := base.ProcessedKey(tc.qname, ts)
			failedKey := base.FailedKey(tc.qname, ts)
			r.client.Set(processedKey, (i+1)*1000, 0)
			r.client.Set(failedKey, (i+1)*10, 0)
		}

		got, err := r.HistoricalStats(tc.qname, tc.n)
		if err != nil {
			t.Errorf("RDB.HistoricalStats(%q, %d) returned error: %v", tc.qname, tc.n, err)
			continue
		}

		if len(got) != tc.n {
			t.Errorf("RDB.HistorycalStats(%q, %d) returned %d daily stats, want %d", tc.qname, tc.n, len(got), tc.n)
			continue
		}

		for i := 0; i < tc.n; i++ {
			want := &DailyStats{
				Queue:     tc.qname,
				Processed: (i + 1) * 1000,
				Failed:    (i + 1) * 10,
				Time:      now.Add(-time.Duration(i) * 24 * time.Hour),
			}
			// Allow 2 seconds difference in timestamp.
			cmpOpt := cmpopts.EquateApproxTime(2 * time.Second)
			if diff := cmp.Diff(want, got[i], cmpOpt); diff != "" {
				t.Errorf("RDB.HistoricalStats for the last %d days; got %+v, want %+v; (-want,+got):\n%s", i, got[i], want, diff)
			}
		}
	}

}

func TestRedisInfo(t *testing.T) {
	r := setup(t)
	defer r.Close()

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

func TestListPending(t *testing.T) {
	r := setup(t)
	defer r.Close()

	m1 := h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"subject": "hello"}))
	m2 := h.NewTaskMessage("reindex", nil)
	m3 := h.NewTaskMessageWithQueue("important_notification", nil, "critical")
	m4 := h.NewTaskMessageWithQueue("minor_notification", nil, "low")

	tests := []struct {
		pending map[string][]*base.TaskMessage
		qname   string
		want    []*base.TaskMessage
	}{
		{
			pending: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1, m2},
			},
			qname: base.DefaultQueueName,
			want:  []*base.TaskMessage{m1, m2},
		},
		{
			pending: map[string][]*base.TaskMessage{
				base.DefaultQueueName: nil,
			},
			qname: base.DefaultQueueName,
			want:  []*base.TaskMessage(nil),
		},
		{
			pending: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m1, m2},
				"critical":            {m3},
				"low":                 {m4},
			},
			qname: base.DefaultQueueName,
			want:  []*base.TaskMessage{m1, m2},
		},
		{
			pending: map[string][]*base.TaskMessage{
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
		h.SeedAllPendingQueues(t, r.client, tc.pending)

		got, err := r.ListPending(tc.qname, Pagination{Size: 20, Page: 0})
		op := fmt.Sprintf("r.ListPending(%q, Pagination{Size: 20, Page: 0})", tc.qname)
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

func TestListPendingPagination(t *testing.T) {
	r := setup(t)
	defer r.Close()
	var msgs []*base.TaskMessage
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		msgs = append(msgs, msg)
	}
	// create 100 tasks in default queue
	h.SeedPendingQueue(t, r.client, msgs, "default")

	msgs = []*base.TaskMessage(nil) // empty list
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessageWithQueue(fmt.Sprintf("custom %d", i), nil, "custom")
		msgs = append(msgs, msg)
	}
	// create 100 tasks in custom queue
	h.SeedPendingQueue(t, r.client, msgs, "custom")

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
		got, err := r.ListPending(tc.qname, Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListPending(%q, Pagination{Size: %d, Page: %d})", tc.qname, tc.size, tc.page)
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

func TestListActive(t *testing.T) {
	r := setup(t)
	defer r.Close()

	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task2", nil, "critical")
	m4 := h.NewTaskMessageWithQueue("task2", nil, "low")

	tests := []struct {
		inProgress map[string][]*base.TaskMessage
		qname      string
		want       []*base.TaskMessage
	}{
		{
			inProgress: map[string][]*base.TaskMessage{
				"default":  {m1, m2},
				"critical": {m3},
				"low":      {m4},
			},
			qname: "default",
			want:  []*base.TaskMessage{m1, m2},
		},
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {},
			},
			qname: "default",
			want:  []*base.TaskMessage(nil),
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllActiveQueues(t, r.client, tc.inProgress)

		got, err := r.ListActive(tc.qname, Pagination{Size: 20, Page: 0})
		op := fmt.Sprintf("r.ListActive(%q, Pagination{Size: 20, Page: 0})", tc.qname)
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.inProgress)
			continue
		}
		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s", op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListActivePagination(t *testing.T) {
	r := setup(t)
	defer r.Close()
	var msgs []*base.TaskMessage
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		msgs = append(msgs, msg)
	}
	h.SeedActiveQueue(t, r.client, msgs, "default")

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
	}

	for _, tc := range tests {
		got, err := r.ListActive(tc.qname, Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListActive(%q, Pagination{Size: %d, Page: %d})", tc.qname, tc.size, tc.page)
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
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessage("task3", nil)
	m4 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	p1 := time.Now().Add(30 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	p3 := time.Now().Add(5 * time.Minute)
	p4 := time.Now().Add(2 * time.Minute)

	tests := []struct {
		scheduled map[string][]base.Z
		qname     string
		want      []base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: p1.Unix()},
					{Message: m2, Score: p2.Unix()},
					{Message: m3, Score: p3.Unix()},
				},
				"custom": {
					{Message: m4, Score: p4.Unix()},
				},
			},
			qname: "default",
			// should be sorted by score in ascending order
			want: []base.Z{
				{Message: m3, Score: p3.Unix()},
				{Message: m1, Score: p1.Unix()},
				{Message: m2, Score: p2.Unix()},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: p1.Unix()},
					{Message: m2, Score: p2.Unix()},
					{Message: m3, Score: p3.Unix()},
				},
				"custom": {
					{Message: m4, Score: p4.Unix()},
				},
			},
			qname: "custom",
			want: []base.Z{
				{Message: m4, Score: p4.Unix()},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []base.Z(nil),
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)

		got, err := r.ListScheduled(tc.qname, Pagination{Size: 20, Page: 0})
		op := fmt.Sprintf("r.ListScheduled(%q, Pagination{Size: 20, Page: 0})", tc.qname)
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.want)
			continue
		}
		if diff := cmp.Diff(tc.want, got, zScoreCmpOpt); diff != "" {
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s", op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListScheduledPagination(t *testing.T) {
	r := setup(t)
	defer r.Close()
	// create 100 tasks with an increasing number of wait time.
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		if err := r.Schedule(msg, time.Now().Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}

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
	}

	for _, tc := range tests {
		got, err := r.ListScheduled(tc.qname, Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListScheduled(%q, Pagination{Size: %d, Page: %d})", tc.qname, tc.size, tc.page)
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
	defer r.Close()
	m1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "task1",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "some error occurred",
		Retry:    25,
		Retried:  10,
	}
	m2 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "task2",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "some error occurred",
		Retry:    25,
		Retried:  2,
	}
	m3 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "task3",
		Queue:    "custom",
		Payload:  nil,
		ErrorMsg: "some error occurred",
		Retry:    25,
		Retried:  3,
	}
	p1 := time.Now().Add(5 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	p3 := time.Now().Add(24 * time.Hour)

	tests := []struct {
		retry map[string][]base.Z
		qname string
		want  []base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: p1.Unix()},
					{Message: m2, Score: p2.Unix()},
				},
				"custom": {
					{Message: m3, Score: p3.Unix()},
				},
			},
			qname: "default",
			want: []base.Z{
				{Message: m1, Score: p1.Unix()},
				{Message: m2, Score: p2.Unix()},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: p1.Unix()},
					{Message: m2, Score: p2.Unix()},
				},
				"custom": {
					{Message: m3, Score: p3.Unix()},
				},
			},
			qname: "custom",
			want: []base.Z{
				{Message: m3, Score: p3.Unix()},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []base.Z(nil),
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllRetryQueues(t, r.client, tc.retry)

		got, err := r.ListRetry(tc.qname, Pagination{Size: 20, Page: 0})
		op := fmt.Sprintf("r.ListRetry(%q, Pagination{Size: 20, Page: 0})", tc.qname)
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.want)
			continue
		}
		if diff := cmp.Diff(tc.want, got, zScoreCmpOpt); diff != "" {
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s",
				op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListRetryPagination(t *testing.T) {
	r := setup(t)
	defer r.Close()
	// create 100 tasks with an increasing number of wait time.
	now := time.Now()
	var seed []base.Z
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		processAt := now.Add(time.Duration(i) * time.Second)
		seed = append(seed, base.Z{Message: msg, Score: processAt.Unix()})
	}
	h.SeedRetryQueue(t, r.client, seed, "default")

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
	}

	for _, tc := range tests {
		got, err := r.ListRetry(tc.qname, Pagination{Size: tc.size, Page: tc.page})
		op := fmt.Sprintf("r.ListRetry(%q, Pagination{Size: %d, Page: %d})",
			tc.qname, tc.size, tc.page)
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

func TestListArchived(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "task1",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "some error occurred",
	}
	m2 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "task2",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "some error occurred",
	}
	m3 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "task3",
		Queue:    "custom",
		Payload:  nil,
		ErrorMsg: "some error occurred",
	}
	f1 := time.Now().Add(-5 * time.Minute)
	f2 := time.Now().Add(-24 * time.Hour)
	f3 := time.Now().Add(-4 * time.Hour)

	tests := []struct {
		archived map[string][]base.Z
		qname    string
		want     []base.Z
	}{
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: f1.Unix()},
					{Message: m2, Score: f2.Unix()},
				},
				"custom": {
					{Message: m3, Score: f3.Unix()},
				},
			},
			qname: "default",
			want: []base.Z{
				{Message: m2, Score: f2.Unix()}, // FIXME: shouldn't be sorted in the other order?
				{Message: m1, Score: f1.Unix()},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: f1.Unix()},
					{Message: m2, Score: f2.Unix()},
				},
				"custom": {
					{Message: m3, Score: f3.Unix()},
				},
			},
			qname: "custom",
			want: []base.Z{
				{Message: m3, Score: f3.Unix()},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []base.Z(nil),
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got, err := r.ListArchived(tc.qname, Pagination{Size: 20, Page: 0})
		op := fmt.Sprintf("r.ListDead(%q, Pagination{Size: 20, Page: 0})", tc.qname)
		if err != nil {
			t.Errorf("%s = %v, %v, want %v, nil", op, got, err, tc.want)
			continue
		}
		if diff := cmp.Diff(tc.want, got, zScoreCmpOpt); diff != "" {
			t.Errorf("%s = %v, %v, want %v, nil; (-want, +got)\n%s",
				op, got, err, tc.want, diff)
			continue
		}
	}
}

func TestListArchivedPagination(t *testing.T) {
	r := setup(t)
	defer r.Close()
	var entries []base.Z
	for i := 0; i < 100; i++ {
		msg := h.NewTaskMessage(fmt.Sprintf("task %d", i), nil)
		entries = append(entries, base.Z{Message: msg, Score: int64(i)})
	}
	h.SeedArchivedQueue(t, r.client, entries, "default")

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
	}

	for _, tc := range tests {
		got, err := r.ListArchived(tc.qname, Pagination{Size: tc.size, Page: tc.page})
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

var (
	timeCmpOpt   = cmpopts.EquateApproxTime(2 * time.Second) // allow for 2 seconds margin in time.Time
	zScoreCmpOpt = h.EquateInt64Approx(2)                    // allow for 2 seconds margin in Z.Score
)

func TestRunArchivedTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessageWithQueue("send_notification", nil, "critical")
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		archived     map[string][]base.Z
		qname        string
		id           uuid.UUID
		want         error // expected return value from calling RunArchivedTask
		wantArchived map[string][]*base.TaskMessage
		wantPending  map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
			},
			qname: "default",
			id:    t2.ID,
			want:  nil,
			wantArchived: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {t2},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
			},
			qname: "default",
			id:    uuid.New(),
			want:  ErrTaskNotFound,
			wantArchived: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
				"critical": {
					{Message: t3, Score: s1},
				},
			},
			qname: "critical",
			id:    t3.ID,
			want:  nil,
			wantArchived: map[string][]*base.TaskMessage{
				"default":  {t1, t2},
				"critical": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t3},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got := r.RunTask(tc.qname, tc.id)
		if got != tc.want {
			t.Errorf("r.RunTask(%q, %s) = %v, want %v", tc.qname, tc.id, got, tc.want)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestRunRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()

	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessageWithQueue("send_notification", nil, "low")
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()
	tests := []struct {
		retry       map[string][]base.Z
		qname       string
		id          uuid.UUID
		want        error // expected return value from calling RunRetryTask
		wantRetry   map[string][]*base.TaskMessage
		wantPending map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
			},
			qname: "default",
			id:    t2.ID,
			want:  nil,
			wantRetry: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {t2},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
			},
			qname: "default",
			id:    uuid.New(),
			want:  ErrTaskNotFound,
			wantRetry: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
				"low": {
					{Message: t3, Score: s2},
				},
			},
			qname: "low",
			id:    t3.ID,
			want:  nil,
			wantRetry: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"low":     {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"low":     {t3},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)                      // clean up db before each test case
		h.SeedAllRetryQueues(t, r.client, tc.retry) // initialize retry queue

		got := r.RunTask(tc.qname, tc.id)
		if got != tc.want {
			t.Errorf("r.RunTask(%q, %s) = %v, want %v", tc.qname, tc.id, got, tc.want)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}

		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.RetryKey(qname), diff)
			}
		}
	}
}

func TestRunScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessageWithQueue("send_notification", nil, "notifications")
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		id            uuid.UUID
		want          error // expected return value from calling RunScheduledTask
		wantScheduled map[string][]*base.TaskMessage
		wantPending   map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
			},
			qname: "default",
			id:    t2.ID,
			want:  nil,
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {t2},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
			},
			qname: "default",
			id:    uuid.New(),
			want:  ErrTaskNotFound,
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: s1},
					{Message: t2, Score: s2},
				},
				"notifications": {
					{Message: t3, Score: s1},
				},
			},
			qname: "notifications",
			id:    t3.ID,
			want:  nil,
			wantScheduled: map[string][]*base.TaskMessage{
				"default":       {t1, t2},
				"notifications": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":       {},
				"notifications": {t3},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)

		got := r.RunTask(tc.qname, tc.id)
		if got != tc.want {
			t.Errorf("r.RunTask(%q, %s) = %v, want %v", tc.qname, tc.id, got, tc.want)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}

		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.ScheduledKey(qname), diff)
			}
		}
	}
}

func TestRunAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("reindex", nil)
	t4 := h.NewTaskMessageWithQueue("important_notification", nil, "custom")
	t5 := h.NewTaskMessageWithQueue("minor_notification", nil, "custom")

	tests := []struct {
		desc          string
		scheduled     map[string][]base.Z
		qname         string
		want          int64
		wantPending   map[string][]*base.TaskMessage
		wantScheduled map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in scheduled queue",
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t2, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t3, Score: time.Now().Add(time.Hour).Unix()},
				},
			},
			qname: "default",
			want:  3,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			desc: "with empty scheduled queue",
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			desc: "with custom queues",
			scheduled: map[string][]base.Z{
				"default": {
					{Message: t1, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t2, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t3, Score: time.Now().Add(time.Hour).Unix()},
				},
				"custom": {
					{Message: t4, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t5, Score: time.Now().Add(time.Hour).Unix()},
				},
			},
			qname: "custom",
			want:  2,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {t4, t5},
			},
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)

		got, err := r.RunAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; r.RunAllScheduledTasks(%q) = %v, %v; want %v, nil",
				tc.desc, tc.qname, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.RunAllScheduledTasks(%q) = %v, %v; want %v, nil",
				tc.desc, tc.qname, got, err, tc.want)
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.ScheduledKey(qname), diff)
			}
		}
	}
}

func TestRunAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("reindex", nil)
	t4 := h.NewTaskMessageWithQueue("important_notification", nil, "custom")
	t5 := h.NewTaskMessageWithQueue("minor_notification", nil, "custom")

	tests := []struct {
		desc        string
		retry       map[string][]base.Z
		qname       string
		want        int64
		wantPending map[string][]*base.TaskMessage
		wantRetry   map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in retry queue",
			retry: map[string][]base.Z{
				"default": {
					{Message: t1, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t2, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t3, Score: time.Now().Add(time.Hour).Unix()},
				},
			},
			qname: "default",
			want:  3,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			desc: "with empty retry queue",
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			desc: "with custom queues",
			retry: map[string][]base.Z{
				"default": {
					{Message: t1, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t2, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t3, Score: time.Now().Add(time.Hour).Unix()},
				},
				"custom": {
					{Message: t4, Score: time.Now().Add(time.Hour).Unix()},
					{Message: t5, Score: time.Now().Add(time.Hour).Unix()},
				},
			},
			qname: "custom",
			want:  2,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {t4, t5},
			},
			wantRetry: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllRetryQueues(t, r.client, tc.retry)

		got, err := r.RunAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; r.RunAllRetryTasks(%q) = %v, %v; want %v, nil",
				tc.desc, tc.qname, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.RunAllRetryTasks(%q) = %v, %v; want %v, nil",
				tc.desc, tc.qname, got, err, tc.want)
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.RetryKey(qname), diff)
			}
		}
	}
}

func TestRunAllArchivedTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("gen_thumbnail", nil)
	t3 := h.NewTaskMessage("reindex", nil)
	t4 := h.NewTaskMessageWithQueue("important_notification", nil, "custom")
	t5 := h.NewTaskMessageWithQueue("minor_notification", nil, "custom")

	tests := []struct {
		desc         string
		archived     map[string][]base.Z
		qname        string
		want         int64
		wantPending  map[string][]*base.TaskMessage
		wantArchived map[string][]*base.TaskMessage
	}{
		{
			desc: "with tasks in archived queue",
			archived: map[string][]base.Z{
				"default": {
					{Message: t1, Score: time.Now().Add(-time.Minute).Unix()},
					{Message: t2, Score: time.Now().Add(-time.Minute).Unix()},
					{Message: t3, Score: time.Now().Add(-time.Minute).Unix()},
				},
			},
			qname: "default",
			want:  3,
			wantPending: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			wantArchived: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			desc: "with empty archived queue",
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantArchived: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			desc: "with custom queues",
			archived: map[string][]base.Z{
				"default": {
					{Message: t1, Score: time.Now().Add(-time.Minute).Unix()},
					{Message: t2, Score: time.Now().Add(-time.Minute).Unix()},
					{Message: t3, Score: time.Now().Add(-time.Minute).Unix()},
				},
				"custom": {
					{Message: t4, Score: time.Now().Add(-time.Minute).Unix()},
					{Message: t5, Score: time.Now().Add(-time.Minute).Unix()},
				},
			},
			qname: "custom",
			want:  2,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {t4, t5},
			},
			wantArchived: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got, err := r.RunAllArchivedTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; r.RunAllDeadTasks(%q) = %v, %v; want %v, nil",
				tc.desc, tc.qname, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.RunAllDeadTasks(%q) = %v, %v; want %v, nil",
				tc.desc, tc.qname, got, err, tc.want)
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortMsgOpt); diff != "" {
				t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestArchiveRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	t1 := time.Now().Add(1 * time.Minute)
	t2 := time.Now().Add(1 * time.Hour)
	t3 := time.Now().Add(2 * time.Hour)
	t4 := time.Now().Add(3 * time.Hour)

	tests := []struct {
		retry        map[string][]base.Z
		archived     map[string][]base.Z
		qname        string
		id           uuid.UUID
		wantRetry    map[string][]base.Z
		wantArchived map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			id:    m1.ID,
			wantRetry: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
			wantArchived: map[string][]base.Z{
				"default": {{Message: m1, Score: time.Now().Unix()}},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m3, Score: t3.Unix()},
					{Message: m4, Score: t4.Unix()},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			id:    m3.ID,
			wantRetry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m4, Score: t4.Unix()},
				},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: m3, Score: time.Now().Unix()}},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllRetryQueues(t, r.client, tc.retry)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		if got := r.ArchiveTask(tc.qname, tc.id); got != nil {
			t.Errorf("(*RDB).ArchiveTask(%q, %v) returned error: %v",
				tc.qname, tc.id, got)
			continue
		}

		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.RetryKey(qname), diff)
			}
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestArchiveScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	t1 := time.Now().Add(1 * time.Minute)
	t2 := time.Now().Add(1 * time.Hour)
	t3 := time.Now().Add(2 * time.Hour)
	t4 := time.Now().Add(3 * time.Hour)

	tests := []struct {
		scheduled     map[string][]base.Z
		archived      map[string][]base.Z
		qname         string
		id            uuid.UUID
		wantScheduled map[string][]base.Z
		wantArchived  map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			id:    m1.ID,
			wantScheduled: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
			wantArchived: map[string][]base.Z{
				"default": {{Message: m1, Score: time.Now().Unix()}},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m3, Score: t3.Unix()},
					{Message: m4, Score: t4.Unix()},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			id:    m3.ID,
			wantScheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m4, Score: t4.Unix()},
				},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: m3, Score: time.Now().Unix()}},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		if got := r.ArchiveTask(tc.qname, tc.id); got != nil {
			t.Errorf("(*RDB).ArchiveTask(%q, %v) returned error: %v",
				tc.qname, tc.id, got)
			continue
		}

		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ScheduledKey(qname), diff)
			}
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestArchivePendingTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	tests := []struct {
		pending      map[string][]*base.TaskMessage
		archived     map[string][]base.Z
		qname        string
		id           uuid.UUID
		wantPending  map[string][]*base.TaskMessage
		wantArchived map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			id:    m1.ID,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m2},
			},
			wantArchived: map[string][]base.Z{
				"default": {{Message: m1, Score: time.Now().Unix()}},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3, m4},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			id:    m3.ID,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m4},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: m3, Score: time.Now().Unix()}},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllPendingQueues(t, r.client, tc.pending)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		if got := r.ArchiveTask(tc.qname, tc.id); got != nil {
			t.Errorf("(*RDB).ArchiveTask(%q, %v) returned error: %v",
				tc.qname, tc.id, got)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.PendingKey(qname), diff)
			}
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestArchiveTaskError(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	t1 := time.Now().Add(1 * time.Minute)
	t2 := time.Now().Add(1 * time.Hour)

	tests := []struct {
		desc          string
		scheduled     map[string][]base.Z
		archived      map[string][]base.Z
		qname         string
		id            uuid.UUID
		match         func(err error) bool
		wantScheduled map[string][]base.Z
		wantArchived  map[string][]base.Z
	}{
		{
			desc: "It should return QueueNotFoundError if provided queue name doesn't exist",
			scheduled: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			archived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
			qname: "nonexistent",
			id:    m2.ID,
			match: errors.IsQueueNotFound,
			wantScheduled: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			wantArchived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
		},
		{
			desc: "It should return TaskNotFoundError if provided task ID doesn't exist in the queue",
			scheduled: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			archived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
			qname: "default",
			id:    uuid.New(),
			match: errors.IsTaskNotFound,
			wantScheduled: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			wantArchived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
		},
		{
			desc: "It should return TaskAlreadyArchivedError if task is already in archived state",
			scheduled: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			archived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
			qname: "default",
			id:    m2.ID,
			match: errors.IsTaskAlreadyArchived,
			wantScheduled: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			wantArchived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got := r.ArchiveTask(tc.qname, tc.id)
		if !tc.match(got) {
			t.Errorf("%s: returned error didn't match: got=%v", tc.desc, got)
			continue
		}

		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ScheduledKey(qname), diff)
			}
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ArchivedKey(qname), diff)
			}
		}
	}
}
func TestArchiveAllPendingTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	t1 := time.Now().Add(1 * time.Minute)
	t2 := time.Now().Add(1 * time.Hour)

	tests := []struct {
		pending      map[string][]*base.TaskMessage
		archived     map[string][]base.Z
		qname        string
		want         int64
		wantPending  map[string][]*base.TaskMessage
		wantArchived map[string][]base.Z
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  2,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Unix()},
					{Message: m2, Score: time.Now().Unix()},
				},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1},
			},
			archived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
			qname: "default",
			want:  1,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			qname: "default",
			want:  0,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3, m4},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			want:  2,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{Message: m3, Score: time.Now().Unix()},
					{Message: m4, Score: time.Now().Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllPendingQueues(t, r.client, tc.pending)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got, err := r.ArchiveAllPendingTasks(tc.qname)
		if got != tc.want || err != nil {
			t.Errorf("(*RDB).KillAllRetryTasks(%q) = %v, %v; want %v, nil",
				tc.qname, got, err, tc.want)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.PendingKey(qname), diff)
			}
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ArchivedKey(qname), diff)
			}
		}
	}
}
func TestArchiveAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	t1 := time.Now().Add(1 * time.Minute)
	t2 := time.Now().Add(1 * time.Hour)
	t3 := time.Now().Add(2 * time.Hour)
	t4 := time.Now().Add(3 * time.Hour)

	tests := []struct {
		retry        map[string][]base.Z
		archived     map[string][]base.Z
		qname        string
		want         int64
		wantRetry    map[string][]base.Z
		wantArchived map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  2,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Unix()},
					{Message: m2, Score: time.Now().Unix()},
				},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			archived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
			qname: "default",
			want:  1,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m3, Score: t3.Unix()},
					{Message: m4, Score: t4.Unix()},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			want:  2,
			wantRetry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{Message: m3, Score: time.Now().Unix()},
					{Message: m4, Score: time.Now().Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllRetryQueues(t, r.client, tc.retry)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got, err := r.ArchiveAllRetryTasks(tc.qname)
		if got != tc.want || err != nil {
			t.Errorf("(*RDB).KillAllRetryTasks(%q) = %v, %v; want %v, nil",
				tc.qname, got, err, tc.want)
			continue
		}

		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.RetryKey(qname), diff)
			}
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestArchiveAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")
	t1 := time.Now().Add(time.Minute)
	t2 := time.Now().Add(time.Hour)
	t3 := time.Now().Add(time.Hour)
	t4 := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     map[string][]base.Z
		archived      map[string][]base.Z
		qname         string
		want          int64
		wantScheduled map[string][]base.Z
		wantArchived  map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  2,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Unix()},
					{Message: m2, Score: time.Now().Unix()},
				},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			archived: map[string][]base.Z{
				"default": {{Message: m2, Score: t2.Unix()}},
			},
			qname: "default",
			want:  1,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m3, Score: t3.Unix()},
					{Message: m4, Score: t4.Unix()},
				},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			want:  2,
			wantScheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {},
			},
			wantArchived: map[string][]base.Z{
				"default": {},
				"custom": {
					{Message: m3, Score: time.Now().Unix()},
					{Message: m4, Score: time.Now().Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got, err := r.ArchiveAllScheduledTasks(tc.qname)
		if got != tc.want || err != nil {
			t.Errorf("(*RDB).KillAllScheduledTasks(%q) = %v, %v; want %v, nil",
				tc.qname, got, err, tc.want)
			continue
		}

		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ScheduledKey(qname), diff)
			}
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt, zScoreCmpOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want,+got)\n%s",
					base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestDeleteArchivedTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	t1 := time.Now().Add(-5 * time.Minute)
	t2 := time.Now().Add(-time.Hour)
	t3 := time.Now().Add(-time.Hour)

	tests := []struct {
		archived     map[string][]base.Z
		qname        string
		id           uuid.UUID
		want         error
		wantArchived map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			qname: "default",
			id:    m1.ID,
			want:  nil,
			wantArchived: map[string][]*base.TaskMessage{
				"default": {m2},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m3, Score: t3.Unix()},
				},
			},
			qname: "custom",
			id:    m3.ID,
			want:  nil,
			wantArchived: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			qname: "default",
			id:    uuid.New(),
			want:  ErrTaskNotFound,
			wantArchived: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			id:    m1.ID,
			want:  ErrTaskNotFound,
			wantArchived: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got := r.DeleteArchivedTask(tc.qname, tc.id)
		if got != tc.want {
			t.Errorf("r.DeleteArchivedTask(%q, %v) = %v, want %v", tc.qname, tc.id, got, tc.want)
			continue
		}

		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestDeleteRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)
	t3 := time.Now().Add(time.Hour)

	tests := []struct {
		retry     map[string][]base.Z
		qname     string
		id        uuid.UUID
		want      error
		wantRetry map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			qname: "default",
			id:    m1.ID,
			want:  nil,
			wantRetry: map[string][]*base.TaskMessage{
				"default": {m2},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m3, Score: t3.Unix()},
				},
			},
			qname: "custom",
			id:    m3.ID,
			want:  nil,
			wantRetry: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			qname: "default",
			id:    uuid.New(),
			want:  ErrTaskNotFound,
			wantRetry: map[string][]*base.TaskMessage{
				"default": {m1},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllRetryQueues(t, r.client, tc.retry)

		got := r.DeleteRetryTask(tc.qname, tc.id)
		if got != tc.want {
			t.Errorf("r.DeleteRetryTask(%q, %v) = %v, want %v", tc.qname, tc.id, got, tc.want)
			continue
		}

		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(qname), diff)
			}
		}
	}
}

func TestDeleteScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)
	t3 := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		id            uuid.UUID
		want          error
		wantScheduled map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
			},
			qname: "default",
			id:    m1.ID,
			want:  nil,
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {m2},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: t1.Unix()},
					{Message: m2, Score: t2.Unix()},
				},
				"custom": {
					{Message: m3, Score: t3.Unix()},
				},
			},
			qname: "custom",
			id:    m3.ID,
			want:  nil,
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {{Message: m1, Score: t1.Unix()}},
			},
			qname: "default",
			id:    uuid.New(),
			want:  ErrTaskNotFound,
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {m1},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)

		got := r.DeleteScheduledTask(tc.qname, tc.id)
		if got != tc.want {
			t.Errorf("r.DeleteScheduledTask(%q, %v) = %v, want %v", tc.qname, tc.id, got, tc.want)
			continue
		}

		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledKey(qname), diff)
			}
		}
	}
}

func TestDeletePendingTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	tests := []struct {
		pending     map[string][]*base.TaskMessage
		qname       string
		id          uuid.UUID
		want        error
		wantPending map[string][]*base.TaskMessage
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			qname: "default",
			id:    m1.ID,
			want:  nil,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m2},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			qname: "custom",
			id:    m3.ID,
			want:  nil,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			qname: "default",
			id:    uuid.New(),
			want:  ErrTaskNotFound,
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllPendingQueues(t, r.client, tc.pending)

		got := r.DeletePendingTask(tc.qname, tc.id)
		if got != tc.want {
			t.Errorf("r.DeletePendingTask(%q, %v) = %v, want %v", tc.qname, tc.id, got, tc.want)
			continue
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}
	}
}

func TestDeleteAllArchivedTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	tests := []struct {
		archived     map[string][]base.Z
		qname        string
		want         int64
		wantArchived map[string][]*base.TaskMessage
	}{
		{
			archived: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Unix()},
					{Message: m2, Score: time.Now().Unix()},
				},
				"custom": {
					{Message: m3, Score: time.Now().Unix()},
				},
			},
			qname: "default",
			want:  2,
			wantArchived: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m3},
			},
		},
		{
			archived: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantArchived: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got, err := r.DeleteAllArchivedTasks(tc.qname)
		if err != nil {
			t.Errorf("r.DeleteAllDeadTasks(%q) returned error: %v", tc.qname, err)
		}
		if got != tc.want {
			t.Errorf("r.DeleteAllDeadTasks(%q) = %d, nil, want %d, nil", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantArchived {
			gotArchived := h.GetArchivedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestDeleteAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	tests := []struct {
		retry     map[string][]base.Z
		qname     string
		want      int64
		wantRetry map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Unix()},
					{Message: m2, Score: time.Now().Unix()},
				},
				"custom": {
					{Message: m3, Score: time.Now().Unix()},
				},
			},
			qname: "custom",
			want:  1,
			wantRetry: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllRetryQueues(t, r.client, tc.retry)

		got, err := r.DeleteAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("r.DeleteAllRetryTasks(%q) returned error: %v", tc.qname, err)
		}
		if got != tc.want {
			t.Errorf("r.DeleteAllRetryTasks(%q) = %d, nil, want %d, nil", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := h.GetRetryMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(qname), diff)
			}
		}
	}
}

func TestDeleteAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		want          int64
		wantScheduled map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {
					{Message: m1, Score: time.Now().Add(time.Minute).Unix()},
					{Message: m2, Score: time.Now().Add(time.Minute).Unix()},
				},
				"custom": {
					{Message: m3, Score: time.Now().Add(time.Minute).Unix()},
				},
			},
			qname: "default",
			want:  2,
			wantScheduled: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m3},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"custom": {},
			},
			qname: "custom",
			want:  0,
			wantScheduled: map[string][]*base.TaskMessage{
				"custom": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)

		got, err := r.DeleteAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("r.DeleteAllScheduledTasks(%q) returned error: %v", tc.qname, err)
		}
		if got != tc.want {
			t.Errorf("r.DeleteAllScheduledTasks(%q) = %d, nil, want %d, nil", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledKey(qname), diff)
			}
		}
	}
}

func TestDeleteAllPendingTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")

	tests := []struct {
		pending     map[string][]*base.TaskMessage
		qname       string
		want        int64
		wantPending map[string][]*base.TaskMessage
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			qname: "default",
			want:  2,
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m3},
			},
		},
		{
			pending: map[string][]*base.TaskMessage{
				"custom": {},
			},
			qname: "custom",
			want:  0,
			wantPending: map[string][]*base.TaskMessage{
				"custom": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllPendingQueues(t, r.client, tc.pending)

		got, err := r.DeleteAllPendingTasks(tc.qname)
		if err != nil {
			t.Errorf("r.DeleteAllPendingTasks(%q) returned error: %v", tc.qname, err)
		}
		if got != tc.want {
			t.Errorf("r.DeleteAllPendingTasks(%q) = %d, nil, want %d, nil", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.PendingKey(qname), diff)
			}
		}
	}
}

func TestRemoveQueue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	tests := []struct {
		pending    map[string][]*base.TaskMessage
		inProgress map[string][]*base.TaskMessage
		scheduled  map[string][]base.Z
		retry      map[string][]base.Z
		archived   map[string][]base.Z
		qname      string // queue to remove
		force      bool
	}{
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {},
			},
			inProgress: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			force: false,
		},
		{
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			inProgress: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: m4, Score: time.Now().Unix()}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			force: true, // allow removing non-empty queue
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllPendingQueues(t, r.client, tc.pending)
		h.SeedAllActiveQueues(t, r.client, tc.inProgress)
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)
		h.SeedAllRetryQueues(t, r.client, tc.retry)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		err := r.RemoveQueue(tc.qname, tc.force)
		if err != nil {
			t.Errorf("(*RDB).RemoveQueue(%q, %t) = %v, want nil",
				tc.qname, tc.force, err)
			continue
		}
		if r.client.SIsMember(base.AllQueues, tc.qname).Val() {
			t.Errorf("%q is a member of %q", tc.qname, base.AllQueues)
		}

		keys := []string{
			base.PendingKey(tc.qname),
			base.ActiveKey(tc.qname),
			base.DeadlinesKey(tc.qname),
			base.ScheduledKey(tc.qname),
			base.RetryKey(tc.qname),
			base.ArchivedKey(tc.qname),
		}
		for _, key := range keys {
			if r.client.Exists(key).Val() != 0 {
				t.Errorf("key %q still exists", key)
			}
		}

		if n := len(r.client.Keys(base.TaskKeyPrefix(tc.qname) + "*").Val()); n != 0 {
			t.Errorf("%d keys still exists for tasks", n)
		}
	}
}

func TestRemoveQueueError(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := h.NewTaskMessage("task1", nil)
	m2 := h.NewTaskMessage("task2", nil)
	m3 := h.NewTaskMessageWithQueue("task3", nil, "custom")
	m4 := h.NewTaskMessageWithQueue("task4", nil, "custom")

	tests := []struct {
		desc       string
		pending    map[string][]*base.TaskMessage
		inProgress map[string][]*base.TaskMessage
		scheduled  map[string][]base.Z
		retry      map[string][]base.Z
		archived   map[string][]base.Z
		qname      string // queue to remove
		force      bool
	}{
		{
			desc: "removing non-existent queue",
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			inProgress: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "nonexistent",
			force: false,
		},
		{
			desc: "removing non-empty queue",
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			inProgress: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: m4, Score: time.Now().Unix()}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			force: false,
		},
		{
			desc: "force removing queue with active tasks",
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3},
			},
			inProgress: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m4},
			},
			scheduled: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			archived: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			// Even with force=true, it should error if there are active tasks.
			force: true,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllPendingQueues(t, r.client, tc.pending)
		h.SeedAllActiveQueues(t, r.client, tc.inProgress)
		h.SeedAllScheduledQueues(t, r.client, tc.scheduled)
		h.SeedAllRetryQueues(t, r.client, tc.retry)
		h.SeedAllArchivedQueues(t, r.client, tc.archived)

		got := r.RemoveQueue(tc.qname, tc.force)
		if got == nil {
			t.Errorf("%s;(*RDB).RemoveQueue(%q) = nil, want error", tc.desc, tc.qname)
			continue
		}

		// Make sure that nothing changed
		for qname, want := range tc.pending {
			gotPending := h.GetPendingMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt); diff != "" {
				t.Errorf("%s;mismatch found in %q; (-want,+got):\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.inProgress {
			gotActive := h.GetActiveMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotActive, h.SortMsgOpt); diff != "" {
				t.Errorf("%s;mismatch found in %q; (-want,+got):\n%s", tc.desc, base.ActiveKey(qname), diff)
			}
		}
		for qname, want := range tc.scheduled {
			gotScheduled := h.GetScheduledEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotScheduled, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("%s;mismatch found in %q; (-want,+got):\n%s", tc.desc, base.ScheduledKey(qname), diff)
			}
		}
		for qname, want := range tc.retry {
			gotRetry := h.GetRetryEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("%s;mismatch found in %q; (-want,+got):\n%s", tc.desc, base.RetryKey(qname), diff)
			}
		}
		for qname, want := range tc.archived {
			gotArchived := h.GetArchivedEntries(t, r.client, qname)
			if diff := cmp.Diff(want, gotArchived, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("%s;mismatch found in %q; (-want,+got):\n%s", tc.desc, base.ArchivedKey(qname), diff)
			}
		}
	}
}

func TestListServers(t *testing.T) {
	r := setup(t)
	defer r.Close()

	started1 := time.Now().Add(-time.Hour)
	info1 := &base.ServerInfo{
		Host:              "do.droplet1",
		PID:               1234,
		ServerID:          "server123",
		Concurrency:       10,
		Queues:            map[string]int{"default": 1},
		Status:            "active",
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
	defer r.Close()

	var (
		host     = "127.0.0.1"
		pid      = 4567
		serverID = "server123"

		m1 = h.NewTaskMessage("send_email", h.JSON(map[string]interface{}{"user_id": "abc123"}))
		m2 = h.NewTaskMessage("gen_thumbnail", h.JSON(map[string]interface{}{"path": "some/path/to/image/file"}))
		m3 = h.NewTaskMessage("reindex", h.JSON(map[string]interface{}{}))
	)

	tests := []struct {
		data []*base.WorkerInfo
	}{
		{
			data: []*base.WorkerInfo{
				{
					Host:     host,
					PID:      pid,
					ServerID: serverID,
					ID:       m1.ID.String(),
					Type:     m1.Type,
					Queue:    m1.Queue,
					Payload:  m1.Payload,
					Started:  time.Now().Add(-1 * time.Second),
					Deadline: time.Now().Add(30 * time.Second),
				},
				{
					Host:     host,
					PID:      pid,
					ServerID: serverID,
					ID:       m2.ID.String(),
					Type:     m2.Type,
					Queue:    m2.Queue,
					Payload:  m2.Payload,
					Started:  time.Now().Add(-5 * time.Second),
					Deadline: time.Now().Add(10 * time.Minute),
				},
				{
					Host:     host,
					PID:      pid,
					ServerID: serverID,
					ID:       m3.ID.String(),
					Type:     m3.Type,
					Queue:    m3.Queue,
					Payload:  m3.Payload,
					Started:  time.Now().Add(-30 * time.Second),
					Deadline: time.Now().Add(30 * time.Minute),
				},
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

func TestWriteListClearSchedulerEntries(t *testing.T) {
	r := setup(t)
	now := time.Now().UTC()
	schedulerID := "127.0.0.1:9876:abc123"

	data := []*base.SchedulerEntry{
		{
			Spec:    "* * * * *",
			Type:    "foo",
			Payload: nil,
			Opts:    nil,
			Next:    now.Add(5 * time.Hour),
			Prev:    now.Add(-2 * time.Hour),
		},
		{
			Spec:    "@every 20m",
			Type:    "bar",
			Payload: h.JSON(map[string]interface{}{"fiz": "baz"}),
			Opts:    nil,
			Next:    now.Add(1 * time.Minute),
			Prev:    now.Add(-19 * time.Minute),
		},
	}

	if err := r.WriteSchedulerEntries(schedulerID, data, 30*time.Second); err != nil {
		t.Fatalf("WriteSchedulerEnties failed: %v", err)
	}
	entries, err := r.ListSchedulerEntries()
	if err != nil {
		t.Fatalf("ListSchedulerEntries failed: %v", err)
	}
	if diff := cmp.Diff(data, entries, h.SortSchedulerEntryOpt); diff != "" {
		t.Errorf("ListSchedulerEntries() = %v, want %v; (-want,+got)\n%s", entries, data, diff)
	}
	if err := r.ClearSchedulerEntries(schedulerID); err != nil {
		t.Fatalf("ClearSchedulerEntries failed: %v", err)
	}
	entries, err = r.ListSchedulerEntries()
	if err != nil {
		t.Fatalf("ListSchedulerEntries() after clear failed: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("found %d entries, want 0 after clearing", len(entries))
	}
}

func TestSchedulerEnqueueEvents(t *testing.T) {
	r := setup(t)

	var (
		now          = time.Now()
		oneDayAgo    = now.Add(-24 * time.Hour)
		fiveHoursAgo = now.Add(-5 * time.Hour)
		oneHourAgo   = now.Add(-1 * time.Hour)
	)

	tests := []struct {
		entryID string
		events  []*base.SchedulerEnqueueEvent
		want    []*base.SchedulerEnqueueEvent
	}{
		{
			entryID: "entry123",
			events: []*base.SchedulerEnqueueEvent{
				{TaskID: "task123", EnqueuedAt: oneDayAgo},
				{TaskID: "task789", EnqueuedAt: oneHourAgo},
				{TaskID: "task456", EnqueuedAt: fiveHoursAgo},
			},
			// Recent events first
			want: []*base.SchedulerEnqueueEvent{
				{TaskID: "task789", EnqueuedAt: oneHourAgo},
				{TaskID: "task456", EnqueuedAt: fiveHoursAgo},
				{TaskID: "task123", EnqueuedAt: oneDayAgo},
			},
		},
		{
			entryID: "entry456",
			events:  nil,
			want:    nil,
		},
	}

loop:
	for _, tc := range tests {
		h.FlushDB(t, r.client)

		for _, e := range tc.events {
			if err := r.RecordSchedulerEnqueueEvent(tc.entryID, e); err != nil {
				t.Errorf("RecordSchedulerEnqueueEvent(%q, %v) failed: %v", tc.entryID, e, err)
				continue loop
			}
		}
		got, err := r.ListSchedulerEnqueueEvents(tc.entryID, Pagination{Size: 20, Page: 0})
		if err != nil {
			t.Errorf("ListSchedulerEnqueueEvents(%q) failed: %v", tc.entryID, err)
			continue
		}
		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
			t.Errorf("ListSchedulerEnqueueEvent(%q) = %v, want %v; (-want,+got)\n%s",
				tc.entryID, got, tc.want, diff)
		}
	}
}

func TestRecordSchedulerEnqueueEventTrimsDataSet(t *testing.T) {
	r := setup(t)
	var (
		entryID = "entry123"
		now     = time.Now()
		key     = base.SchedulerHistoryKey(entryID)
	)

	// Record maximum number of events.
	for i := 1; i <= maxEvents; i++ {
		event := base.SchedulerEnqueueEvent{
			TaskID:     fmt.Sprintf("task%d", i),
			EnqueuedAt: now.Add(-time.Duration(i) * time.Second),
		}
		if err := r.RecordSchedulerEnqueueEvent(entryID, &event); err != nil {
			t.Fatalf("RecordSchedulerEnqueueEvent failed: %v", err)
		}
	}

	// Make sure the set is full.
	if n := r.client.ZCard(key).Val(); n != maxEvents {
		t.Fatalf("unexpected number of events; got %d, want %d", n, maxEvents)
	}

	// Record one more event, should evict the oldest event.
	event := base.SchedulerEnqueueEvent{
		TaskID:     "latest",
		EnqueuedAt: now,
	}
	if err := r.RecordSchedulerEnqueueEvent(entryID, &event); err != nil {
		t.Fatalf("RecordSchedulerEnqueueEvent failed: %v", err)
	}
	if n := r.client.ZCard(key).Val(); n != maxEvents {
		t.Fatalf("unexpected number of events; got %d, want %d", n, maxEvents)
	}
	events, err := r.ListSchedulerEnqueueEvents(entryID, Pagination{Size: maxEvents})
	if err != nil {
		t.Fatalf("ListSchedulerEnqueueEvents failed: %v", err)
	}
	if first := events[0]; first.TaskID != "latest" {
		t.Errorf("unexpected first event; got %q, want %q", first.TaskID, "latest")
	}
	if last := events[maxEvents-1]; last.TaskID != fmt.Sprintf("task%d", maxEvents-1) {
		t.Errorf("unexpected last event; got %q, want %q", last.TaskID, fmt.Sprintf("task%d", maxEvents-1))
	}
}

func TestPause(t *testing.T) {
	r := setup(t)

	tests := []struct {
		qname string // name of the queue to pause
	}{
		{qname: "default"},
		{qname: "custom"},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		err := r.Pause(tc.qname)
		if err != nil {
			t.Errorf("Pause(%q) returned error: %v", tc.qname, err)
		}
		key := base.PausedKey(tc.qname)
		if r.client.Exists(key).Val() == 0 {
			t.Errorf("key %q does not exist", key)
		}
	}
}

func TestPauseError(t *testing.T) {
	r := setup(t)

	tests := []struct {
		desc   string   // test case description
		paused []string // already paused queues
		qname  string   // name of the queue to pause
	}{
		{"queue already paused", []string{"default", "custom"}, "default"},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		for _, qname := range tc.paused {
			if err := r.Pause(qname); err != nil {
				t.Fatalf("could not pause %q: %v", qname, err)
			}
		}

		err := r.Pause(tc.qname)
		if err == nil {
			t.Errorf("%s; Pause(%q) returned nil: want error", tc.desc, tc.qname)
		}
	}
}

func TestUnpause(t *testing.T) {
	r := setup(t)

	tests := []struct {
		paused []string // already paused queues
		qname  string   // name of the queue to unpause
	}{
		{[]string{"default", "custom"}, "default"},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		for _, qname := range tc.paused {
			if err := r.Pause(qname); err != nil {
				t.Fatalf("could not pause %q: %v", qname, err)
			}
		}

		err := r.Unpause(tc.qname)
		if err != nil {
			t.Errorf("Unpause(%q) returned error: %v", tc.qname, err)
		}
		key := base.PausedKey(tc.qname)
		if r.client.Exists(key).Val() == 1 {
			t.Errorf("key %q exists", key)
		}
	}
}

func TestUnpauseError(t *testing.T) {
	r := setup(t)

	tests := []struct {
		desc   string   // test case description
		paused []string // already paused queues
		qname  string   // name of the queue to unpause
	}{
		{"queue is not paused", []string{"default"}, "custom"},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		for _, qname := range tc.paused {
			if err := r.Pause(qname); err != nil {
				t.Fatalf("could not pause %q: %v", qname, err)
			}
		}

		err := r.Unpause(tc.qname)
		if err == nil {
			t.Errorf("%s; Unpause(%q) returned nil: want error", tc.desc, tc.qname)
		}
	}
}
