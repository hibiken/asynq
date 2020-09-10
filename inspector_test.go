// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hibiken/asynq/internal/asynqtest"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

func TestInspectorQueues(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		queues []string
	}{
		{queues: []string{"default"}},
		{queues: []string{"custom1", "custom2"}},
		{queues: []string{"default", "custom1", "custom2"}},
		{queues: []string{}},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		for _, qname := range tc.queues {
			if err := r.SAdd(base.AllQueues, qname).Err(); err != nil {
				t.Fatalf("could not initialize all queue set: %v", err)
			}
		}
		got, err := inspector.Queues()
		if err != nil {
			t.Errorf("Queues() returned an error: %v", err)
			continue
		}
		if diff := cmp.Diff(tc.queues, got, h.SortStringSliceOpt); diff != "" {
			t.Errorf("Queues() = %v, want %v; (-want, +got)\n%s", got, tc.queues, diff)
		}
	}

}

func TestInspectorCurrentStats(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessage("task4", nil)
	m5 := asynqtest.NewTaskMessageWithQueue("task5", nil, "critical")
	m6 := h.NewTaskMessageWithQueue("task6", nil, "low")
	now := time.Now()
	timeCmpOpt := cmpopts.EquateApproxTime(time.Second)

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		pending    map[string][]*base.TaskMessage
		inProgress map[string][]*base.TaskMessage
		scheduled  map[string][]base.Z
		retry      map[string][]base.Z
		dead       map[string][]base.Z
		processed  map[string]int
		failed     map[string]int
		qname      string
		want       *QueueStats
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
			dead: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			processed: map[string]int{
				"default":  120,
				"critical": 100,
				"low":      42,
			},
			failed: map[string]int{
				"default":  2,
				"critical": 0,
				"low":      5,
			},
			qname: "default",
			want: &QueueStats{
				Queue:     "default",
				Size:      4,
				Pending:   1,
				Active:    1,
				Scheduled: 2,
				Retry:     0,
				Dead:      0,
				Processed: 120,
				Failed:    2,
				Paused:    false,
				Timestamp: now,
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllPendingQueues(t, r, tc.pending)
		asynqtest.SeedAllActiveQueues(t, r, tc.inProgress)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)
		for qname, n := range tc.processed {
			processedKey := base.ProcessedKey(qname, now)
			r.Set(processedKey, n, 0)
		}
		for qname, n := range tc.failed {
			failedKey := base.FailedKey(qname, now)
			r.Set(failedKey, n, 0)
		}

		got, err := inspector.CurrentStats(tc.qname)
		if err != nil {
			t.Errorf("r.CurrentStats(%q) = %v, %v, want %v, nil",
				tc.qname, got, err, tc.want)
			continue
		}
		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
			t.Errorf("r.CurrentStats(%q) = %v, %v, want %v, nil; (-want, +got)\n%s",
				tc.qname, got, err, tc.want, diff)
			continue
		}
	}

}

func TestInspectorHistory(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now().UTC()
	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		qname string // queue of interest
		n     int    // number of days
	}{
		{"default", 90},
		{"custom", 7},
		{"default", 1},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)

		r.SAdd(base.AllQueues, tc.qname)
		// populate last n days data
		for i := 0; i < tc.n; i++ {
			ts := now.Add(-time.Duration(i) * 24 * time.Hour)
			processedKey := base.ProcessedKey(tc.qname, ts)
			failedKey := base.FailedKey(tc.qname, ts)
			r.Set(processedKey, (i+1)*1000, 0)
			r.Set(failedKey, (i+1)*10, 0)
		}

		got, err := inspector.History(tc.qname, tc.n)
		if err != nil {
			t.Errorf("Inspector.History(%q, %d) returned error: %v", tc.qname, tc.n, err)
			continue
		}
		if len(got) != tc.n {
			t.Errorf("Inspector.History(%q, %d) returned %d daily stats, want %d",
				tc.qname, tc.n, len(got), tc.n)
			continue
		}
		for i := 0; i < tc.n; i++ {
			want := &DailyStats{
				Queue:     tc.qname,
				Processed: (i + 1) * 1000,
				Failed:    (i + 1) * 10,
				Date:      now.Add(-time.Duration(i) * 24 * time.Hour),
			}
			// Allow 2 seconds difference in timestamp.
			timeCmpOpt := cmpopts.EquateApproxTime(2 * time.Second)
			if diff := cmp.Diff(want, got[i], timeCmpOpt); diff != "" {
				t.Errorf("Inspector.History %d days ago data; got %+v, want %+v; (-want,+got):\n%s",
					i, got[i], want, diff)
			}
		}
	}
}

func createPendingTask(msg *base.TaskMessage) *PendingTask {
	return &PendingTask{
		Task:  NewTask(msg.Type, msg.Payload),
		ID:    msg.ID.String(),
		Queue: msg.Queue,
	}
}

func TestInspectorListPendingTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessage("task4", nil)

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		desc    string
		pending map[string][]*base.TaskMessage
		qname   string
		want    []*PendingTask
	}{
		{
			desc: "with default queue",
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			qname: "default",
			want: []*PendingTask{
				createPendingTask(m1),
				createPendingTask(m2),
			},
		},
		{
			desc: "with named queue",
			pending: map[string][]*base.TaskMessage{
				"default":  {m1, m2},
				"critical": {m3},
				"low":      {m4},
			},
			qname: "critical",
			want: []*PendingTask{
				createPendingTask(m3),
			},
		},
		{
			desc: "with empty queue",
			pending: map[string][]*base.TaskMessage{
				"default": {},
			},
			qname: "default",
			want:  []*PendingTask(nil),
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		for q, msgs := range tc.pending {
			asynqtest.SeedPendingQueue(t, r, msgs, q)
		}

		got, err := inspector.ListPendingTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListPendingTasks(%q) returned error: %v",
				tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListPendingTasks(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func TestInspectorListActiveTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessage("task4", nil)

	inspector := NewInspector(getRedisConnOpt(t))

	createActiveTask := func(msg *base.TaskMessage) *ActiveTask {
		return &ActiveTask{
			Task:  NewTask(msg.Type, msg.Payload),
			ID:    msg.ID.String(),
			Queue: msg.Queue,
		}
	}

	tests := []struct {
		desc       string
		inProgress map[string][]*base.TaskMessage
		qname      string
		want       []*ActiveTask
	}{
		{
			desc: "with a few active tasks",
			inProgress: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3, m4},
			},
			qname: "default",
			want: []*ActiveTask{
				createActiveTask(m1),
				createActiveTask(m2),
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllActiveQueues(t, r, tc.inProgress)

		got, err := inspector.ListActiveTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListActiveTasks(%q) returned error: %v", tc.qname, tc.desc, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListActiveTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createScheduledTask(z base.Z) *ScheduledTask {
	msg := z.Message
	return &ScheduledTask{
		Task:          NewTask(msg.Type, msg.Payload),
		ID:            msg.ID.String(),
		Queue:         msg.Queue,
		NextProcessAt: time.Unix(z.Score, 0),
		score:         z.Score,
	}
}

func TestInspectorListScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		desc      string
		scheduled map[string][]base.Z
		qname     string
		want      []*ScheduledTask
	}{
		{
			desc: "with a few scheduled tasks",
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by NextProcessAt.
			want: []*ScheduledTask{
				createScheduledTask(z3),
				createScheduledTask(z1),
				createScheduledTask(z2),
			},
		},
		{
			desc: "with empty scheduled queue",
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*ScheduledTask(nil),
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)

		got, err := inspector.ListScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListScheduledTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{}, ScheduledTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListScheduledTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createRetryTask(z base.Z) *RetryTask {
	msg := z.Message
	return &RetryTask{
		Task:          NewTask(msg.Type, msg.Payload),
		ID:            msg.ID.String(),
		Queue:         msg.Queue,
		NextProcessAt: time.Unix(z.Score, 0),
		MaxRetry:      msg.Retry,
		Retried:       msg.Retried,
		ErrorMsg:      msg.ErrorMsg,
		score:         z.Score,
	}
}

func TestInspectorListRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		desc  string
		retry map[string][]base.Z
		qname string
		want  []*RetryTask
	}{
		{
			desc: "with a few retry tasks",
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by NextProcessAt.
			want: []*RetryTask{
				createRetryTask(z3),
				createRetryTask(z1),
				createRetryTask(z2),
			},
		},
		{
			desc: "with empty retry queue",
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*RetryTask(nil),
		},
		// TODO(hibiken): ErrQueueNotFound when queue doesn't exist
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)

		got, err := inspector.ListRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListRetryTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{}, RetryTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListRetryTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func createDeadTask(z base.Z) *DeadTask {
	msg := z.Message
	return &DeadTask{
		Task:         NewTask(msg.Type, msg.Payload),
		ID:           msg.ID.String(),
		Queue:        msg.Queue,
		MaxRetry:     msg.Retry,
		Retried:      msg.Retried,
		LastFailedAt: time.Unix(z.Score, 0),
		ErrorMsg:     msg.ErrorMsg,
		score:        z.Score,
	}
}

func TestInspectorListDeadTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		desc  string
		dead  map[string][]base.Z
		qname string
		want  []*DeadTask
	}{
		{
			desc: "with a few dead tasks",
			dead: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			// Should be sorted by LastFailedAt.
			want: []*DeadTask{
				createDeadTask(z2),
				createDeadTask(z1),
				createDeadTask(z3),
			},
		},
		{
			desc: "with empty dead queue",
			dead: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  []*DeadTask(nil),
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)

		got, err := inspector.ListDeadTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListDeadTasks(%q) returned error: %v", tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{}, DeadTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListDeadTask(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func TestInspectorListPagination(t *testing.T) {
	// Create 100 tasks.
	var msgs []*base.TaskMessage
	for i := 0; i <= 99; i++ {
		msgs = append(msgs,
			asynqtest.NewTaskMessage(fmt.Sprintf("task%d", i), nil))
	}
	r := setup(t)
	defer r.Close()
	asynqtest.SeedPendingQueue(t, r, msgs, base.DefaultQueueName)

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		page     int
		pageSize int
		want     []*PendingTask
	}{
		{
			page:     1,
			pageSize: 5,
			want: []*PendingTask{
				createPendingTask(msgs[0]),
				createPendingTask(msgs[1]),
				createPendingTask(msgs[2]),
				createPendingTask(msgs[3]),
				createPendingTask(msgs[4]),
			},
		},
		{
			page:     3,
			pageSize: 10,
			want: []*PendingTask{
				createPendingTask(msgs[20]),
				createPendingTask(msgs[21]),
				createPendingTask(msgs[22]),
				createPendingTask(msgs[23]),
				createPendingTask(msgs[24]),
				createPendingTask(msgs[25]),
				createPendingTask(msgs[26]),
				createPendingTask(msgs[27]),
				createPendingTask(msgs[28]),
				createPendingTask(msgs[29]),
			},
		},
	}

	for _, tc := range tests {
		got, err := inspector.ListPendingTasks("default", Page(tc.page), PageSize(tc.pageSize))
		if err != nil {
			t.Errorf("ListPendingTask('default') returned error: %v", err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("ListPendingTask('default') = %v, want %v; (-want,+got)\n%s",
				got, tc.want, diff)
		}
	}
}

func TestInspectorDeleteAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		want          int
		wantScheduled map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantScheduled: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)

		got, err := inspector.DeleteAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		retry     map[string][]base.Z
		qname     string
		want      int
		wantRetry map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantRetry: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)

		got, err := inspector.DeleteAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteAllDeadTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		dead     map[string][]base.Z
		qname    string
		want     int
		wantDead map[string][]base.Z
	}{
		{
			dead: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
			wantDead: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
		},
		{
			dead: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantDead: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)

		got, err := inspector.DeleteAllDeadTasks(tc.qname)
		if err != nil {
			t.Errorf("DeleteAllDeadTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllDeadTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorKillAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		dead          map[string][]base.Z
		qname         string
		want          int
		wantScheduled map[string][]base.Z
		wantDead      map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			dead: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			want:  3,
			wantScheduled: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
			wantDead: map[string][]base.Z{
				"default": {
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
					base.Z{Message: m3, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
			},
			dead: map[string][]base.Z{
				"default": {z3},
			},
			qname: "default",
			want:  2,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantDead: map[string][]base.Z{
				"default": {
					z3,
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
				},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			dead: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantDead: map[string][]base.Z{
				"default": {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			dead: map[string][]base.Z{
				"default": {z1, z2},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantDead: map[string][]base.Z{
				"default": {z1, z2},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)

		got, err := inspector.KillAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("KillAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("KillAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantDead {
			// Allow Z.Score to differ by up to 2.
			approxOpt := cmp.Comparer(func(a, b int64) bool {
				return math.Abs(float64(a-b)) < 2
			})
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt, approxOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorKillAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		retry     map[string][]base.Z
		dead      map[string][]base.Z
		qname     string
		want      int
		wantRetry map[string][]base.Z
		wantDead  map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			dead: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			want:  3,
			wantRetry: map[string][]base.Z{
				"default": {},
				"custom":  {z4},
			},
			wantDead: map[string][]base.Z{
				"default": {
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
					base.Z{Message: m3, Score: now.Unix()},
				},
				"custom": {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {z1, z2},
			},
			dead: map[string][]base.Z{
				"default": {z3},
			},
			qname: "default",
			want:  2,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantDead: map[string][]base.Z{
				"default": {
					z3,
					base.Z{Message: m1, Score: now.Unix()},
					base.Z{Message: m2, Score: now.Unix()},
				},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			dead: map[string][]base.Z{
				"default": {z1, z2},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantDead: map[string][]base.Z{
				"default": {z1, z2},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)

		got, err := inspector.KillAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("KillAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("KillAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		cmpOpt := asynqtest.EquateInt64Approx(2) // allow for 2 seconds difference in Z.Score
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt, cmpOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllScheduledTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := asynqtest.NewTaskMessage("task4", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		pending       map[string][]*base.TaskMessage
		qname         string
		want          int
		wantScheduled map[string][]base.Z
		wantPending   map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantScheduled: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  1,
			wantScheduled: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
				"low":      {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)
		asynqtest.SeedAllPendingQueues(t, r, tc.pending)

		got, err := inspector.RunAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := asynqtest.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllRetryTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := asynqtest.NewTaskMessage("task2", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		retry       map[string][]base.Z
		pending     map[string][]*base.TaskMessage
		qname       string
		want        int
		wantRetry   map[string][]base.Z
		wantPending map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantRetry: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  1,
			wantRetry: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
				"low":      {},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)
		asynqtest.SeedAllPendingQueues(t, r, tc.pending)

		got, err := inspector.RunAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := asynqtest.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunAllDeadTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := asynqtest.NewTaskMessage("task2", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		dead        map[string][]base.Z
		pending     map[string][]*base.TaskMessage
		qname       string
		want        int
		wantDead    map[string][]base.Z
		wantPending map[string][]*base.TaskMessage
	}{
		{
			dead: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "default",
			want:  2,
			wantDead: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m1, m4},
				"critical": {},
				"low":      {},
			},
		},
		{
			dead: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
			},
			qname: "default",
			want:  1,
			wantDead: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
			},
		},
		{
			dead: map[string][]base.Z{
				"default": {},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantDead: map[string][]base.Z{
				"default": {},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)
		asynqtest.SeedAllPendingQueues(t, r, tc.pending)

		got, err := inspector.RunAllDeadTasks(tc.qname)
		if err != nil {
			t.Errorf("RunAllDeadTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("RunAllDeadTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}

		}
		for qname, want := range tc.wantPending {
			gotPending := asynqtest.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		qname         string
		key           string
		wantScheduled map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			key:   createScheduledTask(z2).Key(),
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)

		if err := inspector.DeleteTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("DeleteTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}

		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		retry     map[string][]base.Z
		qname     string
		key       string
		wantRetry map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			key:   createRetryTask(z2).Key(),
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)

		if err := inspector.DeleteTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("DeleteTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesDeadTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		dead     map[string][]base.Z
		qname    string
		key      string
		wantDead map[string][]base.Z
	}{
		{
			dead: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			qname: "default",
			key:   createDeadTask(z2).Key(),
			wantDead: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)

		if err := inspector.DeleteTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("DeleteTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskByKeyRunsScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		pending       map[string][]*base.TaskMessage
		qname         string
		key           string
		wantScheduled map[string][]base.Z
		wantPending   map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			key:   createScheduledTask(z2).Key(),
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {m2},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)
		asynqtest.SeedAllPendingQueues(t, r, tc.pending)

		if err := inspector.RunTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("RunTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}

		}
		for qname, want := range tc.wantPending {
			gotPending := asynqtest.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskByKeyRunsRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		retry       map[string][]base.Z
		pending     map[string][]*base.TaskMessage
		qname       string
		key         string
		wantRetry   map[string][]base.Z
		wantPending map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			key:   createRetryTask(z2).Key(),
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m2},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)
		asynqtest.SeedAllPendingQueues(t, r, tc.pending)

		if err := inspector.RunTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("RunTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := asynqtest.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorRunTaskByKeyRunsDeadTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		dead        map[string][]base.Z
		pending     map[string][]*base.TaskMessage
		qname       string
		key         string
		wantDead    map[string][]base.Z
		wantPending map[string][]*base.TaskMessage
	}{
		{
			dead: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			pending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			qname: "critical",
			key:   createDeadTask(z2).Key(),
			wantDead: map[string][]base.Z{
				"default":  {z1},
				"critical": {},
				"low":      {z3},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {m2},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)
		asynqtest.SeedAllPendingQueues(t, r, tc.pending)

		if err := inspector.RunTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("RunTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantPending {
			gotPending := asynqtest.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected pending tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorKillTaskByKeyKillsScheduledTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		scheduled     map[string][]base.Z
		dead          map[string][]base.Z
		qname         string
		key           string
		want          string
		wantScheduled map[string][]base.Z
		wantDead      map[string][]base.Z
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			dead: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			key:   createScheduledTask(z2).Key(),
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantDead: map[string][]base.Z{
				"default": {},
				"custom":  {{m2, now.Unix()}},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)

		if err := inspector.KillTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("KillTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}

		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}

func TestInspectorKillTaskByKeyKillsRetryTask(t *testing.T) {
	r := setup(t)
	defer r.Close()
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(getRedisConnOpt(t))

	tests := []struct {
		retry     map[string][]base.Z
		dead      map[string][]base.Z
		qname     string
		key       string
		wantRetry map[string][]base.Z
		wantDead  map[string][]base.Z
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			dead: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			key:   createRetryTask(z2).Key(),
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantDead: map[string][]base.Z{
				"default": {},
				"custom":  {{m2, now.Unix()}},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)

		if err := inspector.KillTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("KillTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s",
					qname, diff)
			}
		}
	}
}
