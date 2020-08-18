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

func TestInspectorCurrentStats(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessage("task4", nil)
	m5 := asynqtest.NewTaskMessageWithQueue("task5", nil, "critical")
	m6 := h.NewTaskMessageWithQueue("task6", nil, "low")
	now := time.Now()
	timeCmpOpt := cmpopts.EquateApproxTime(time.Second)

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		enqueued   map[string][]*base.TaskMessage
		inProgress map[string][]*base.TaskMessage
		scheduled  map[string][]base.Z
		retry      map[string][]base.Z
		dead       map[string][]base.Z
		processed  map[string]int
		failed     map[string]int
		qname      string
		want       *Stats
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
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
			want: &Stats{
				Queue:      "default",
				Enqueued:   1,
				InProgress: 1,
				Scheduled:  2,
				Retry:      0,
				Dead:       0,
				Processed:  120,
				Failed:     2,
				Paused:     false,
				Timestamp:  now,
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)
		asynqtest.SeedAllInProgressQueues(t, r, tc.inProgress)
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
	now := time.Now().UTC()
	timeCmpOpt := cmpopts.EquateApproxTime(time.Second)

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		qname string // queue of interest
		n     int    // number of days
	}{
		{"default", 90},
		{"custom", 7},
		{"default", 0},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)

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
			if diff := cmp.Diff(want, got[i], timeCmpOpt); diff != "" {
				t.Errorf("Inspector.History %d days ago data; got %+v, want %+v; (-want,+got):\n%s",
					i, got[i], want, diff)
			}
		}
	}
}

func createEnqueuedTask(msg *base.TaskMessage) *EnqueuedTask {
	return &EnqueuedTask{
		Task:  NewTask(msg.Type, msg.Payload),
		ID:    msg.ID.String(),
		Queue: msg.Queue,
	}
}

func TestInspectorListEnqueuedTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessage("task4", nil)

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		desc     string
		enqueued map[string][]*base.TaskMessage
		qname    string
		want     []*EnqueuedTask
	}{
		{
			desc: "with default queue",
			enqueued: map[string][]*base.TaskMessage{
				"default": {m1, m2},
			},
			qname: "default",
			want: []*EnqueuedTask{
				createEnqueuedTask(m1),
				createEnqueuedTask(m2),
			},
		},
		{
			desc: "with named queue",
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m1, m2},
				"critical": {m3},
				"low":      {m4},
			},
			qname: "critical",
			want: []*EnqueuedTask{
				createEnqueuedTask(m3),
			},
		},
		{
			desc: "with empty queue",
			enqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			qname: "default",
			want:  []*EnqueuedTask(nil),
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		for q, msgs := range tc.enqueued {
			asynqtest.SeedEnqueuedQueue(t, r, msgs, q)
		}

		got, err := inspector.ListEnqueuedTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListEnqueuedTasks(%q) returned error: %v",
				tc.desc, tc.qname, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListEnqueuedTasks(%q) = %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.qname, got, tc.want, diff)
		}
	}
}

func TestInspectorListInProgressTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessage("task4", nil)

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	createInProgressTask := func(msg *base.TaskMessage) *InProgressTask {
		return &InProgressTask{
			Task:  NewTask(msg.Type, msg.Payload),
			ID:    msg.ID.String(),
			Queue: msg.Queue,
		}
	}

	tests := []struct {
		desc       string
		inProgress map[string][]*base.TaskMessage
		qname      string
		want       []*InProgressTask
	}{
		{
			desc: "with a few in-progress tasks",
			inProgress: map[string][]*base.TaskMessage{
				"default": {m1, m2},
				"custom":  {m3, m4},
			},
			qname: "default",
			want: []*InProgressTask{
				createInProgressTask(m1),
				createInProgressTask(m2),
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllInProgressQueues(t, r, tc.inProgress)

		got, err := inspector.ListInProgressTasks(tc.qname)
		if err != nil {
			t.Errorf("%s; ListInProgressTasks(%q) returned error: %v", tc.qname, tc.desc, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListInProgressTask(%wq) = %v, want %v; (-want,+got)\n%s",
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
		NextEnqueueAt: time.Unix(z.Score, 0),
	}
}

func TestInspectorListScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
			// Should be sorted by NextEnqueuedAt.
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
		NextEnqueueAt: time.Unix(z.Score, 0),
		MaxRetry:      msg.Retry,
		Retried:       msg.Retried,
		ErrorMsg:      msg.ErrorMsg,
	}
}

func TestInspectorListRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
			// Should be sorted by NextEnqueuedAt.
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
	}
}

func TestInspectorListDeadTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
	asynqtest.SeedEnqueuedQueue(t, r, msgs, base.DefaultQueueName)

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		page     int
		pageSize int
		want     []*EnqueuedTask
	}{
		{
			page:     1,
			pageSize: 5,
			want: []*EnqueuedTask{
				createEnqueuedTask(msgs[0]),
				createEnqueuedTask(msgs[1]),
				createEnqueuedTask(msgs[2]),
				createEnqueuedTask(msgs[3]),
				createEnqueuedTask(msgs[4]),
			},
		},
		{
			page:     3,
			pageSize: 10,
			want: []*EnqueuedTask{
				createEnqueuedTask(msgs[20]),
				createEnqueuedTask(msgs[21]),
				createEnqueuedTask(msgs[22]),
				createEnqueuedTask(msgs[23]),
				createEnqueuedTask(msgs[24]),
				createEnqueuedTask(msgs[25]),
				createEnqueuedTask(msgs[26]),
				createEnqueuedTask(msgs[27]),
				createEnqueuedTask(msgs[28]),
				createEnqueuedTask(msgs[29]),
			},
		},
	}

	for _, tc := range tests {
		got, err := inspector.ListEnqueuedTasks("default", Page(tc.page), PageSize(tc.pageSize))
		if err != nil {
			t.Errorf("ListEnqueuedTask('default') returned error: %v", err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("ListEnqueuedTask('default') = %v, want %v; (-want,+got)\n%s",
				got, tc.want, diff)
		}
	}
}

func TestInspectorDeleteAllScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled map[string][]base.Z
		qname     string
		want      int
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
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
		gotScheduled := asynqtest.GetScheduledEntries(t, r, tc.qname)
		if len(gotScheduled) != 0 {
			t.Errorf("There are still %d scheduled tasks in queue %q, want empty",
				tc.qname, len(gotScheduled))
		}
	}
}

func TestInspectorDeleteAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry map[string][]base.Z
		qname string
		want  int
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
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
		gotRetry := asynqtest.GetRetryEntries(t, r, tc.qname)
		if len(gotRetry) != 0 {
			t.Errorf("There are still %d retry tasks in queue %q, want empty",
				tc.qname, len(gotRetry))
		}
	}
}

func TestInspectorDeleteAllDeadTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		dead  map[string][]base.Z
		qname string
		want  int
	}{
		{
			dead: map[string][]base.Z{
				"default": {z1, z2, z3},
				"custom":  {z4},
			},
			qname: "default",
			want:  3,
		},
		{
			dead: map[string][]base.Z{
				"default": {},
			},
			qname: "default",
			want:  0,
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
		gotDead := asynqtest.GetDeadEntries(t, r, tc.qname)
		if len(gotDead) != 0 {
			t.Errorf("There are still %d dead tasks in queue %q, want empty",
				tc.qname, len(gotDead))
		}
	}
}

func TestInspectorKillAllScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
		for qname, want := range tc.wantDead {
			// Allow Z.Score to differ by up to 2.
			approxOpt := cmp.Comparer(func(a, b int64) bool {
				return math.Abs(float64(a-b)) < 2
			})
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt, approxOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
	}
}

func TestInspectorKillAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	m4 := asynqtest.NewTaskMessageWithQueue("task4", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
			want: 0,
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
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
	}
}

func TestInspectorEnqueueAllScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := asynqtest.NewTaskMessage("task4", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled     map[string][]base.Z
		enqueued      map[string][]*base.TaskMessage
		qname         string
		want          int
		wantScheduled map[string][]base.Z
		wantEnqueued  map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			enqueued: map[string][]*base.TaskMessage{
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
			wantEnqueued: map[string][]*base.TaskMessage{
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
			enqueued: map[string][]*base.TaskMessage{
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
				"low":      {},
			},
		},
		{
			scheduled: map[string][]base.Z{
				"default": {},
			},
			enqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		got, err := inspector.EnqueueAllScheduledTasks(tc.qname)
		if err != nil {
			t.Errorf("EnqueueAllScheduledTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("EnqueueAllScheduledTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected enqueued tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
	}
}

func TestInspectorEnqueueAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := asynqtest.NewTaskMessage("task2", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry        map[string][]base.Z
		enqueued     map[string][]*base.TaskMessage
		qname        string
		want         int
		wantRetry    map[string][]base.Z
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			enqueued: map[string][]*base.TaskMessage{
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
			wantEnqueued: map[string][]*base.TaskMessage{
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
			enqueued: map[string][]*base.TaskMessage{
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {m2},
				"low":      {m3},
			},
		},
		{
			retry: map[string][]base.Z{
				"default": {},
			},
			enqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantRetry: map[string][]base.Z{
				"default": {},
			},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		got, err := inspector.EnqueueAllRetryTasks(tc.qname)
		if err != nil {
			t.Errorf("EnqueueAllRetryTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("EnqueueAllRetryTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected enqueued tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
	}
}

func TestInspectorEnqueueAllDeadTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	m4 := asynqtest.NewTaskMessage("task2", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}
	z4 := base.Z{Message: m4, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		dead         map[string][]base.Z
		enqueued     map[string][]*base.TaskMessage
		qname        string
		want         int
		wantDead     map[string][]base.Z
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			dead: map[string][]base.Z{
				"default":  {z1, z4},
				"critical": {z2},
				"low":      {z3},
			},
			enqueued: map[string][]*base.TaskMessage{
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
			wantEnqueued: map[string][]*base.TaskMessage{
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
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
			},
			qname: "default",
			want:  1,
			wantDead: map[string][]base.Z{
				"default":  {},
				"critical": {z2},
			},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {},
			},
		},
		{
			dead: map[string][]base.Z{
				"default": {},
			},
			enqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			qname: "default",
			want:  0,
			wantDead: map[string][]base.Z{
				"default": {},
			},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		got, err := inspector.EnqueueAllDeadTasks(tc.qname)
		if err != nil {
			t.Errorf("EnqueueAllDeadTasks(%q) returned error: %v", tc.qname, err)
			continue
		}
		if got != tc.want {
			t.Errorf("EnqueueAllDeadTasks(%q) = %d, want %d", tc.qname, got, tc.want)
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}

		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected enqueued tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}

		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesRetryTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
			gotRetry := asynqtest.GetRetryEntries(t, r, tc.qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesDeadTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s", tc.qname, diff)
			}
		}
	}
}

func TestInspectorEnqueueTaskByKeyEnqueuesScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled     map[string][]base.Z
		enqueued      map[string][]*base.TaskMessage
		qname         string
		key           string
		wantScheduled map[string][]base.Z
		wantEnqueued  map[string][]*base.TaskMessage
	}{
		{
			scheduled: map[string][]base.Z{
				"default": {z1, z2},
				"custom":  {z3},
			},
			enqueued: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			qname: "default",
			key:   createScheduledTask(z2).Key(),
			wantScheduled: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {m2},
				"custom":  {},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllScheduledQueues(t, r, tc.scheduled)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		if err := inspector.EnqueueTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("EnqueueTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}

		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected enqueued tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}
		}
	}
}

func TestInspectorEnqueueTaskByKeyEnqueuesRetryTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry        map[string][]base.Z
		enqueued     map[string][]*base.TaskMessage
		qname        string
		key          string
		wantRetry    map[string][]base.Z
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			retry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z2, z3},
			},
			enqueued: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {},
			},
			qname: "custom",
			key:   createRetryTask(z2).Key(),
			wantRetry: map[string][]base.Z{
				"default": {z1},
				"custom":  {z3},
			},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {},
				"custom":  {m2},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllRetryQueues(t, r, tc.retry)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		if err := inspector.EnqueueTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("EnqueueTaskByKey(%q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected enqueued tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}
		}
	}
}

func TestInspectorEnqueueTaskByKeyEnqueuesDeadTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		dead         map[string][]base.Z
		enqueued     map[string][]*base.TaskMessage
		qname        string
		key          string
		wantDead     map[string][]base.Z
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			dead: map[string][]base.Z{
				"default":  {z1},
				"critical": {z2},
				"low":      {z3},
			},
			enqueued: map[string][]*base.TaskMessage{
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {m2},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllDeadQueues(t, r, tc.dead)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		if err := inspector.EnqueueTaskByKey(tc.qname, tc.key); err != nil {
			t.Errorf("EnqueueTaskByKey(%q, %q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("unexpected enqueued tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}
		}
	}
}

func TestInspectorKillTaskByKeyKillsScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
			t.Errorf("KillTaskByKey(%q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := asynqtest.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected scheduled tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}

		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}
		}
	}
}

func TestInspectorKillTaskByKeyKillsRetryTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "custom")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "custom")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
			t.Errorf("KillTaskByKey(%q) returned error: %v", tc.qname, tc.key, err)
			continue
		}
		for qname, want := range tc.wantRetry {
			gotRetry := asynqtest.GetRetryEntries(t, r, qname)
			if diff := cmp.Diff(want, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected retry tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}
		}
		for qname, want := range tc.wantDead {
			gotDead := asynqtest.GetDeadEntries(t, r, qname)
			if diff := cmp.Diff(want, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
				t.Errorf("unexpected dead tasks in queue %q: (-want, +got)\n%s",
					tc.qname, diff)
			}
		}
	}
}
