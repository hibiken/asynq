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
		inProgress []*base.TaskMessage
		scheduled  []base.Z
		retry      []base.Z
		dead       []base.Z
		processed  int
		failed     int
		allQueues  []interface{}
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
				Queues: []*QueueInfo{
					{Name: "critical", Paused: false, Size: 1},
					{Name: "default", Paused: false, Size: 1},
					{Name: "low", Paused: false, Size: 1},
				},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)
		asynqtest.SeedInProgressQueue(t, r, tc.inProgress)
		asynqtest.SeedScheduledQueue(t, r, tc.scheduled)
		asynqtest.SeedRetryQueue(t, r, tc.retry)
		asynqtest.SeedDeadQueue(t, r, tc.dead)
		processedKey := base.ProcessedKey(now)
		failedKey := base.FailureKey(now)
		r.Set(processedKey, tc.processed, 0)
		r.Set(failedKey, tc.failed, 0)
		r.SAdd(base.AllQueues, tc.allQueues...)

		got, err := inspector.CurrentStats()
		if err != nil {
			t.Errorf("r.CurrentStats() = %v, %v, want %v, nil",
				got, err, tc.want)
			continue
		}
		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
			t.Errorf("r.CurrentStats() = %v, %v, want %v, nil; (-want, +got)\n%s",
				got, err, tc.want, diff)
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
		n int // number of days
	}{
		{90},
		{7},
		{0},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)

		// populate last n days data
		for i := 0; i < tc.n; i++ {
			ts := now.Add(-time.Duration(i) * 24 * time.Hour)
			processedKey := base.ProcessedKey(ts)
			failedKey := base.FailureKey(ts)
			r.Set(processedKey, (i+1)*1000, 0)
			r.Set(failedKey, (i+1)*10, 0)
		}

		got, err := inspector.History(tc.n)
		if err != nil {
			t.Errorf("Inspector.History(%d) returned error: %v", tc.n, err)
			continue
		}
		if len(got) != tc.n {
			t.Errorf("Inspector.History(%d) returned %d daily stats, want %d",
				tc.n, len(got), tc.n)
			continue
		}
		for i := 0; i < tc.n; i++ {
			want := &DailyStats{
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

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	createInProgressTask := func(msg *base.TaskMessage) *InProgressTask {
		return &InProgressTask{
			Task: NewTask(msg.Type, msg.Payload),
			ID:   msg.ID.String(),
		}
	}

	tests := []struct {
		desc       string
		inProgress []*base.TaskMessage
		want       []*InProgressTask
	}{
		{
			desc:       "with a few in-progress tasks",
			inProgress: []*base.TaskMessage{m1, m2},
			want: []*InProgressTask{
				createInProgressTask(m1),
				createInProgressTask(m2),
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedInProgressQueue(t, r, tc.inProgress)

		got, err := inspector.ListInProgressTasks()
		if err != nil {
			t.Errorf("%s; ListInProgressTasks() returned error: %v", tc.desc, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListInProgressTask() = %v, want %v; (-want,+got)\n%s",
				tc.desc, got, tc.want, diff)
		}
	}
}

func TestInspectorListScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	createScheduledTask := func(z base.Z) *ScheduledTask {
		msg := z.Message
		return &ScheduledTask{
			Task:          NewTask(msg.Type, msg.Payload),
			ID:            msg.ID.String(),
			Queue:         msg.Queue,
			NextEnqueueAt: time.Unix(z.Score, 0),
		}
	}

	tests := []struct {
		desc      string
		scheduled []base.Z
		want      []*ScheduledTask
	}{
		{
			desc:      "with a few scheduled tasks",
			scheduled: []base.Z{z1, z2, z3},
			// Should be sorted by NextEnqueuedAt.
			want: []*ScheduledTask{
				createScheduledTask(z3),
				createScheduledTask(z1),
				createScheduledTask(z2),
			},
		},
		{
			desc:      "with empty scheduled queue",
			scheduled: []base.Z{},
			want:      []*ScheduledTask(nil),
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedScheduledQueue(t, r, tc.scheduled)

		got, err := inspector.ListScheduledTasks()
		if err != nil {
			t.Errorf("%s; ListScheduledTasks() returned error: %v", tc.desc, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{}, ScheduledTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListScheduledTask() = %v, want %v; (-want,+got)\n%s",
				tc.desc, got, tc.want, diff)
		}
	}
}

func TestInspectorListRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	createRetryTask := func(z base.Z) *RetryTask {
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

	tests := []struct {
		desc  string
		retry []base.Z
		want  []*RetryTask
	}{
		{
			desc:  "with a few retry tasks",
			retry: []base.Z{z1, z2, z3},
			// Should be sorted by NextEnqueuedAt.
			want: []*RetryTask{
				createRetryTask(z3),
				createRetryTask(z1),
				createRetryTask(z2),
			},
		},
		{
			desc:  "with empty retry queue",
			retry: []base.Z{},
			want:  []*RetryTask(nil),
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedRetryQueue(t, r, tc.retry)

		got, err := inspector.ListRetryTasks()
		if err != nil {
			t.Errorf("%s; ListRetryTasks() returned error: %v", tc.desc, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{}, RetryTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListRetryTask() = %v, want %v; (-want,+got)\n%s",
				tc.desc, got, tc.want, diff)
		}
	}
}

func TestInspectorListDeadTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	createDeadTask := func(z base.Z) *DeadTask {
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

	tests := []struct {
		desc  string
		retry []base.Z
		want  []*DeadTask
	}{
		{
			desc:  "with a few dead tasks",
			retry: []base.Z{z1, z2, z3},
			// Should be sorted by LastFailedAt.
			want: []*DeadTask{
				createDeadTask(z2),
				createDeadTask(z1),
				createDeadTask(z3),
			},
		},
		{
			desc:  "with empty dead queue",
			retry: []base.Z{},
			want:  []*DeadTask(nil),
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedDeadQueue(t, r, tc.retry)

		got, err := inspector.ListDeadTasks()
		if err != nil {
			t.Errorf("%s; ListDeadTasks() returned error: %v", tc.desc, err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreUnexported(Payload{}, DeadTask{})
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("%s; ListDeadTask() = %v, want %v; (-want,+got)\n%s",
				tc.desc, got, tc.want, diff)
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
	asynqtest.SeedEnqueuedQueue(t, r, msgs)

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
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled []base.Z
		want      int
	}{
		{
			scheduled: []base.Z{z1, z2, z3},
			want:      3,
		},
		{
			scheduled: []base.Z{},
			want:      0,
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedScheduledQueue(t, r, tc.scheduled)

		got, err := inspector.DeleteAllScheduledTasks()
		if err != nil {
			t.Errorf("DeleteAllScheduledTasks() returned error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllScheduledTasks() = %d, want %d", got, tc.want)
		}
		gotScheduled := asynqtest.GetScheduledEntries(t, r)
		if len(gotScheduled) != 0 {
			t.Errorf("There are still %d entries in dead queue, want empty",
				len(gotScheduled))
		}
	}
}

func TestInspectorDeleteAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry []base.Z
		want  int
	}{
		{
			retry: []base.Z{z1, z2, z3},
			want:  3,
		},
		{
			retry: []base.Z{},
			want:  0,
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedRetryQueue(t, r, tc.retry)

		got, err := inspector.DeleteAllRetryTasks()
		if err != nil {
			t.Errorf("DeleteAllRetryTasks() returned error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllRetryTasks() = %d, want %d", got, tc.want)
		}
		gotRetry := asynqtest.GetRetryEntries(t, r)
		if len(gotRetry) != 0 {
			t.Errorf("There are still %d entries in dead queue, want empty",
				len(gotRetry))
		}
	}
}

func TestInspectorDeleteAllDeadTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		dead []base.Z
		want int
	}{
		{
			dead: []base.Z{z1, z2, z3},
			want: 3,
		},
		{
			dead: []base.Z{},
			want: 0,
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedDeadQueue(t, r, tc.dead)

		got, err := inspector.DeleteAllDeadTasks()
		if err != nil {
			t.Errorf("DeleteAllDeadTasks() returned error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("DeleteAllDeadTasks() = %d, want %d", got, tc.want)
		}
		gotDead := asynqtest.GetDeadEntries(t, r)
		if len(gotDead) != 0 {
			t.Errorf("There are still %d entries in dead queue, want empty",
				len(gotDead))
		}
	}
}

func TestInspectorKillAllScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled []base.Z
		dead      []base.Z
		want      int
		wantDead  []base.Z
	}{
		{
			scheduled: []base.Z{z1, z2, z3},
			dead:      []base.Z{},
			want:      3,
			wantDead: []base.Z{
				base.Z{Message: m1, Score: now.Unix()},
				base.Z{Message: m2, Score: now.Unix()},
				base.Z{Message: m3, Score: now.Unix()},
			},
		},
		{
			scheduled: []base.Z{z1, z2},
			dead:      []base.Z{z3},
			want:      2,
			wantDead: []base.Z{
				z3,
				base.Z{Message: m1, Score: now.Unix()},
				base.Z{Message: m2, Score: now.Unix()},
			},
		},
		{
			scheduled: []base.Z(nil),
			dead:      []base.Z(nil),
			want:      0,
			wantDead:  []base.Z(nil),
		},
		{
			scheduled: []base.Z(nil),
			dead:      []base.Z{z1, z2},
			want:      0,
			wantDead:  []base.Z{z1, z2},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedScheduledQueue(t, r, tc.scheduled)
		asynqtest.SeedDeadQueue(t, r, tc.dead)

		got, err := inspector.KillAllScheduledTasks()
		if err != nil {
			t.Errorf("KillAllScheduledTasks() returned error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("KillAllScheduledTasks() = %d, want %d", got, tc.want)
		}
		gotScheduled := asynqtest.GetScheduledEntries(t, r)
		if len(gotScheduled) != 0 {
			t.Errorf("There are still %d entries in scheduled queue, want empty",
				len(gotScheduled))
		}
		// Allow Z.Score to differ by up to 2.
		approxOpt := cmp.Comparer(func(a, b int64) bool {
			return math.Abs(float64(a-b)) < 2
		})
		gotDead := asynqtest.GetDeadEntries(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, asynqtest.SortZSetEntryOpt, approxOpt); diff != "" {
			t.Errorf("mismatch in %q; (-want,+got)\n%s", base.DeadQueue, diff)
		}
	}
}

func TestInspectorKillAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry    []base.Z
		dead     []base.Z
		want     int
		wantDead []base.Z
	}{
		{
			retry: []base.Z{z1, z2, z3},
			dead:  []base.Z{},
			want:  3,
			wantDead: []base.Z{
				base.Z{Message: m1, Score: now.Unix()},
				base.Z{Message: m2, Score: now.Unix()},
				base.Z{Message: m3, Score: now.Unix()},
			},
		},
		{
			retry: []base.Z{z1, z2},
			dead:  []base.Z{z3},
			want:  2,
			wantDead: []base.Z{
				z3,
				base.Z{Message: m1, Score: now.Unix()},
				base.Z{Message: m2, Score: now.Unix()},
			},
		},
		{
			retry:    []base.Z(nil),
			dead:     []base.Z(nil),
			want:     0,
			wantDead: []base.Z(nil),
		},
		{
			retry:    []base.Z(nil),
			dead:     []base.Z{z1, z2},
			want:     0,
			wantDead: []base.Z{z1, z2},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedRetryQueue(t, r, tc.retry)
		asynqtest.SeedDeadQueue(t, r, tc.dead)

		got, err := inspector.KillAllRetryTasks()
		if err != nil {
			t.Errorf("KillAllRetryTasks() returned error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("KillAllRetryTasks() = %d, want %d", got, tc.want)
		}
		gotRetry := asynqtest.GetRetryEntries(t, r)
		if len(gotRetry) != 0 {
			t.Errorf("There are still %d entries in retry queue, want empty",
				len(gotRetry))
		}
		gotDead := asynqtest.GetDeadEntries(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch in %q; (-want,+got)\n%s", base.DeadQueue, diff)
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

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled    []base.Z
		enqueued     map[string][]*base.TaskMessage
		want         int
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			scheduled: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m2},
				"low":      {m3},
			},
		},
		{
			scheduled: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {m2},
				"low":      {m3},
			},
		},
		{
			scheduled: []base.Z{},
			enqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			want: 0,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedScheduledQueue(t, r, tc.scheduled)
		for q, msgs := range tc.enqueued {
			asynqtest.SeedEnqueuedQueue(t, r, msgs, q)
		}

		got, err := inspector.EnqueueAllScheduledTasks()
		if err != nil {
			t.Errorf("EnqueueAllScheduledTasks() returned error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("EnqueueAllScheduledTasks() = %d, want %d", got, tc.want)
		}
		gotScheduled := asynqtest.GetScheduledEntries(t, r)
		if len(gotScheduled) != 0 {
			t.Errorf("There are still %d entries in scheduled queue, want empty",
				len(gotScheduled))
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
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

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry        []base.Z
		enqueued     map[string][]*base.TaskMessage
		want         int
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			retry: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m2},
				"low":      {m3},
			},
		},
		{
			retry: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {m2},
				"low":      {m3},
			},
		},
		{
			retry: []base.Z{},
			enqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			want: 0,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedRetryQueue(t, r, tc.retry)
		for q, msgs := range tc.enqueued {
			asynqtest.SeedEnqueuedQueue(t, r, msgs, q)
		}

		got, err := inspector.EnqueueAllRetryTasks()
		if err != nil {
			t.Errorf("EnqueueAllRetryTasks() returned error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("EnqueueAllRetryTasks() = %d, want %d", got, tc.want)
		}
		gotRetry := asynqtest.GetRetryEntries(t, r)
		if len(gotRetry) != 0 {
			t.Errorf("There are still %d entries in retry queue, want empty",
				len(gotRetry))
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
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

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		dead         []base.Z
		enqueued     map[string][]*base.TaskMessage
		want         int
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			dead: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m1},
				"critical": {m2},
				"low":      {m3},
			},
		},
		{
			dead: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {m4},
				"critical": {},
				"low":      {},
			},
			want: 3,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {m4, m1},
				"critical": {m2},
				"low":      {m3},
			},
		},
		{
			dead: []base.Z{},
			enqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
			want: 0,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {m1, m4},
			},
		},
	}

	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedDeadQueue(t, r, tc.dead)
		for q, msgs := range tc.enqueued {
			asynqtest.SeedEnqueuedQueue(t, r, msgs, q)
		}

		got, err := inspector.EnqueueAllDeadTasks()
		if err != nil {
			t.Errorf("EnqueueAllDeadTasks() returned error: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("EnqueueAllDeadTasks() = %d, want %d", got, tc.want)
		}
		gotDead := asynqtest.GetDeadEntries(t, r)
		if len(gotDead) != 0 {
			t.Errorf("There are still %d entries in dead queue, want empty",
				len(gotDead))
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
			}
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled     []base.Z
		target        *base.TaskMessage
		wantScheduled []base.Z
	}{
		{
			scheduled:     []base.Z{z1, z2, z3},
			target:        m2,
			wantScheduled: []base.Z{z1, z3},
		},
	}

loop:
	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedScheduledQueue(t, r, tc.scheduled)

		tasks, err := inspector.ListScheduledTasks()
		if err != nil {
			t.Errorf("ListScheduledTasks() returned error: %v", err)
			continue
		}
		for _, task := range tasks {
			if task.ID == tc.target.ID.String() {
				if err := inspector.DeleteTaskByKey(task.Key()); err != nil {
					t.Errorf("DeleteTaskByKey(%q) returned error: %v",
						task.Key(), err)
					continue loop
				}
			}
		}
		gotScheduled := asynqtest.GetScheduledEntries(t, r)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.ScheduledQueue, diff)
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesRetryTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry     []base.Z
		target    *base.TaskMessage
		wantRetry []base.Z
	}{
		{
			retry:     []base.Z{z1, z2, z3},
			target:    m2,
			wantRetry: []base.Z{z1, z3},
		},
	}

loop:
	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedRetryQueue(t, r, tc.retry)

		tasks, err := inspector.ListRetryTasks()
		if err != nil {
			t.Errorf("ListRetryTasks() returned error: %v", err)
			continue
		}
		for _, task := range tasks {
			if task.ID == tc.target.ID.String() {
				if err := inspector.DeleteTaskByKey(task.Key()); err != nil {
					t.Errorf("DeleteTaskByKey(%q) returned error: %v",
						task.Key(), err)
					continue loop
				}
			}
		}
		gotRetry := asynqtest.GetRetryEntries(t, r)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.RetryQueue, diff)
		}
	}
}

func TestInspectorDeleteTaskByKeyDeletesDeadTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessage("task2", nil)
	m3 := asynqtest.NewTaskMessage("task3", nil)
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(-5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(-15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(-2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		dead     []base.Z
		target   *base.TaskMessage
		wantDead []base.Z
	}{
		{
			dead:     []base.Z{z1, z2, z3},
			target:   m2,
			wantDead: []base.Z{z1, z3},
		},
	}

loop:
	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedDeadQueue(t, r, tc.dead)

		tasks, err := inspector.ListDeadTasks()
		if err != nil {
			t.Errorf("ListDeadTasks() returned error: %v", err)
			continue
		}
		for _, task := range tasks {
			if task.ID == tc.target.ID.String() {
				if err := inspector.DeleteTaskByKey(task.Key()); err != nil {
					t.Errorf("DeleteTaskByKey(%q) returned error: %v",
						task.Key(), err)
					continue loop
				}
			}
		}
		gotDead := asynqtest.GetDeadEntries(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.DeadQueue, diff)
		}
	}
}

func TestInspectorEnqueueTaskByKeyEnqueuesScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled     []base.Z
		enqueued      map[string][]*base.TaskMessage
		target        *base.TaskMessage
		wantScheduled []base.Z
		wantEnqueued  map[string][]*base.TaskMessage
	}{
		{
			scheduled: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			target:        m2,
			wantScheduled: []base.Z{z1, z3},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {m2},
				"low":      {},
			},
		},
	}

loop:
	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedScheduledQueue(t, r, tc.scheduled)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		tasks, err := inspector.ListScheduledTasks()
		if err != nil {
			t.Errorf("ListScheduledTasks() returned error: %v", err)
			continue
		}
		for _, task := range tasks {
			if task.ID == tc.target.ID.String() {
				if err := inspector.EnqueueTaskByKey(task.Key()); err != nil {
					t.Errorf("EnqueueTaskByKey(%q) returned error: %v",
						task.Key(), err)
					continue loop
				}
			}
		}
		gotScheduled := asynqtest.GetScheduledEntries(t, r)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s", base.ScheduledQueue, diff)
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
			}
		}
	}
}

func TestInspectorEnqueueTaskByKeyEnqueuesRetryTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry        []base.Z
		enqueued     map[string][]*base.TaskMessage
		target       *base.TaskMessage
		wantRetry    []base.Z
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			retry: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			target:    m2,
			wantRetry: []base.Z{z1, z3},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {m2},
				"low":      {},
			},
		},
	}

loop:
	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedRetryQueue(t, r, tc.retry)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		tasks, err := inspector.ListRetryTasks()
		if err != nil {
			t.Errorf("ListRetryTasks() returned error: %v", err)
			continue
		}
		for _, task := range tasks {
			if task.ID == tc.target.ID.String() {
				if err := inspector.EnqueueTaskByKey(task.Key()); err != nil {
					t.Errorf("EnqueueTaskByKey(%q) returned error: %v",
						task.Key(), err)
					continue loop
				}
			}
		}
		gotRetry := asynqtest.GetRetryEntries(t, r)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s", base.RetryQueue, diff)
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
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
		dead         []base.Z
		enqueued     map[string][]*base.TaskMessage
		target       *base.TaskMessage
		wantDead     []base.Z
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			dead: []base.Z{z1, z2, z3},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			target:   m2,
			wantDead: []base.Z{z1, z3},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {m2},
				"low":      {},
			},
		},
	}

loop:
	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedDeadQueue(t, r, tc.dead)
		asynqtest.SeedAllEnqueuedQueues(t, r, tc.enqueued)

		tasks, err := inspector.ListDeadTasks()
		if err != nil {
			t.Errorf("ListDeadTasks() returned error: %v", err)
			continue
		}
		for _, task := range tasks {
			if task.ID == tc.target.ID.String() {
				if err := inspector.EnqueueTaskByKey(task.Key()); err != nil {
					t.Errorf("EnqueueTaskByKey(%q) returned error: %v",
						task.Key(), err)
					continue loop
				}
			}
		}
		gotDead := asynqtest.GetDeadEntries(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s", base.DeadQueue, diff)
		}
		for qname, want := range tc.wantEnqueued {
			gotEnqueued := asynqtest.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, asynqtest.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
			}
		}
	}
}

func TestInspectorKillTaskByKeyKillsScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		scheduled     []base.Z
		dead          []base.Z
		target        *base.TaskMessage
		wantScheduled []base.Z
		wantDead      []base.Z
	}{
		{
			scheduled:     []base.Z{z1, z2, z3},
			dead:          []base.Z{},
			target:        m2,
			wantScheduled: []base.Z{z1, z3},
			wantDead: []base.Z{
				{m2, now.Unix()},
			},
		},
	}

loop:
	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedScheduledQueue(t, r, tc.scheduled)
		asynqtest.SeedDeadQueue(t, r, tc.dead)

		tasks, err := inspector.ListScheduledTasks()
		if err != nil {
			t.Errorf("ListScheduledTasks() returned error: %v", err)
			continue
		}
		for _, task := range tasks {
			if task.ID == tc.target.ID.String() {
				if err := inspector.KillTaskByKey(task.Key()); err != nil {
					t.Errorf("KillTaskByKey(%q) returned error: %v",
						task.Key(), err)
					continue loop
				}
			}
		}
		gotScheduled := asynqtest.GetScheduledEntries(t, r)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s", base.ScheduledQueue, diff)
		}
		gotDead := asynqtest.GetDeadEntries(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s", base.DeadQueue, diff)
		}

	}
}

func TestInspectorKillTaskByKeyKillsRetryTask(t *testing.T) {
	r := setup(t)
	m1 := asynqtest.NewTaskMessage("task1", nil)
	m2 := asynqtest.NewTaskMessageWithQueue("task2", nil, "critical")
	m3 := asynqtest.NewTaskMessageWithQueue("task3", nil, "low")
	now := time.Now()
	z1 := base.Z{Message: m1, Score: now.Add(5 * time.Minute).Unix()}
	z2 := base.Z{Message: m2, Score: now.Add(15 * time.Minute).Unix()}
	z3 := base.Z{Message: m3, Score: now.Add(2 * time.Minute).Unix()}

	inspector := NewInspector(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		retry     []base.Z
		dead      []base.Z
		target    *base.TaskMessage
		wantRetry []base.Z
		wantDead  []base.Z
	}{
		{
			retry:     []base.Z{z1, z2, z3},
			dead:      []base.Z{},
			target:    m2,
			wantRetry: []base.Z{z1, z3},
			wantDead: []base.Z{
				{m2, now.Unix()},
			},
		},
	}

loop:
	for _, tc := range tests {
		asynqtest.FlushDB(t, r)
		asynqtest.SeedRetryQueue(t, r, tc.retry)
		asynqtest.SeedDeadQueue(t, r, tc.dead)

		tasks, err := inspector.ListRetryTasks()
		if err != nil {
			t.Errorf("ListRetryTasks() returned error: %v", err)
			continue
		}
		for _, task := range tasks {
			if task.ID == tc.target.ID.String() {
				if err := inspector.KillTaskByKey(task.Key()); err != nil {
					t.Errorf("KillTaskByKey(%q) returned error: %v",
						task.Key(), err)
					continue loop
				}
			}
		}
		gotRetry := asynqtest.GetRetryEntries(t, r)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s", base.RetryQueue, diff)
		}
		gotDead := asynqtest.GetDeadEntries(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, asynqtest.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s", base.DeadQueue, diff)
		}

	}
}
