// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

// TODO(hibiken): Get Redis address and db number from ENV variables.
func setup(t *testing.T) *RDB {
	t.Helper()
	r := NewRDB(redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   13,
	}))
	// Start each test with a clean slate.
	h.FlushDB(t, r.client)
	return r
}

func TestEnqueue(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"})
	t2 := h.NewTaskMessage("generate_csv", map[string]interface{}{})
	t2.Queue = "csv"
	t3 := h.NewTaskMessage("sync", nil)
	t3.Queue = "low"

	tests := []struct {
		msg *base.TaskMessage
	}{
		{t1},
		{t2},
		{t3},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case.

		err := r.Enqueue(tc.msg)
		if err != nil {
			t.Errorf("(*RDB).Enqueue(msg) = %v, want nil", err)
		}

		qkey := base.QueueKey(tc.msg.Queue)
		gotEnqueued := h.GetEnqueuedMessages(t, r.client, tc.msg.Queue)
		if len(gotEnqueued) != 1 {
			t.Errorf("%q has length %d, want 1", qkey, len(gotEnqueued))
			continue
		}
		if diff := cmp.Diff(tc.msg, gotEnqueued[0]); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
		if !r.client.SIsMember(base.AllQueues, qkey).Val() {
			t.Errorf("%q is not a member of SET %q", qkey, base.AllQueues)
		}
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello!"})
	t2 := h.NewTaskMessage("export_csv", nil)
	t3 := h.NewTaskMessage("reindex", nil)

	tests := []struct {
		enqueued       map[string][]*base.TaskMessage
		args           []string // list of queues to query
		want           *base.TaskMessage
		err            error
		wantEnqueued   map[string][]*base.TaskMessage
		wantInProgress []*base.TaskMessage
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			args: []string{"default"},
			want: t1,
			err:  nil,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantInProgress: []*base.TaskMessage{t1},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			args: []string{"default"},
			want: nil,
			err:  ErrNoProcessableTask,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantInProgress: []*base.TaskMessage{},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
				"low":      {t3},
			},
			args: []string{"critical", "default", "low"},
			want: t2,
			err:  nil,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {t3},
			},
			wantInProgress: []*base.TaskMessage{t2},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {t2, t3},
			},
			args: []string{"critical", "default", "low"},
			want: t1,
			err:  nil,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {t2, t3},
			},
			wantInProgress: []*base.TaskMessage{t1},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			args: []string{"critical", "default", "low"},
			want: nil,
			err:  ErrNoProcessableTask,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantInProgress: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		for queue, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r.client, msgs, queue)
		}

		got, err := r.Dequeue(tc.args...)
		if !cmp.Equal(got, tc.want) || err != tc.err {
			t.Errorf("(*RDB).Dequeue(%v) = %v, %v; want %v, %v",
				tc.args, got, err, tc.want, tc.err)
			continue
		}

		for queue, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.QueueKey(queue), diff)
			}
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.InProgressQueue, diff)
		}
	}
}

func TestDone(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("export_csv", nil)

	tests := []struct {
		inProgress     []*base.TaskMessage // initial state of the in-progress list
		target         *base.TaskMessage   // task to remove
		wantInProgress []*base.TaskMessage // final state of the in-progress list
	}{
		{
			inProgress:     []*base.TaskMessage{t1, t2},
			target:         t1,
			wantInProgress: []*base.TaskMessage{t2},
		},
		{
			inProgress:     []*base.TaskMessage{t1},
			target:         t1,
			wantInProgress: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedInProgressQueue(t, r.client, tc.inProgress)

		err := r.Done(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Done(task) = %v, want nil", err)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.InProgressQueue, diff)
			continue
		}

		processedKey := base.ProcessedKey(time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}

		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}
	}
}

func TestRequeue(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("export_csv", nil)
	t3 := h.NewTaskMessageWithQueue("send_email", nil, "critical")

	tests := []struct {
		enqueued       map[string][]*base.TaskMessage // initial state of queues
		inProgress     []*base.TaskMessage            // initial state of the in-progress list
		target         *base.TaskMessage              // task to requeue
		wantEnqueued   map[string][]*base.TaskMessage // final state of queues
		wantInProgress []*base.TaskMessage            // final state of the in-progress list
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
			inProgress: []*base.TaskMessage{t1, t2},
			target:     t1,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
			},
			wantInProgress: []*base.TaskMessage{t2},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
			},
			inProgress: []*base.TaskMessage{t2},
			target:     t2,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2},
			},
			wantInProgress: []*base.TaskMessage{},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
				"critical":            {},
			},
			inProgress: []*base.TaskMessage{t2, t3},
			target:     t3,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
				"critical":            {t3},
			},
			wantInProgress: []*base.TaskMessage{t2},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r.client, msgs, qname)
		}
		h.SeedInProgressQueue(t, r.client, tc.inProgress)

		err := r.Requeue(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Requeue(task) = %v, want nil", err)
			continue
		}

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.QueueKey(qname), diff)
			}
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.InProgressQueue, diff)
		}
	}
}

func TestSchedule(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	tests := []struct {
		msg       *base.TaskMessage
		processAt time.Time
	}{
		{t1, time.Now().Add(15 * time.Minute)},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case

		desc := fmt.Sprintf("(*RDB).Schedule(%v, %v)", tc.msg, tc.processAt)
		err := r.Schedule(tc.msg, tc.processAt)
		if err != nil {
			t.Errorf("%s = %v, want nil", desc, err)
			continue
		}

		gotScheduled := h.GetScheduledEntries(t, r.client)
		if len(gotScheduled) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted", desc, len(gotScheduled), base.ScheduledQueue)
			continue
		}
		if int64(gotScheduled[0].Score) != tc.processAt.Unix() {
			t.Errorf("%s inserted an item with score %d, want %d", desc, int64(gotScheduled[0].Score), tc.processAt.Unix())
			continue
		}
	}
}

func TestRetry(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", map[string]interface{}{"subject": "Hola!"})
	t2 := h.NewTaskMessage("gen_thumbnail", map[string]interface{}{"path": "some/path/to/image.jpg"})
	t3 := h.NewTaskMessage("reindex", nil)
	t1.Retried = 10
	errMsg := "SMTP server is not responding"
	t1AfterRetry := &base.TaskMessage{
		ID:       t1.ID,
		Type:     t1.Type,
		Payload:  t1.Payload,
		Queue:    t1.Queue,
		Retry:    t1.Retry,
		Retried:  t1.Retried + 1,
		ErrorMsg: errMsg,
	}
	now := time.Now()

	tests := []struct {
		inProgress     []*base.TaskMessage
		retry          []h.ZSetEntry
		msg            *base.TaskMessage
		processAt      time.Time
		errMsg         string
		wantInProgress []*base.TaskMessage
		wantRetry      []h.ZSetEntry
	}{
		{
			inProgress: []*base.TaskMessage{t1, t2},
			retry: []h.ZSetEntry{
				{
					Msg:   t3,
					Score: float64(now.Add(time.Minute).Unix()),
				},
			},
			msg:            t1,
			processAt:      now.Add(5 * time.Minute),
			errMsg:         errMsg,
			wantInProgress: []*base.TaskMessage{t2},
			wantRetry: []h.ZSetEntry{
				{
					Msg:   t1AfterRetry,
					Score: float64(now.Add(5 * time.Minute).Unix()),
				},
				{
					Msg:   t3,
					Score: float64(now.Add(time.Minute).Unix()),
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		h.SeedRetryQueue(t, r.client, tc.retry)

		err := r.Retry(tc.msg, tc.processAt, tc.errMsg)
		if err != nil {
			t.Errorf("(*RDB).Retry = %v, want nil", err)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.InProgressQueue, diff)
		}

		gotRetry := h.GetRetryEntries(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}

		processedKey := base.ProcessedKey(time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}
		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		failureKey := base.FailureKey(time.Now())
		gotFailure := r.client.Get(failureKey).Val()
		if gotFailure != "1" {
			t.Errorf("GET %q = %q, want 1", failureKey, gotFailure)
		}
		gotTTL = r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", failureKey, gotTTL, statsTTL)
		}
	}
}

func TestKill(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("reindex", nil)
	t3 := h.NewTaskMessage("generate_csv", nil)
	errMsg := "SMTP server not responding"
	t1AfterKill := &base.TaskMessage{
		ID:       t1.ID,
		Type:     t1.Type,
		Payload:  t1.Payload,
		Queue:    t1.Queue,
		Retry:    t1.Retry,
		Retried:  t1.Retried,
		ErrorMsg: errMsg,
	}
	now := time.Now()

	// TODO(hibiken): add test cases for trimming
	tests := []struct {
		inProgress     []*base.TaskMessage
		dead           []h.ZSetEntry
		target         *base.TaskMessage // task to kill
		wantInProgress []*base.TaskMessage
		wantDead       []h.ZSetEntry
	}{
		{
			inProgress: []*base.TaskMessage{t1, t2},
			dead: []h.ZSetEntry{
				{
					Msg:   t3,
					Score: float64(now.Add(-time.Hour).Unix()),
				},
			},
			target:         t1,
			wantInProgress: []*base.TaskMessage{t2},
			wantDead: []h.ZSetEntry{
				{
					Msg:   t1AfterKill,
					Score: float64(now.Unix()),
				},
				{
					Msg:   t3,
					Score: float64(now.Add(-time.Hour).Unix()),
				},
			},
		},
		{
			inProgress:     []*base.TaskMessage{t1, t2, t3},
			dead:           []h.ZSetEntry{},
			target:         t1,
			wantInProgress: []*base.TaskMessage{t2, t3},
			wantDead: []h.ZSetEntry{
				{
					Msg:   t1AfterKill,
					Score: float64(now.Unix()),
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		h.SeedDeadQueue(t, r.client, tc.dead)

		err := r.Kill(tc.target, errMsg)
		if err != nil {
			t.Errorf("(*RDB).Kill(%v, %v) = %v, want nil", tc.target, errMsg, err)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got)\n%s", base.InProgressQueue, diff)
		}

		gotDead := h.GetDeadEntries(t, r.client)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*RDB).Kill: (-want, +got):\n%s", base.DeadQueue, diff)
		}

		processedKey := base.ProcessedKey(time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}
		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		failureKey := base.FailureKey(time.Now())
		gotFailure := r.client.Get(failureKey).Val()
		if gotFailure != "1" {
			t.Errorf("GET %q = %q, want 1", failureKey, gotFailure)
		}
		gotTTL = r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", failureKey, gotTTL, statsTTL)
		}
	}
}

func TestRestoreUnfinished(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("export_csv", nil)
	t3 := h.NewTaskMessage("sync_stuff", nil)
	t4 := h.NewTaskMessageWithQueue("important", nil, "critical")
	t5 := h.NewTaskMessageWithQueue("minor", nil, "low")

	tests := []struct {
		inProgress     []*base.TaskMessage
		enqueued       map[string][]*base.TaskMessage
		want           int64
		wantInProgress []*base.TaskMessage
		wantEnqueued   map[string][]*base.TaskMessage
	}{
		{
			inProgress: []*base.TaskMessage{t1, t2, t3},
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
			want:           3,
			wantInProgress: []*base.TaskMessage{},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			inProgress: []*base.TaskMessage{},
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
			want:           0,
			wantInProgress: []*base.TaskMessage{},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			inProgress: []*base.TaskMessage{t2, t3},
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
			},
			want:           2,
			wantInProgress: []*base.TaskMessage{},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
			},
		},
		{
			inProgress: []*base.TaskMessage{t2, t3, t4, t5},
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
				"critical":            {},
				"low":                 {},
			},
			want:           4,
			wantInProgress: []*base.TaskMessage{},
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2, t3},
				"critical":            {t4},
				"low":                 {t5},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r.client, msgs, qname)
		}

		got, err := r.RestoreUnfinished()
		if got != tc.want || err != nil {
			t.Errorf("(*RDB).RestoreUnfinished() = %v %v, want %v nil", got, err, tc.want)
			continue
		}

		gotInProgress := h.GetInProgressMessages(t, r.client)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.InProgressQueue, diff)
		}

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.QueueKey(qname), diff)
			}
		}
	}
}

func TestCheckAndEnqueue(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessage("send_email", nil)
	t2 := h.NewTaskMessage("generate_csv", nil)
	t3 := h.NewTaskMessage("gen_thumbnail", nil)
	t4 := h.NewTaskMessage("important_task", nil)
	t4.Queue = "critical"
	t5 := h.NewTaskMessage("minor_task", nil)
	t5.Queue = "low"
	secondAgo := time.Now().Add(-time.Second)
	hourFromNow := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []h.ZSetEntry
		retry         []h.ZSetEntry
		qnames        []string
		wantEnqueued  map[string][]*base.TaskMessage
		wantScheduled []*base.TaskMessage
		wantRetry     []*base.TaskMessage
	}{
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(secondAgo.Unix())},
				{Msg: t2, Score: float64(secondAgo.Unix())},
			},
			retry: []h.ZSetEntry{
				{Msg: t3, Score: float64(secondAgo.Unix())}},
			qnames: []string{"default"},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			wantScheduled: []*base.TaskMessage{},
			wantRetry:     []*base.TaskMessage{},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(hourFromNow.Unix())},
				{Msg: t2, Score: float64(secondAgo.Unix())}},
			retry: []h.ZSetEntry{
				{Msg: t3, Score: float64(secondAgo.Unix())}},
			qnames: []string{"default"},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {t2, t3},
			},
			wantScheduled: []*base.TaskMessage{t1},
			wantRetry:     []*base.TaskMessage{},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(hourFromNow.Unix())},
				{Msg: t2, Score: float64(hourFromNow.Unix())}},
			retry: []h.ZSetEntry{
				{Msg: t3, Score: float64(hourFromNow.Unix())}},
			qnames: []string{"default"},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: []*base.TaskMessage{t1, t2},
			wantRetry:     []*base.TaskMessage{t3},
		},
		{
			scheduled: []h.ZSetEntry{
				{Msg: t1, Score: float64(secondAgo.Unix())},
				{Msg: t4, Score: float64(secondAgo.Unix())},
			},
			retry: []h.ZSetEntry{
				{Msg: t5, Score: float64(secondAgo.Unix())}},
			qnames: []string{"default", "critical", "low"},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t4},
				"low":      {t5},
			},
			wantScheduled: []*base.TaskMessage{},
			wantRetry:     []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedScheduledQueue(t, r.client, tc.scheduled)
		h.SeedRetryQueue(t, r.client, tc.retry)

		err := r.CheckAndEnqueue(tc.qnames...)
		if err != nil {
			t.Errorf("(*RDB).CheckScheduled() = %v, want nil", err)
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
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledQueue, diff)
		}

		gotRetry := h.GetRetryMessages(t, r.client)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}

func TestReadWriteClearProcessInfo(t *testing.T) {
	r := setup(t)
	pinfo := &base.ProcessInfo{
		Concurrency:       10,
		Queues:            map[string]uint{"default": 2, "email": 5, "low": 1},
		PID:               98765,
		Host:              "localhost",
		State:             "running",
		Started:           time.Now(),
		ActiveWorkerCount: 1,
	}

	tests := []struct {
		pi  *base.ProcessInfo
		ttl time.Duration
	}{
		{pinfo, 5 * time.Second},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)

		err := r.WriteProcessInfo(tc.pi, tc.ttl)
		if err != nil {
			t.Errorf("r.WriteProcessInfo returned an error: %v", err)
			continue
		}

		got, err := r.ReadProcessInfo(tc.pi.Host, tc.pi.PID)
		if err != nil {
			t.Errorf("r.ReadProcessInfo returned an error: %v", err)
			continue
		}

		ignoreOpt := cmpopts.IgnoreUnexported(base.ProcessInfo{})
		if diff := cmp.Diff(tc.pi, got, ignoreOpt); diff != "" {
			t.Errorf("r.ReadProcessInfo(%q, %d) = %+v, want %+v; (-want,+got)\n%s",
				tc.pi.Host, tc.pi.PID, got, tc.pi, diff)
		}

		key := base.ProcessInfoKey(tc.pi.Host, tc.pi.PID)
		gotTTL := r.client.TTL(key).Val()
		if !cmp.Equal(tc.ttl, gotTTL, timeCmpOpt) {
			t.Errorf("redis TTL %q returned %v, want %v", key, gotTTL, tc.ttl)
		}

		now := time.Now().UTC()
		allKeys, err := r.client.ZRangeByScore(base.AllProcesses, &redis.ZRangeBy{
			Min: strconv.Itoa(int(now.Unix())),
			Max: "+inf",
		}).Result()
		if err != nil {
			t.Errorf("redis ZRANGEBYSCORE %q %d +inf returned an error: %v",
				base.AllProcesses, now.Unix(), err)
			continue
		}

		wantAllKeys := []string{key}
		if diff := cmp.Diff(wantAllKeys, allKeys); diff != "" {
			t.Errorf("all keys = %v, want %v; (-want,+got)\n%s", allKeys, wantAllKeys, diff)
		}

		if err := r.ClearProcessInfo(tc.pi); err != nil {
			t.Errorf("r.ClearProcessInfo returned an error: %v", err)
			continue
		}

		// 1 means key exists
		if r.client.Exists(key).Val() == 1 {
			t.Errorf("expected %q to be deleted", key)
		}

		if r.client.ZCard(base.AllProcesses).Val() != 0 {
			t.Errorf("expected %q to be empty", base.AllProcesses)
		}

	}
}
