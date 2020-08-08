// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
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
	t2 := h.NewTaskMessageWithQueue("generate_csv", map[string]interface{}{}, "csv")
	t3 := h.NewTaskMessageWithQueue("sync", nil, "low")

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

		gotEnqueued := h.GetEnqueuedMessages(t, r.client, tc.msg.Queue)
		if len(gotEnqueued) != 1 {
			t.Errorf("%q has length %d, want 1", base.QueueKey(tc.msg.Queue), len(gotEnqueued))
			continue
		}
		if diff := cmp.Diff(tc.msg, gotEnqueued[0]); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
		}
	}
}

func TestEnqueueUnique(t *testing.T) {
	r := setup(t)
	m1 := base.TaskMessage{
		ID:        uuid.New(),
		Type:      "email",
		Payload:   map[string]interface{}{"user_id": 123},
		Queue:     base.DefaultQueueName,
		UniqueKey: base.UniqueKey(base.DefaultQueueName, "email", map[string]interface{}{"user_id": 123}),
	}

	tests := []struct {
		msg *base.TaskMessage
		ttl time.Duration // uniqueness ttl
	}{
		{&m1, time.Minute},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case.

		err := r.EnqueueUnique(tc.msg, tc.ttl)
		if err != nil {
			t.Errorf("First message: (*RDB).EnqueueUnique(%v, %v) = %v, want nil",
				tc.msg, tc.ttl, err)
			continue
		}

		got := r.EnqueueUnique(tc.msg, tc.ttl)
		if got != ErrDuplicateTask {
			t.Errorf("Second message: (*RDB).EnqueueUnique(%v, %v) = %v, want %v",
				tc.msg, tc.ttl, got, ErrDuplicateTask)
			continue
		}
		gotTTL := r.client.TTL(tc.msg.UniqueKey).Val()
		if !cmp.Equal(tc.ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL %q = %v, want %v", tc.msg.UniqueKey, gotTTL, tc.ttl)
			continue
		}
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
		}
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "send_email",
		Payload:  map[string]interface{}{"subject": "hello!"},
		Timeout:  1800,
		Deadline: 0,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "export_csv",
		Payload:  nil,
		Timeout:  0,
		Deadline: 1593021600,
	}
	t2Deadline := t2.Deadline
	t3 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "reindex",
		Payload:  nil,
		Timeout:  int64((5 * time.Minute).Seconds()),
		Deadline: time.Now().Add(10 * time.Minute).Unix(),
	}
	t3Deadline := now.Unix() + t3.Timeout // use whichever is earliest

	tests := []struct {
		enqueued       map[string][]*base.TaskMessage
		args           []string // list of queues to query
		wantMsg        *base.TaskMessage
		wantDeadline   time.Time
		err            error
		wantEnqueued   map[string][]*base.TaskMessage
		wantInProgress map[string][]*base.TaskMessage
		wantDeadlines  map[string][]base.Z
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			args:         []string{"default"},
			wantMsg:      t1,
			wantDeadline: time.Unix(t1Deadline, 0),
			err:          nil,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
			},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			args:         []string{"default"},
			wantMsg:      nil,
			wantDeadline: time.Time{},
			err:          ErrNoProcessableTask,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
				"low":      {t3},
			},
			args:         []string{"critical", "default", "low"},
			wantMsg:      t2,
			wantDeadline: time.Unix(t2Deadline, 0),
			err:          nil,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
				"low":      {t3},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t2},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {},
				"critical": {{Message: t2, Score: t2Deadline}},
				"low":      {},
			},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default":  {t3},
				"critical": {},
				"low":      {t2, t1},
			},
			args:         []string{"critical", "default", "low"},
			wantMsg:      t3,
			wantDeadline: time.Unix(t3Deadline, 0),
			err:          nil,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {t2, t1},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {t3},
				"critical": {},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {{Message: t3, Score: t3Deadline}},
				"critical": {},
				"low":      {},
			},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			args:         []string{"critical", "default", "low"},
			wantMsg:      nil,
			wantDeadline: time.Time{},
			err:          ErrNoProcessableTask,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
				"low":      {},
			},
			wantDeadlines: map[string][]base.Z{
				"default":  {},
				"critical": {},
				"low":      {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllEnqueuedQueues(t, r.client, msgs, queue, tc.enqueued)

		gotMsg, gotDeadline, err := r.Dequeue(tc.args...)
		if err != tc.err {
			t.Errorf("(*RDB).Dequeue(%v) returned error %v; want %v",
				tc.args, err, tc.err)
			continue
		}
		if !cmp.Equal(gotMsg, tc.wantMsg) || err != tc.err {
			t.Errorf("(*RDB).Dequeue(%v) returned message %v; want %v",
				tc.args, gotMsg, tc.wantMsg)
			continue
		}
		if gotDeadline != tc.wantDeadline {
			t.Errorf("(*RDB).Dequeue(%v) returned deadline %v; want %v",
				tc.args, gotDeadline, tc.wantDeadline)
			continue
		}

		for queue, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.QueueKey(queue), diff)
			}
		}
		for queue, want := range tc.wantInProgress {
			gotInProgress := h.GetInProgressMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotInProgress, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.InProgressKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.DeadlinesKey(queue), diff)
			}
		}
	}
}

func TestDequeueIgnoresPausedQueues(t *testing.T) {
	r := setup(t)
	t1 := h.NewTaskMessageWithQueue("send_email", map[string]interface{}{"subject": "hello!"}, "default")
	t2 := h.NewTaskMessageWithQueue("export_csv", nil, "critical")

	tests := []struct {
		paused         []string // list of paused queues
		enqueued       map[string][]*base.TaskMessage
		args           []string // list of queues to query
		wantMsg        *base.TaskMessage
		err            error
		wantEnqueued   map[string][]*base.TaskMessage
		wantInProgress map[string][]*base.TaskMessage
	}{
		{
			paused: []string{"default"},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			args:    []string{"default", "critical"},
			wantMsg: t2,
			err:     nil,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {t2},
			},
		},
		{
			paused: []string{"default"},
			enqueued: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			args:    []string{"default"},
			wantMsg: nil,
			err:     ErrNoProcessableTask,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {},
			},
		},
		{
			paused: []string{"critical", "default"},
			enqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			args:    []string{"default", "critical"},
			wantMsg: nil,
			err:     ErrNoProcessableTask,
			wantEnqueued: map[string][]*base.TaskMessage{
				"default":  {t1},
				"critical": {t2},
			},
			wantInProgress: map[string][]*base.TaskMessage{
				"default":  {},
				"critical": {},
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
		h.SeedAllEnqueuedQueues(t, r.client, msgs, queue, tc.enqueued)

		got, _, err := r.Dequeue(tc.args...)
		if !cmp.Equal(got, tc.wantMsg) || err != tc.err {
			t.Errorf("Dequeue(%v) = %v, %v; want %v, %v",
				tc.args, got, err, tc.wantMsg, tc.err)
			continue
		}

		for queue, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotEnqueued, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.QueueKey(queue), diff)
			}
		}
		for queue, want := range tc.wantInProgress {
			gotInProgress := h.GetInProgressMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotInProgress, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.InProgressKey(queue), diff)
			}
		}
	}
}

func TestDone(t *testing.T) {
	r := setup(t)
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "send_email",
		Payload:  nil,
		Timeout:  1800,
		Deadline: 0,
		Queue:    "default",
	}
	t2 := &base.TaskMessage{
		ID:       uuid.New(),
		Type:     "export_csv",
		Payload:  nil,
		Timeout:  0,
		Deadline: 1592485787,
		Queue:    "custom",
	}
	t3 := &base.TaskMessage{
		ID:        uuid.New(),
		Type:      "reindex",
		Payload:   nil,
		Timeout:   1800,
		Deadline:  0,
		UniqueKey: "reindex:nil:default",
		Queue:     "default",
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := t2.Deadline
	t3Deadline := now.Unix() + t3.Deadline

	tests := []struct {
		inProgress     map[string][]*base.TaskMessage // initial state of the in-progress list
		deadlines      map[string][]base.Z            // initial state of deadlines set
		target         *base.TaskMessage              // task to remove
		wantInProgress map[string][]*base.TaskMessage // final state of the in-progress list
		wantDeadlines  map[string][]base.Z            // final state of the deadline set
	}{
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
			target: t1,
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {t2},
				"custom":  {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
		},
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
			},
			target: t1,
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {},
			},
		},
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1, t3},
				"custom":  {t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t3, Score: t3Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
			target: t3,
			wantInProgress: map[string][]*base.TaskMessage{
				"defualt": {t1},
				"custom":  {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {{Message: t2, Score: t2Deadline}},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllDeadlines(t, r.client, tc.deadlines)
		h.SeedAllInProgressQueues(t, r.client, tc.inProgress)
		for _, msgs := range tc.inProgress {
			for _, msg := range msgs {
				// Set uniqueness lock if unique key is present.
				if len(msg.UniqueKey) > 0 {
					err := r.client.SetNX(msg.UniqueKey, msg.ID.String(), time.Minute).Err()
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		err := r.Done(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Done(task) = %v, want nil", err)
			continue
		}

		for queue, want := range tc.wantInProgress {
			gotInProgress := h.GetInProgressMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotInProgress, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.InProgressKey(queue), diff)
				continue
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.DeadlinesKey(queue), diff)
				continue
			}
		}

		processedKey := base.ProcessedKey(tc.target.Queue, time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}

		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		if len(tc.target.UniqueKey) > 0 && r.client.Exists(tc.target.UniqueKey).Val() != 0 {
			t.Errorf("Uniqueness lock %q still exists", tc.target.UniqueKey)
		}
	}
}

func TestRequeue(t *testing.T) {
	r := setup(t)
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "default",
		Timeout: 1800,
	}
	t2 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "export_csv",
		Payload: nil,
		Queue:   "default",
		Timeout: 3000,
	}
	t3 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "critical",
		Timeout: 80,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := now.Unix() + t2.Timeout
	t3Deadline := now.Unix() + t3.Timeout

	tests := []struct {
		enqueued       map[string][]*base.TaskMessage // initial state of queues
		inProgress     []*base.TaskMessage            // initial state of the in-progress list
		deadlines      []base.Z                       // initial state of the deadlines set
		target         *base.TaskMessage              // task to requeue
		wantEnqueued   map[string][]*base.TaskMessage // final state of queues
		wantInProgress []*base.TaskMessage            // final state of the in-progress list
		wantDeadlines  []base.Z                       // final state of the deadlines set
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {},
			},
			inProgress: []*base.TaskMessage{t1, t2},
			deadlines: []base.Z{
				{Message: t1, Score: t1Deadline},
				{Message: t2, Score: t2Deadline},
			},
			target: t1,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
			},
			wantInProgress: []*base.TaskMessage{t2},
			wantDeadlines: []base.Z{
				{Message: t2, Score: t2Deadline},
			},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
			},
			inProgress: []*base.TaskMessage{t2},
			deadlines: []base.Z{
				{Message: t2, Score: t2Deadline},
			},
			target: t2,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1, t2},
			},
			wantInProgress: []*base.TaskMessage{},
			wantDeadlines:  []base.Z{},
		},
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
				"critical":            {},
			},
			inProgress: []*base.TaskMessage{t2, t3},
			deadlines: []base.Z{
				{Message: t2, Score: t2Deadline},
				{Message: t3, Score: t3Deadline},
			},
			target: t3,
			wantEnqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {t1},
				"critical":            {t3},
			},
			wantInProgress: []*base.TaskMessage{t2},
			wantDeadlines: []base.Z{
				{Message: t2, Score: t2Deadline},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r.client, msgs, qname)
		}
		h.SeedInProgressQueue(t, r.client, tc.inProgress)
		h.SeedDeadlines(t, r.client, tc.deadlines)

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
		gotDeadlines := h.GetDeadlinesEntries(t, r.client)
		if diff := cmp.Diff(tc.wantDeadlines, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", base.KeyDeadlines, diff)
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

		gotScheduled := h.GetScheduledEntries(t, r.client, tc.msg.Queue)
		if len(gotScheduled) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted",
				desc, len(gotScheduled), base.ScheduledKey(tc.msg.Queue))
			continue
		}
		if int64(gotScheduled[0].Score) != tc.processAt.Unix() {
			t.Errorf("%s inserted an item with score %d, want %d",
				desc, int64(gotScheduled[0].Score), tc.processAt.Unix())
			continue
		}
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
		}
	}
}

func TestScheduleUnique(t *testing.T) {
	r := setup(t)
	m1 := base.TaskMessage{
		ID:        uuid.New(),
		Type:      "email",
		Payload:   map[string]interface{}{"user_id": 123},
		Queue:     base.DefaultQueueName,
		UniqueKey: base.UniqueKey(base.DefaultQueueName, "email", map[string]interface{}{"user_id": 123}),
	}

	tests := []struct {
		msg       *base.TaskMessage
		processAt time.Time
		ttl       time.Duration // uniqueness lock ttl
	}{
		{&m1, time.Now().Add(15 * time.Minute), time.Minute},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case

		desc := fmt.Sprintf("(*RDB).ScheduleUnique(%v, %v, %v)", tc.msg, tc.processAt, tc.ttl)
		err := r.ScheduleUnique(tc.msg, tc.processAt, tc.ttl)
		if err != nil {
			t.Errorf("Frist task: %s = %v, want nil", desc, err)
			continue
		}

		gotScheduled := h.GetScheduledEntries(t, r.client, tc.msg.Queue)
		if len(gotScheduled) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted", desc, len(gotScheduled), base.ScheduledKey(tc.msg.Queue))
			continue
		}
		if int64(gotScheduled[0].Score) != tc.processAt.Unix() {
			t.Errorf("%s inserted an item with score %d, want %d", desc, int64(gotScheduled[0].Score), tc.processAt.Unix())
			continue
		}

		got := r.ScheduleUnique(tc.msg, tc.processAt, tc.ttl)
		if got != ErrDuplicateTask {
			t.Errorf("Second task: %s = %v, want %v",
				desc, got, ErrDuplicateTask)
		}

		gotTTL := r.client.TTL(tc.msg.UniqueKey).Val()
		if !cmp.Equal(tc.ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL %q = %v, want %v", tc.msg.UniqueKey, gotTTL, tc.ttl)
			continue
		}
		if !r.client.SIsMember(base.AllQueues, tc.msg.Queue).Val() {
			t.Errorf("%q is not a member of SET %q", tc.msg.Queue, base.AllQueues)
			continue
		}
	}
}

func TestRetry(t *testing.T) {
	r := setup(t)
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: map[string]interface{}{"subject": "Hola!"},
		Retried: 10,
		Timeout: 1800,
		Queue:   "default",
	}
	t2 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "gen_thumbnail",
		Payload: map[string]interface{}{"path": "some/path/to/image.jpg"},
		Timeout: 3000,
		Queue:   "default",
	}
	t3 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "reindex",
		Payload: nil,
		Timeout: 60,
		Queue:   "default",
	}
	t4 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_notification",
		Payload: nil,
		Timeout: 1800,
		Queue:   "custom",
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2Deadline := now.Unix() + t2.Timeout
	t4Deadline := now.Unix() + t4.Timeout
	errMsg := "SMTP server is not responding"

	tests := []struct {
		inProgress     map[string][]*base.TaskMessage
		deadlines      map[string][]base.Z
		retry          map[string][]base.Z
		msg            *base.TaskMessage
		processAt      time.Time
		errMsg         string
		wantInProgress map[string][]*base.TaskMessage
		wantDeadlines  map[string][]base.Z
		wantRetry      map[string][]base.Z
	}{
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {{Message: t3, Score: now.Add(time.Minute).Unix()}},
			},
			msg:       t1,
			processAt: now.Add(5 * time.Minute),
			errMsg:    errMsg,
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t2, Score: t2Deadline}},
			},
			wantRetry: map[string][]base.Z{
				"default": {
					{Message: h.TaskMessageAfterRetry(*t1, errMsg), Score: now.Add(5 * time.Minute).Unix()},
					{Message: t3, Score: now.Add(time.Minute).Unix()},
				},
			},
		},
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {t4},
			},
			deadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {{Message: t4, Score: t4Deadline}},
			},
			retry: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			msg:       t4,
			processAt: now.Add(5 * time.Minute),
			errMsg:    errMsg,
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {t1, t2},
				"custom":  {},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {{Message: t1, Score: t1Deadline}, {Message: t2, Score: t2Deadline}},
				"custom":  {},
			},
			wantRetry: map[string][]base.Z{
				"default": {},
				"custom": {
					{Message: h.TaskMessageAfterRetry(*t4, errMsg), Score: now.Add(5 * time.Minute).Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedAllInProgressQueues(t, r.client, tc.inProgress)
		h.SeedAllDeadlines(t, r.client, tc.deadlines)
		h.SeedAllRetryQueues(t, r.client, tc.retry)

		err := r.Retry(tc.msg, tc.processAt, tc.errMsg)
		if err != nil {
			t.Errorf("(*RDB).Retry = %v, want nil", err)
			continue
		}

		for queue, want := range tc.wantInProgress {
			gotInProgress := h.GetInProgressMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotInProgress, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.InProgressKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadlinesKey(queue), diff)
			}
		}
		for queue, want := range tc.wantRetry {
			gotRetry := h.GetRetryEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotRetry, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryKey(queue), diff)
			}
		}

		processedKey := base.ProcessedKey(tc.msg.Queue, time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}
		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		failedKey := base.FailedKey(tc.msg.Queue, time.Now())
		gotFailed := r.client.Get(failedKey).Val()
		if gotFailed != "1" {
			t.Errorf("GET %q = %q, want 1", failedKey, gotFailed)
		}
		gotTTL = r.client.TTL(failedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", failedKey, gotTTL, statsTTL)
		}
	}
}

func TestKill(t *testing.T) {
	r := setup(t)
	now := time.Now()
	t1 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 25,
		Timeout: 1800,
	}
	t1Deadline := now.Unix() + t1.Timeout
	t2 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "reindex",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 0,
		Timeout: 3000,
	}
	t2Deadline := now.Unix() + t2.Timeout
	t3 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "generate_csv",
		Payload: nil,
		Queue:   "default",
		Retry:   25,
		Retried: 0,
		Timeout: 60,
	}
	t3Deadline := now.Unix() + t3.Timeout
	t4 := &base.TaskMessage{
		ID:      uuid.New(),
		Type:    "send_email",
		Payload: nil,
		Queue:   "custom",
		Retry:   25,
		Retried: 25,
		Timeout: 1800,
	}
	t4Deadline := now.Unix() + t4.Timeout
	errMsg := "SMTP server not responding"

	// TODO(hibiken): add test cases for trimming
	tests := []struct {
		inProgress     map[string][]*base.TaskMessage
		deadlines      map[string][]base.Z
		dead           map[string][]base.Z
		target         *base.TaskMessage // task to kill
		wantInProgress map[string][]*base.TaskMessage
		wantDeadlines  map[string][]base.Z
		wantDead       map[string][]base.Z
	}{
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1, t2},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
					{Message: t2, Score: t2Deadline},
				},
			},
			dead: map[string][]base.Z{
				"default": {
					{Message: t3, Score: now.Add(-time.Hour).Unix()},
				},
			},
			target: t1,
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {t2},
			},
			wantDeadlines: map[string]base.Z{
				"default": {{Message: t2, Score: t2Deadline}},
			},
			wantDead: map[string][]base.Z{
				"default": {
					{Message: h.TaskMessageWithError(*t1, errMsg), Score: now.Unix()},
					{Message: t3, Score: now.Add(-time.Hour).Unix()},
				},
			},
		},
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
					{Message: t2, Score: t2Deadline},
					{Message: t3, Score: t3Deadline},
				},
			},
			dead: map[string][]base.Z{
				"default": {},
			},
			target: t1,
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {t2, t3},
			},
			wantDeadlines: map[string][]base.Z{
				"default": {
					{Message: t2, Score: t2Deadline},
					{Message: t3, Score: t3Deadline},
				},
			},
			wantDead: map[string][]base.Z{
				"default": {
					{Message: h.TaskMessageWithError(*t1, errMsg), Score: now.Unix()},
				},
			},
		},
		{
			inProgress: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {t4},
			},
			deadlines: map[string][]base.Z{
				"default": {
					{Message: t1, Score: t1Deadline},
				},
				"custom": {
					{Message: t4, Score: t4Deadline},
				},
			},
			dead: map[string][]base.Z{
				"default": {},
				"custom":  {},
			},
			target: t4,
			wantInProgress: map[string][]*base.TaskMessage{
				"default": {t1},
				"custom":  {},
			},
			wantDeadlines: map[string]base.Z{
				"default": {{Message: t1, Score: t1Deadline}},
				"custom":  {},
			},
			wantDead: map[string][]base.Z{
				"default": {},
				"custom": {
					{Message: h.TaskMessageWithError(*t4, errMsg), Score: now.Unix()},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r.client) // clean up db before each test case
		h.SeedAllInProgressQueues(t, r.client, tc.inProgress)
		h.SeedAllDeadlines(t, r.client, tc.deadlines)
		h.SeedAllDeadQueues(t, r.client, tc.dead)

		err := r.Kill(tc.target, errMsg)
		if err != nil {
			t.Errorf("(*RDB).Kill(%v, %v) = %v, want nil", tc.target, errMsg, err)
			continue
		}

		for queue, want := range tc.wantInProgress {
			gotInProgress := h.GetInProgressMessages(t, r.client, queue)
			if diff := cmp.Diff(want, gotInProgress, h.SortMsgOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want, +got)\n%s", base.InProgressKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDeadlines {
			gotDeadlines := h.GetDeadlinesEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDeadlines, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q after calling (*RDB).Kill: (-want, +got):\n%s", base.DeadlinesKey(queue), diff)
			}
		}
		for queue, want := range tc.wantDead {
			gotDead := h.GetDeadEntries(t, r.client, queue)
			if diff := cmp.Diff(want, gotDead, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q after calling (*RDB).Kill: (-want, +got):\n%s", base.DeadKey(queue), diff)
			}
		}

		processedKey := base.ProcessedKey(tc.target.Queue, time.Now())
		gotProcessed := r.client.Get(processedKey).Val()
		if gotProcessed != "1" {
			t.Errorf("GET %q = %q, want 1", processedKey, gotProcessed)
		}
		gotTTL := r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", processedKey, gotTTL, statsTTL)
		}

		failedKey := base.FailedKey(tc.target.Queue, time.Now())
		gotFailed := r.client.Get(failedKey).Val()
		if gotFailed != "1" {
			t.Errorf("GET %q = %q, want 1", failedKey, gotFailed)
		}
		gotTTL = r.client.TTL(processedKey).Val()
		if gotTTL > statsTTL {
			t.Errorf("TTL %q = %v, want less than or equal to %v", failedKey, gotTTL, statsTTL)
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
		scheduled     []base.Z
		retry         []base.Z
		wantEnqueued  map[string][]*base.TaskMessage
		wantScheduled []*base.TaskMessage
		wantRetry     []*base.TaskMessage
	}{
		{
			scheduled: []base.Z{
				{Message: t1, Score: secondAgo.Unix()},
				{Message: t2, Score: secondAgo.Unix()},
			},
			retry: []base.Z{
				{Message: t3, Score: secondAgo.Unix()}},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			wantScheduled: []*base.TaskMessage{},
			wantRetry:     []*base.TaskMessage{},
		},
		{
			scheduled: []base.Z{
				{Message: t1, Score: hourFromNow.Unix()},
				{Message: t2, Score: secondAgo.Unix()}},
			retry: []base.Z{
				{Message: t3, Score: secondAgo.Unix()}},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {t2, t3},
			},
			wantScheduled: []*base.TaskMessage{t1},
			wantRetry:     []*base.TaskMessage{},
		},
		{
			scheduled: []base.Z{
				{Message: t1, Score: hourFromNow.Unix()},
				{Message: t2, Score: hourFromNow.Unix()}},
			retry: []base.Z{
				{Message: t3, Score: hourFromNow.Unix()}},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: []*base.TaskMessage{t1, t2},
			wantRetry:     []*base.TaskMessage{t3},
		},
		{
			scheduled: []base.Z{
				{Message: t1, Score: secondAgo.Unix()},
				{Message: t4, Score: secondAgo.Unix()},
			},
			retry: []base.Z{
				{Message: t5, Score: secondAgo.Unix()}},
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

		err := r.CheckAndEnqueue()
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

func TestListDeadlineExceeded(t *testing.T) {
	t1 := h.NewTaskMessage("task1", nil)
	t2 := h.NewTaskMessage("task2", nil)
	t3 := h.NewTaskMessageWithQueue("task3", nil, "critical")

	now := time.Now()
	oneHourFromNow := now.Add(1 * time.Hour)
	fiveMinutesFromNow := now.Add(5 * time.Minute)
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	oneHourAgo := now.Add(-1 * time.Hour)

	tests := []struct {
		desc      string
		deadlines []base.Z
		t         time.Time
		want      []*base.TaskMessage
	}{
		{
			desc: "with one task in-progress",
			deadlines: []base.Z{
				{Message: t1, Score: fiveMinutesAgo.Unix()},
			},
			t:    time.Now(),
			want: []*base.TaskMessage{t1},
		},
		{
			desc: "with multiple tasks in-progress, and one expired",
			deadlines: []base.Z{
				{Message: t1, Score: oneHourAgo.Unix()},
				{Message: t2, Score: fiveMinutesFromNow.Unix()},
				{Message: t3, Score: oneHourFromNow.Unix()},
			},
			t:    time.Now(),
			want: []*base.TaskMessage{t1},
		},
		{
			desc: "with multiple expired tasks in-progress",
			deadlines: []base.Z{
				{Message: t1, Score: oneHourAgo.Unix()},
				{Message: t2, Score: fiveMinutesAgo.Unix()},
				{Message: t3, Score: oneHourFromNow.Unix()},
			},
			t:    time.Now(),
			want: []*base.TaskMessage{t1, t2},
		},
		{
			desc:      "with empty in-progress queue",
			deadlines: []base.Z{},
			t:         time.Now(),
			want:      []*base.TaskMessage{},
		},
	}

	r := setup(t)
	for _, tc := range tests {
		h.FlushDB(t, r.client)
		h.SeedDeadlines(t, r.client, tc.deadlines)

		got, err := r.ListDeadlineExceeded(tc.t)
		if err != nil {
			t.Errorf("%s; ListDeadlineExceeded(%v) returned error: %v", tc.desc, tc.t, err)
			continue
		}

		if diff := cmp.Diff(tc.want, got, h.SortMsgOpt); diff != "" {
			t.Errorf("%s; ListDeadlineExceeded(%v) returned %v, want %v;(-want,+got)\n%s",
				tc.desc, tc.t, got, tc.want, diff)
		}
	}
}

func TestWriteServerState(t *testing.T) {
	r := setup(t)

	var (
		host     = "localhost"
		pid      = 4242
		serverID = "server123"

		ttl = 5 * time.Second
	)

	info := base.ServerInfo{
		Host:              host,
		PID:               pid,
		ServerID:          serverID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           time.Now(),
		Status:            "running",
		ActiveWorkerCount: 0,
	}

	err := r.WriteServerState(&info, nil /* workers */, ttl)
	if err != nil {
		t.Errorf("r.WriteServerState returned an error: %v", err)
	}

	// Check ServerInfo was written correctly.
	skey := base.ServerInfoKey(host, pid, serverID)
	data := r.client.Get(skey).Val()
	var got base.ServerInfo
	err = json.Unmarshal([]byte(data), &got)
	if err != nil {
		t.Fatalf("could not decode json: %v", err)
	}
	if diff := cmp.Diff(info, got); diff != "" {
		t.Errorf("persisted ServerInfo was %v, want %v; (-want,+got)\n%s",
			got, info, diff)
	}
	// Check ServerInfo TTL was set correctly.
	gotTTL := r.client.TTL(skey).Val()
	if !cmp.Equal(ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
		t.Errorf("TTL of %q was %v, want %v", skey, gotTTL, ttl)
	}
	// Check ServerInfo key was added to the set all server keys correctly.
	gotServerKeys := r.client.ZRange(base.AllServers, 0, -1).Val()
	wantServerKeys := []string{skey}
	if diff := cmp.Diff(wantServerKeys, gotServerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllServers, gotServerKeys, wantServerKeys)
	}

	// Check WorkersInfo was written correctly.
	wkey := base.WorkersKey(host, pid, serverID)
	workerExist := r.client.Exists(wkey).Val()
	if workerExist != 0 {
		t.Errorf("%q key exists", wkey)
	}
	// Check WorkersInfo key was added to the set correctly.
	gotWorkerKeys := r.client.ZRange(base.AllWorkers, 0, -1).Val()
	wantWorkerKeys := []string{wkey}
	if diff := cmp.Diff(wantWorkerKeys, gotWorkerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllWorkers, gotWorkerKeys, wantWorkerKeys)
	}
}

func TestWriteServerStateWithWorkers(t *testing.T) {
	r := setup(t)

	var (
		host     = "127.0.0.1"
		pid      = 4242
		serverID = "server123"

		msg1 = h.NewTaskMessage("send_email", map[string]interface{}{"user_id": "123"})
		msg2 = h.NewTaskMessage("gen_thumbnail", map[string]interface{}{"path": "some/path/to/imgfile"})

		ttl = 5 * time.Second
	)

	workers := []*base.WorkerInfo{
		{
			Host:    host,
			PID:     pid,
			ID:      msg1.ID.String(),
			Type:    msg1.Type,
			Queue:   msg1.Queue,
			Payload: msg1.Payload,
			Started: time.Now().Add(-10 * time.Second),
		},
		{
			Host:    host,
			PID:     pid,
			ID:      msg2.ID.String(),
			Type:    msg2.Type,
			Queue:   msg2.Queue,
			Payload: msg2.Payload,
			Started: time.Now().Add(-2 * time.Minute),
		},
	}

	serverInfo := base.ServerInfo{
		Host:              host,
		PID:               pid,
		ServerID:          serverID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           time.Now().Add(-10 * time.Minute),
		Status:            "running",
		ActiveWorkerCount: len(workers),
	}

	err := r.WriteServerState(&serverInfo, workers, ttl)
	if err != nil {
		t.Fatalf("r.WriteServerState returned an error: %v", err)
	}

	// Check ServerInfo was written correctly.
	skey := base.ServerInfoKey(host, pid, serverID)
	data := r.client.Get(skey).Val()
	var got base.ServerInfo
	err = json.Unmarshal([]byte(data), &got)
	if err != nil {
		t.Fatalf("could not decode json: %v", err)
	}
	if diff := cmp.Diff(serverInfo, got); diff != "" {
		t.Errorf("persisted ServerInfo was %v, want %v; (-want,+got)\n%s",
			got, serverInfo, diff)
	}
	// Check ServerInfo TTL was set correctly.
	gotTTL := r.client.TTL(skey).Val()
	if !cmp.Equal(ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
		t.Errorf("TTL of %q was %v, want %v", skey, gotTTL, ttl)
	}
	// Check ServerInfo key was added to the set correctly.
	gotServerKeys := r.client.ZRange(base.AllServers, 0, -1).Val()
	wantServerKeys := []string{skey}
	if diff := cmp.Diff(wantServerKeys, gotServerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllServers, gotServerKeys, wantServerKeys)
	}

	// Check WorkersInfo was written correctly.
	wkey := base.WorkersKey(host, pid, serverID)
	wdata := r.client.HGetAll(wkey).Val()
	if len(wdata) != 2 {
		t.Fatalf("HGETALL %q returned a hash of size %d, want 2", wkey, len(wdata))
	}
	var gotWorkers []*base.WorkerInfo
	for _, val := range wdata {
		var w base.WorkerInfo
		if err := json.Unmarshal([]byte(val), &w); err != nil {
			t.Fatalf("could not unmarshal worker's data: %v", err)
		}
		gotWorkers = append(gotWorkers, &w)
	}
	if diff := cmp.Diff(workers, gotWorkers, h.SortWorkerInfoOpt); diff != "" {
		t.Errorf("persisted workers info was %v, want %v; (-want,+got)\n%s",
			gotWorkers, workers, diff)
	}

	// Check WorkersInfo TTL was set correctly.
	gotTTL = r.client.TTL(wkey).Val()
	if !cmp.Equal(ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
		t.Errorf("TTL of %q was %v, want %v", wkey, gotTTL, ttl)
	}
	// Check WorkersInfo key was added to the set correctly.
	gotWorkerKeys := r.client.ZRange(base.AllWorkers, 0, -1).Val()
	wantWorkerKeys := []string{wkey}
	if diff := cmp.Diff(wantWorkerKeys, gotWorkerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllWorkers, gotWorkerKeys, wantWorkerKeys)
	}
}

func TestClearServerState(t *testing.T) {
	r := setup(t)

	var (
		host     = "127.0.0.1"
		pid      = 1234
		serverID = "server123"

		otherHost     = "127.0.0.2"
		otherPID      = 9876
		otherServerID = "server987"

		msg1 = h.NewTaskMessage("send_email", map[string]interface{}{"user_id": "123"})
		msg2 = h.NewTaskMessage("gen_thumbnail", map[string]interface{}{"path": "some/path/to/imgfile"})

		ttl = 5 * time.Second
	)

	workers1 := []*base.WorkerInfo{
		{
			Host:    host,
			PID:     pid,
			ID:      msg1.ID.String(),
			Type:    msg1.Type,
			Queue:   msg1.Queue,
			Payload: msg1.Payload,
			Started: time.Now().Add(-10 * time.Second),
		},
	}
	serverInfo1 := base.ServerInfo{
		Host:              host,
		PID:               pid,
		ServerID:          serverID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           time.Now().Add(-10 * time.Minute),
		Status:            "running",
		ActiveWorkerCount: len(workers1),
	}

	workers2 := []*base.WorkerInfo{
		{
			Host:    otherHost,
			PID:     otherPID,
			ID:      msg2.ID.String(),
			Type:    msg2.Type,
			Queue:   msg2.Queue,
			Payload: msg2.Payload,
			Started: time.Now().Add(-30 * time.Second),
		},
	}
	serverInfo2 := base.ServerInfo{
		Host:              otherHost,
		PID:               otherPID,
		ServerID:          otherServerID,
		Concurrency:       10,
		Queues:            map[string]int{"default": 2, "email": 5, "low": 1},
		StrictPriority:    false,
		Started:           time.Now().Add(-15 * time.Minute),
		Status:            "running",
		ActiveWorkerCount: len(workers2),
	}

	// Write server and workers data.
	if err := r.WriteServerState(&serverInfo1, workers1, ttl); err != nil {
		t.Fatalf("could not write server state: %v", err)
	}
	if err := r.WriteServerState(&serverInfo2, workers2, ttl); err != nil {
		t.Fatalf("could not write server state: %v", err)
	}

	err := r.ClearServerState(host, pid, serverID)
	if err != nil {
		t.Fatalf("(*RDB).ClearServerState failed: %v", err)
	}

	skey := base.ServerInfoKey(host, pid, serverID)
	wkey := base.WorkersKey(host, pid, serverID)
	otherSKey := base.ServerInfoKey(otherHost, otherPID, otherServerID)
	otherWKey := base.WorkersKey(otherHost, otherPID, otherServerID)
	// Check all keys are cleared.
	if r.client.Exists(skey).Val() != 0 {
		t.Errorf("Redis key %q exists", skey)
	}
	if r.client.Exists(wkey).Val() != 0 {
		t.Errorf("Redis key %q exists", wkey)
	}
	gotServerKeys := r.client.ZRange(base.AllServers, 0, -1).Val()
	wantServerKeys := []string{otherSKey}
	if diff := cmp.Diff(wantServerKeys, gotServerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllServers, gotServerKeys, wantServerKeys)
	}
	gotWorkerKeys := r.client.ZRange(base.AllWorkers, 0, -1).Val()
	wantWorkerKeys := []string{otherWKey}
	if diff := cmp.Diff(wantWorkerKeys, gotWorkerKeys); diff != "" {
		t.Errorf("%q contained %v, want %v", base.AllWorkers, gotWorkerKeys, wantWorkerKeys)
	}
}

func TestCancelationPubSub(t *testing.T) {
	r := setup(t)

	pubsub, err := r.CancelationPubSub()
	if err != nil {
		t.Fatalf("(*RDB).CancelationPubSub() returned an error: %v", err)
	}

	cancelCh := pubsub.Channel()

	var (
		mu       sync.Mutex
		received []string
	)

	go func() {
		for msg := range cancelCh {
			mu.Lock()
			received = append(received, msg.Payload)
			mu.Unlock()
		}
	}()

	publish := []string{"one", "two", "three"}

	for _, msg := range publish {
		r.PublishCancelation(msg)
	}

	// allow for message to reach subscribers.
	time.Sleep(time.Second)

	pubsub.Close()

	mu.Lock()
	if diff := cmp.Diff(publish, received, h.SortStringSliceOpt); diff != "" {
		t.Errorf("subscriber received %v, want %v; (-want,+got)\n%s", received, publish, diff)
	}
	mu.Unlock()
}
