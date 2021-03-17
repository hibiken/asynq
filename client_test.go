// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

func TestClientEnqueueWithProcessAtOption(t *testing.T) {
	r := setup(t)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()

	task := NewTask("send_email", h.KV(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))

	var (
		now          = time.Now()
		oneHourLater = now.Add(time.Hour)
	)

	tests := []struct {
		desc          string
		task          *Task
		processAt     time.Time // value for ProcessAt option
		opts          []Option  // other options
		wantRes       *Result
		wantPending   map[string][]*base.TaskMessage
		wantScheduled map[string][]base.Z
	}{
		{
			desc:      "Process task immediately",
			task:      task,
			processAt: now,
			opts:      []Option{},
			wantRes: &Result{
				EnqueuedAt: now.UTC(),
				ProcessAt:  now,
				Queue:      "default",
				Retry:      defaultMaxRetry,
				Timeout:    defaultTimeout,
				Deadline:   noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
		},
		{
			desc:      "Schedule task to be processed in the future",
			task:      task,
			processAt: oneHourLater,
			opts:      []Option{},
			wantRes: &Result{
				EnqueuedAt: now.UTC(),
				ProcessAt:  oneHourLater,
				Queue:      "default",
				Retry:      defaultMaxRetry,
				Timeout:    defaultTimeout,
				Deadline:   noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: map[string][]base.Z{
				"default": {
					{
						Message: &base.TaskMessage{
							Type:     task.Type(),
							Payload:  task.Payload(),
							Retry:    defaultMaxRetry,
							Queue:    "default",
							Timeout:  int64(defaultTimeout.Seconds()),
							Deadline: noDeadline.Unix(),
						},
						Score: oneHourLater.Unix(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		opts := append(tc.opts, ProcessAt(tc.processAt))
		gotRes, err := client.Enqueue(tc.task, opts...)
		if err != nil {
			t.Error(err)
			continue
		}
		cmpOptions := []cmp.Option{
			cmpopts.IgnoreFields(Result{}, "ID"),
			cmpopts.EquateApproxTime(500 * time.Millisecond),
		}
		if diff := cmp.Diff(tc.wantRes, gotRes, cmpOptions...); diff != "" {
			t.Errorf("%s;\nEnqueue(task, ProcessAt(%v)) returned %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.processAt, gotRes, tc.wantRes, diff)
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.IgnoreIDOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.IgnoreIDOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.ScheduledKey(qname), diff)
			}
		}
	}
}

func TestClientEnqueue(t *testing.T) {
	r := setup(t)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()

	task := NewTask("send_email", h.KV(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))
	now := time.Now()

	tests := []struct {
		desc        string
		task        *Task
		opts        []Option
		wantRes     *Result
		wantPending map[string][]*base.TaskMessage
	}{
		{
			desc: "Process task immediately with a custom retry count",
			task: task,
			opts: []Option{
				MaxRetry(3),
			},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "default",
				Retry:     3,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    3,
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "Negative retry count",
			task: task,
			opts: []Option{
				MaxRetry(-2),
			},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "default",
				Retry:     0,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    0, // Retry count should be set to zero
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "Conflicting options",
			task: task,
			opts: []Option{
				MaxRetry(2),
				MaxRetry(10),
			},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "default",
				Retry:     10,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    10, // Last option takes precedence
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "With queue option",
			task: task,
			opts: []Option{
				Queue("custom"),
			},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "custom",
				Retry:     defaultMaxRetry,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"custom": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "custom",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "Queue option should be case-insensitive",
			task: task,
			opts: []Option{
				Queue("HIGH"),
			},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "high",
				Retry:     defaultMaxRetry,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"high": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "high",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "With timeout option",
			task: task,
			opts: []Option{
				Timeout(20 * time.Second),
			},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "default",
				Retry:     defaultMaxRetry,
				Timeout:   20 * time.Second,
				Deadline:  noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  20,
						Deadline: noDeadline.Unix(),
					},
				},
			},
		},
		{
			desc: "With deadline option",
			task: task,
			opts: []Option{
				Deadline(time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC)),
			},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "default",
				Retry:     defaultMaxRetry,
				Timeout:   noTimeout,
				Deadline:  time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC),
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  int64(noTimeout.Seconds()),
						Deadline: time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC).Unix(),
					},
				},
			},
		},
		{
			desc: "With both deadline and timeout options",
			task: task,
			opts: []Option{
				Timeout(20 * time.Second),
				Deadline(time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC)),
			},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "default",
				Retry:     defaultMaxRetry,
				Timeout:   20 * time.Second,
				Deadline:  time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC),
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  20,
						Deadline: time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC).Unix(),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		gotRes, err := client.Enqueue(tc.task, tc.opts...)
		if err != nil {
			t.Error(err)
			continue
		}
		cmpOptions := []cmp.Option{
			cmpopts.IgnoreFields(Result{}, "ID", "EnqueuedAt"),
			cmpopts.EquateApproxTime(500 * time.Millisecond),
		}
		if diff := cmp.Diff(tc.wantRes, gotRes, cmpOptions...); diff != "" {
			t.Errorf("%s;\nEnqueue(task) returned %v, want %v; (-want,+got)\n%s",
				tc.desc, gotRes, tc.wantRes, diff)
		}

		for qname, want := range tc.wantPending {
			got := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, got, h.IgnoreIDOpt); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
	}
}

func TestClientEnqueueWithProcessInOption(t *testing.T) {
	r := setup(t)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()

	task := NewTask("send_email", h.KV(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))
	now := time.Now()

	tests := []struct {
		desc          string
		task          *Task
		delay         time.Duration // value for ProcessIn option
		opts          []Option      // other options
		wantRes       *Result
		wantPending   map[string][]*base.TaskMessage
		wantScheduled map[string][]base.Z
	}{
		{
			desc:  "schedule a task to be processed in one hour",
			task:  task,
			delay: 1 * time.Hour,
			opts:  []Option{},
			wantRes: &Result{
				ProcessAt: now.Add(1 * time.Hour),
				Queue:     "default",
				Retry:     defaultMaxRetry,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {},
			},
			wantScheduled: map[string][]base.Z{
				"default": {
					{
						Message: &base.TaskMessage{
							Type:     task.Type(),
							Payload:  task.Payload(),
							Retry:    defaultMaxRetry,
							Queue:    "default",
							Timeout:  int64(defaultTimeout.Seconds()),
							Deadline: noDeadline.Unix(),
						},
						Score: time.Now().Add(time.Hour).Unix(),
					},
				},
			},
		},
		{
			desc:  "Zero delay",
			task:  task,
			delay: 0,
			opts:  []Option{},
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "default",
				Retry:     defaultMaxRetry,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type(),
						Payload:  task.Payload(),
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  int64(defaultTimeout.Seconds()),
						Deadline: noDeadline.Unix(),
					},
				},
			},
			wantScheduled: map[string][]base.Z{
				"default": {},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		opts := append(tc.opts, ProcessIn(tc.delay))
		gotRes, err := client.Enqueue(tc.task, opts...)
		if err != nil {
			t.Error(err)
			continue
		}
		cmpOptions := []cmp.Option{
			cmpopts.IgnoreFields(Result{}, "ID", "EnqueuedAt"),
			cmpopts.EquateApproxTime(500 * time.Millisecond),
		}
		if diff := cmp.Diff(tc.wantRes, gotRes, cmpOptions...); diff != "" {
			t.Errorf("%s;\nEnqueue(task, ProcessIn(%v)) returned %v, want %v; (-want,+got)\n%s",
				tc.desc, tc.delay, gotRes, tc.wantRes, diff)
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.IgnoreIDOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		for qname, want := range tc.wantScheduled {
			gotScheduled := h.GetScheduledEntries(t, r, qname)
			if diff := cmp.Diff(want, gotScheduled, h.IgnoreIDOpt, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.ScheduledKey(qname), diff)
			}
		}
	}
}

func TestClientEnqueueError(t *testing.T) {
	r := setup(t)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()

	task := NewTask("send_email", h.KV(map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}))

	tests := []struct {
		desc string
		task *Task
		opts []Option
	}{
		{
			desc: "With empty queue name",
			task: task,
			opts: []Option{
				Queue(""),
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)

		_, err := client.Enqueue(tc.task, tc.opts...)
		if err == nil {
			t.Errorf("%s; client.Enqueue(task, opts...) did not return non-nil error", tc.desc)
		}
	}
}

func TestClientDefaultOptions(t *testing.T) {
	r := setup(t)

	now := time.Now()

	tests := []struct {
		desc        string
		defaultOpts []Option // options set at the client level.
		opts        []Option // options used at enqueue time.
		task        *Task
		wantRes     *Result
		queue       string // queue that the message should go into.
		want        *base.TaskMessage
	}{
		{
			desc:        "With queue routing option",
			defaultOpts: []Option{Queue("feed")},
			opts:        []Option{},
			task:        NewTask("feed:import", nil),
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "feed",
				Retry:     defaultMaxRetry,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			queue: "feed",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    defaultMaxRetry,
				Queue:    "feed",
				Timeout:  int64(defaultTimeout.Seconds()),
				Deadline: noDeadline.Unix(),
			},
		},
		{
			desc:        "With multiple options",
			defaultOpts: []Option{Queue("feed"), MaxRetry(5)},
			opts:        []Option{},
			task:        NewTask("feed:import", nil),
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "feed",
				Retry:     5,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			queue: "feed",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    5,
				Queue:    "feed",
				Timeout:  int64(defaultTimeout.Seconds()),
				Deadline: noDeadline.Unix(),
			},
		},
		{
			desc:        "With overriding options at enqueue time",
			defaultOpts: []Option{Queue("feed"), MaxRetry(5)},
			opts:        []Option{Queue("critical")},
			task:        NewTask("feed:import", nil),
			wantRes: &Result{
				ProcessAt: now,
				Queue:     "critical",
				Retry:     5,
				Timeout:   defaultTimeout,
				Deadline:  noDeadline,
			},
			queue: "critical",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    5,
				Queue:    "critical",
				Timeout:  int64(defaultTimeout.Seconds()),
				Deadline: noDeadline.Unix(),
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		c := NewClient(getRedisConnOpt(t))
		defer c.Close()
		c.SetDefaultOptions(tc.task.Type(), tc.defaultOpts...)
		gotRes, err := c.Enqueue(tc.task, tc.opts...)
		if err != nil {
			t.Fatal(err)
		}
		cmpOptions := []cmp.Option{
			cmpopts.IgnoreFields(Result{}, "ID", "EnqueuedAt"),
			cmpopts.EquateApproxTime(500 * time.Millisecond),
		}
		if diff := cmp.Diff(tc.wantRes, gotRes, cmpOptions...); diff != "" {
			t.Errorf("%s;\nEnqueue(task, opts...) returned %v, want %v; (-want,+got)\n%s",
				tc.desc, gotRes, tc.wantRes, diff)
		}
		pending := h.GetPendingMessages(t, r, tc.queue)
		if len(pending) != 1 {
			t.Errorf("%s;\nexpected queue %q to have one message; got %d messages in the queue.",
				tc.desc, tc.queue, len(pending))
			continue
		}
		got := pending[0]
		if diff := cmp.Diff(tc.want, got, h.IgnoreIDOpt); diff != "" {
			t.Errorf("%s;\nmismatch found in pending task message; (-want,+got)\n%s",
				tc.desc, diff)
		}
	}
}

func TestClientEnqueueUnique(t *testing.T) {
	r := setup(t)
	c := NewClient(getRedisConnOpt(t))
	defer c.Close()

	tests := []struct {
		task *Task
		ttl  time.Duration
	}{
		{
			NewTask("email", h.KV(map[string]interface{}{"user_id": 123})),
			time.Hour,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		// Enqueue the task first. It should succeed.
		_, err := c.Enqueue(tc.task, Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := r.TTL(base.UniqueKey(base.DefaultQueueName, tc.task.Type(), tc.task.Payload())).Val()
		if !cmp.Equal(tc.ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, tc.ttl)
			continue
		}

		// Enqueue the task again. It should fail.
		_, err = c.Enqueue(tc.task, Unique(tc.ttl))
		if err == nil {
			t.Errorf("Enqueueing %+v did not return an error", tc.task)
			continue
		}
		if !errors.Is(err, ErrDuplicateTask) {
			t.Errorf("Enqueueing %+v returned an error that is not ErrDuplicateTask", tc.task)
			continue
		}
	}
}

func TestClientEnqueueUniqueWithProcessInOption(t *testing.T) {
	r := setup(t)
	c := NewClient(getRedisConnOpt(t))
	defer c.Close()

	tests := []struct {
		task *Task
		d    time.Duration
		ttl  time.Duration
	}{
		{
			NewTask("reindex", nil),
			time.Hour,
			10 * time.Minute,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		// Enqueue the task first. It should succeed.
		_, err := c.Enqueue(tc.task, ProcessIn(tc.d), Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := r.TTL(base.UniqueKey(base.DefaultQueueName, tc.task.Type(), tc.task.Payload())).Val()
		wantTTL := time.Duration(tc.ttl.Seconds()+tc.d.Seconds()) * time.Second
		if !cmp.Equal(wantTTL.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, wantTTL)
			continue
		}

		// Enqueue the task again. It should fail.
		_, err = c.Enqueue(tc.task, ProcessIn(tc.d), Unique(tc.ttl))
		if err == nil {
			t.Errorf("Enqueueing %+v did not return an error", tc.task)
			continue
		}
		if !errors.Is(err, ErrDuplicateTask) {
			t.Errorf("Enqueueing %+v returned an error that is not ErrDuplicateTask", tc.task)
			continue
		}
	}
}

func TestClientEnqueueUniqueWithProcessAtOption(t *testing.T) {
	r := setup(t)
	c := NewClient(getRedisConnOpt(t))
	defer c.Close()

	tests := []struct {
		task *Task
		at   time.Time
		ttl  time.Duration
	}{
		{
			NewTask("reindex", nil),
			time.Now().Add(time.Hour),
			10 * time.Minute,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		// Enqueue the task first. It should succeed.
		_, err := c.Enqueue(tc.task, ProcessAt(tc.at), Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := r.TTL(base.UniqueKey(base.DefaultQueueName, tc.task.Type(), tc.task.Payload())).Val()
		wantTTL := tc.at.Add(tc.ttl).Sub(time.Now())
		if !cmp.Equal(wantTTL.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, wantTTL)
			continue
		}

		// Enqueue the task again. It should fail.
		_, err = c.Enqueue(tc.task, ProcessAt(tc.at), Unique(tc.ttl))
		if err == nil {
			t.Errorf("Enqueueing %+v did not return an error", tc.task)
			continue
		}
		if !errors.Is(err, ErrDuplicateTask) {
			t.Errorf("Enqueueing %+v returned an error that is not ErrDuplicateTask", tc.task)
			continue
		}
	}
}
