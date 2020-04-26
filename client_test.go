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

var (
	noTimeout  = time.Duration(0).String()
	noDeadline = time.Time{}.Format(time.RFC3339)
)

func TestClientEnqueueAt(t *testing.T) {
	r := setup(t)
	client := NewClient(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	task := NewTask("send_email", map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"})

	var (
		now          = time.Now()
		oneHourLater = now.Add(time.Hour)
	)

	tests := []struct {
		desc          string
		task          *Task
		processAt     time.Time
		opts          []Option
		wantEnqueued  map[string][]*base.TaskMessage
		wantScheduled []h.ZSetEntry
	}{
		{
			desc:      "Process task immediately",
			task:      task,
			processAt: now,
			opts:      []Option{},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  noTimeout,
						Deadline: noDeadline,
					},
				},
			},
			wantScheduled: nil, // db is flushed in setup so zset does not exist hence nil
		},
		{
			desc:         "Schedule task to be processed in the future",
			task:         task,
			processAt:    oneHourLater,
			opts:         []Option{},
			wantEnqueued: nil, // db is flushed in setup so list does not exist hence nil
			wantScheduled: []h.ZSetEntry{
				{
					Msg: &base.TaskMessage{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  noTimeout,
						Deadline: noDeadline,
					},
					Score: float64(oneHourLater.Unix()),
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		err := client.EnqueueAt(tc.processAt, tc.task, tc.opts...)
		if err != nil {
			t.Error(err)
			continue
		}

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.IgnoreIDOpt); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.QueueKey(qname), diff)
			}
		}

		gotScheduled := h.GetScheduledEntries(t, r)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.IgnoreIDOpt); diff != "" {
			t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.ScheduledQueue, diff)
		}
	}
}

func TestClientEnqueue(t *testing.T) {
	r := setup(t)
	client := NewClient(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	task := NewTask("send_email", map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"})

	tests := []struct {
		desc         string
		task         *Task
		opts         []Option
		wantEnqueued map[string][]*base.TaskMessage
	}{
		{
			desc: "Process task immediately with a custom retry count",
			task: task,
			opts: []Option{
				MaxRetry(3),
			},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    3,
						Queue:    "default",
						Timeout:  noTimeout,
						Deadline: noDeadline,
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    0, // Retry count should be set to zero
						Queue:    "default",
						Timeout:  noTimeout,
						Deadline: noDeadline,
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    10, // Last option takes precedence
						Queue:    "default",
						Timeout:  noTimeout,
						Deadline: noDeadline,
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"custom": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    defaultMaxRetry,
						Queue:    "custom",
						Timeout:  noTimeout,
						Deadline: noDeadline,
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"high": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    defaultMaxRetry,
						Queue:    "high",
						Timeout:  noTimeout,
						Deadline: noDeadline,
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  (20 * time.Second).String(),
						Deadline: noDeadline,
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
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  noTimeout,
						Deadline: time.Date(2020, time.June, 24, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		err := client.Enqueue(tc.task, tc.opts...)
		if err != nil {
			t.Error(err)
			continue
		}

		for qname, want := range tc.wantEnqueued {
			got := h.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, got, h.IgnoreIDOpt); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.QueueKey(qname), diff)
			}
		}
	}
}

func TestClientEnqueueIn(t *testing.T) {
	r := setup(t)
	client := NewClient(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	task := NewTask("send_email", map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"})

	tests := []struct {
		desc          string
		task          *Task
		delay         time.Duration
		opts          []Option
		wantEnqueued  map[string][]*base.TaskMessage
		wantScheduled []h.ZSetEntry
	}{
		{
			desc:         "schedule a task to be enqueued in one hour",
			task:         task,
			delay:        time.Hour,
			opts:         []Option{},
			wantEnqueued: nil, // db is flushed in setup so list does not exist hence nil
			wantScheduled: []h.ZSetEntry{
				{
					Msg: &base.TaskMessage{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  noTimeout,
						Deadline: noDeadline,
					},
					Score: float64(time.Now().Add(time.Hour).Unix()),
				},
			},
		},
		{
			desc:  "Zero delay",
			task:  task,
			delay: 0,
			opts:  []Option{},
			wantEnqueued: map[string][]*base.TaskMessage{
				"default": {
					{
						Type:     task.Type,
						Payload:  task.Payload.data,
						Retry:    defaultMaxRetry,
						Queue:    "default",
						Timeout:  noTimeout,
						Deadline: noDeadline,
					},
				},
			},
			wantScheduled: nil, // db is flushed in setup so zset does not exist hence nil
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		err := client.EnqueueIn(tc.delay, tc.task, tc.opts...)
		if err != nil {
			t.Error(err)
			continue
		}

		for qname, want := range tc.wantEnqueued {
			gotEnqueued := h.GetEnqueuedMessages(t, r, qname)
			if diff := cmp.Diff(want, gotEnqueued, h.IgnoreIDOpt); diff != "" {
				t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.QueueKey(qname), diff)
			}
		}

		gotScheduled := h.GetScheduledEntries(t, r)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, h.IgnoreIDOpt); diff != "" {
			t.Errorf("%s;\nmismatch found in %q; (-want,+got)\n%s", tc.desc, base.ScheduledQueue, diff)
		}
	}
}

func TestClientDefaultOptions(t *testing.T) {
	r := setup(t)

	tests := []struct {
		desc        string
		defaultOpts []Option // options set at the client level.
		opts        []Option // options used at enqueue time.
		task        *Task
		queue       string // queue that the message should go into.
		want        *base.TaskMessage
	}{
		{
			desc:        "With queue routing option",
			defaultOpts: []Option{Queue("feed")},
			opts:        []Option{},
			task:        NewTask("feed:import", nil),
			queue:       "feed",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    defaultMaxRetry,
				Queue:    "feed",
				Timeout:  noTimeout,
				Deadline: noDeadline,
			},
		},
		{
			desc:        "With multiple options",
			defaultOpts: []Option{Queue("feed"), MaxRetry(5)},
			opts:        []Option{},
			task:        NewTask("feed:import", nil),
			queue:       "feed",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    5,
				Queue:    "feed",
				Timeout:  noTimeout,
				Deadline: noDeadline,
			},
		},
		{
			desc:        "With overriding options at enqueue time",
			defaultOpts: []Option{Queue("feed"), MaxRetry(5)},
			opts:        []Option{Queue("critical")},
			task:        NewTask("feed:import", nil),
			queue:       "critical",
			want: &base.TaskMessage{
				Type:     "feed:import",
				Payload:  nil,
				Retry:    5,
				Queue:    "critical",
				Timeout:  noTimeout,
				Deadline: noDeadline,
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)
		c := NewClient(RedisClientOpt{Addr: redisAddr, DB: redisDB})
		c.SetDefaultOptions(tc.task.Type, tc.defaultOpts...)
		err := c.Enqueue(tc.task, tc.opts...)
		if err != nil {
			t.Fatal(err)
		}
		enqueued := h.GetEnqueuedMessages(t, r, tc.queue)
		if len(enqueued) != 1 {
			t.Errorf("%s;\nexpected queue %q to have one message; got %d messages in the queue.",
				tc.desc, tc.queue, len(enqueued))
			continue
		}
		got := enqueued[0]
		if diff := cmp.Diff(tc.want, got, h.IgnoreIDOpt); diff != "" {
			t.Errorf("%s;\nmismatch found in enqueued task message; (-want,+got)\n%s",
				tc.desc, diff)
		}
	}
}

func TestUniqueKey(t *testing.T) {
	tests := []struct {
		desc  string
		task  *Task
		ttl   time.Duration
		qname string
		want  string
	}{
		{
			"with zero TTL",
			NewTask("email:send", map[string]interface{}{"a": 123, "b": "hello", "c": true}),
			0,
			"default",
			"",
		},
		{
			"with primitive types",
			NewTask("email:send", map[string]interface{}{"a": 123, "b": "hello", "c": true}),
			10 * time.Minute,
			"default",
			"email:send:a=123,b=hello,c=true:default",
		},
		{
			"with unsorted keys",
			NewTask("email:send", map[string]interface{}{"b": "hello", "c": true, "a": 123}),
			10 * time.Minute,
			"default",
			"email:send:a=123,b=hello,c=true:default",
		},
		{
			"with composite types",
			NewTask("email:send",
				map[string]interface{}{
					"address": map[string]string{"line": "123 Main St", "city": "Boston", "state": "MA"},
					"names":   []string{"bob", "mike", "rob"}}),
			10 * time.Minute,
			"default",
			"email:send:address=map[city:Boston line:123 Main St state:MA],names=[bob mike rob]:default",
		},
		{
			"with complex types",
			NewTask("email:send",
				map[string]interface{}{
					"time":     time.Date(2020, time.July, 28, 0, 0, 0, 0, time.UTC),
					"duration": time.Hour}),
			10 * time.Minute,
			"default",
			"email:send:duration=1h0m0s,time=2020-07-28 00:00:00 +0000 UTC:default",
		},
		{
			"with nil payload",
			NewTask("reindex", nil),
			10 * time.Minute,
			"default",
			"reindex:nil:default",
		},
	}

	for _, tc := range tests {
		got := uniqueKey(tc.task, tc.ttl, tc.qname)
		if got != tc.want {
			t.Errorf("%s: uniqueKey(%v, %v, %q) = %q, want %q", tc.desc, tc.task, tc.ttl, tc.qname, got, tc.want)
		}
	}
}

func TestEnqueueUnique(t *testing.T) {
	r := setup(t)
	c := NewClient(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

	tests := []struct {
		task *Task
		ttl  time.Duration
	}{
		{
			NewTask("email", map[string]interface{}{"user_id": 123}),
			time.Hour,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.

		// Enqueue the task first. It should succeed.
		err := c.Enqueue(tc.task, Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := r.TTL(uniqueKey(tc.task, tc.ttl, base.DefaultQueueName)).Val()
		if !cmp.Equal(tc.ttl.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, tc.ttl)
			continue
		}

		// Enqueue the task again. It should fail.
		err = c.Enqueue(tc.task, Unique(tc.ttl))
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

func TestEnqueueInUnique(t *testing.T) {
	r := setup(t)
	c := NewClient(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
		err := c.EnqueueIn(tc.d, tc.task, Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := r.TTL(uniqueKey(tc.task, tc.ttl, base.DefaultQueueName)).Val()
		wantTTL := time.Duration(tc.ttl.Seconds()+tc.d.Seconds()) * time.Second
		if !cmp.Equal(wantTTL.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, wantTTL)
			continue
		}

		// Enqueue the task again. It should fail.
		err = c.EnqueueIn(tc.d, tc.task, Unique(tc.ttl))
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

func TestEnqueueAtUnique(t *testing.T) {
	r := setup(t)
	c := NewClient(RedisClientOpt{
		Addr: redisAddr,
		DB:   redisDB,
	})

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
		err := c.EnqueueAt(tc.at, tc.task, Unique(tc.ttl))
		if err != nil {
			t.Fatal(err)
		}

		gotTTL := r.TTL(uniqueKey(tc.task, tc.ttl, base.DefaultQueueName)).Val()
		wantTTL := tc.at.Add(tc.ttl).Sub(time.Now())
		if !cmp.Equal(wantTTL.Seconds(), gotTTL.Seconds(), cmpopts.EquateApprox(0, 1)) {
			t.Errorf("TTL = %v, want %v", gotTTL, wantTTL)
			continue
		}

		// Enqueue the task again. It should fail.
		err = c.EnqueueAt(tc.at, tc.task, Unique(tc.ttl))
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
