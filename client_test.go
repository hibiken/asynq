// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
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

		noTimeout  = time.Duration(0).String()
		noDeadline = time.Time{}.Format(time.RFC3339)
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

	var (
		noTimeout  = time.Duration(0).String()
		noDeadline = time.Time{}.Format(time.RFC3339)
	)

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

	var (
		noTimeout  = time.Duration(0).String()
		noDeadline = time.Time{}.Format(time.RFC3339)
	)

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
