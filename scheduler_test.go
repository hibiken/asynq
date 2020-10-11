// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
)

func TestScheduler(t *testing.T) {
	tests := []struct {
		cronspec string
		task     *Task
		opts     []Option
		wait     time.Duration
		queue    string
		want     []*base.TaskMessage
	}{
		{
			cronspec: "@every 3s",
			task:     NewTask("task1", nil),
			opts:     []Option{MaxRetry(10)},
			wait:     10 * time.Second,
			queue:    "default",
			want: []*base.TaskMessage{
				{
					Type:    "task1",
					Payload: nil,
					Retry:   10,
					Timeout: int64(defaultTimeout.Seconds()),
					Queue:   "default",
				},
				{
					Type:    "task1",
					Payload: nil,
					Retry:   10,
					Timeout: int64(defaultTimeout.Seconds()),
					Queue:   "default",
				},
				{
					Type:    "task1",
					Payload: nil,
					Retry:   10,
					Timeout: int64(defaultTimeout.Seconds()),
					Queue:   "default",
				},
			},
		},
	}

	r := setup(t)

	for _, tc := range tests {
		scheduler := NewScheduler(getRedisConnOpt(t), nil)
		if _, err := scheduler.Register(tc.cronspec, tc.task, tc.opts...); err != nil {
			t.Fatal(err)
		}

		if err := scheduler.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(tc.wait)
		if err := scheduler.Stop(); err != nil {
			t.Fatal(err)
		}

		got := asynqtest.GetPendingMessages(t, r, tc.queue)
		if diff := cmp.Diff(tc.want, got, asynqtest.IgnoreIDOpt); diff != "" {
			t.Errorf("mismatch found in queue %q: (-want,+got)\n%s", tc.queue, diff)
		}
	}
}

func TestSchedulerWhenRedisDown(t *testing.T) {
	var (
		mu      sync.Mutex
		counter int
	)
	errorHandler := func(task *Task, opts []Option, err error) {
		mu.Lock()
		counter++
		mu.Unlock()
	}

	// Connect to non-existent redis instance to simulate a redis server being down.
	scheduler := NewScheduler(
		RedisClientOpt{Addr: ":9876"},
		&SchedulerOpts{EnqueueErrorHandler: errorHandler},
	)

	task := NewTask("test", nil)

	if _, err := scheduler.Register("@every 3s", task); err != nil {
		t.Fatal(err)
	}

	if err := scheduler.Start(); err != nil {
		t.Fatal(err)
	}
	// Scheduler should attempt to enqueue the task three times (every 3s).
	time.Sleep(10 * time.Second)
	if err := scheduler.Stop(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	if counter != 3 {
		t.Errorf("EnqueueErrorHandler was called %d times, want 3", counter)
	}
	mu.Unlock()
}
