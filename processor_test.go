// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

// fakeHeartbeater receives from starting and finished channels and do nothing.
func fakeHeartbeater(starting, finished <-chan *base.TaskMessage, done <-chan struct{}) {
	for {
		select {
		case <-starting:
		case <-finished:
		case <-done:
			return
		}
	}
}

// fakeSyncer receives from sync channel and do nothing.
func fakeSyncer(syncCh <-chan *syncRequest, done <-chan struct{}) {
	for {
		select {
		case <-syncCh:
		case <-done:
			return
		}
	}
}

func TestProcessorSuccess(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("gen_thumbnail", nil)
	m3 := h.NewTaskMessage("reindex", nil)
	m4 := h.NewTaskMessage("sync", nil)

	t1 := NewTask(m1.Type, m1.Payload)
	t2 := NewTask(m2.Type, m2.Payload)
	t3 := NewTask(m3.Type, m3.Payload)
	t4 := NewTask(m4.Type, m4.Payload)

	tests := []struct {
		enqueued      []*base.TaskMessage // initial default queue state
		incoming      []*base.TaskMessage // tasks to be enqueued during run
		wantProcessed []*Task             // tasks to be processed at the end
	}{
		{
			enqueued:      []*base.TaskMessage{m1},
			incoming:      []*base.TaskMessage{m2, m3, m4},
			wantProcessed: []*Task{t1, t2, t3, t4},
		},
		{
			enqueued:      []*base.TaskMessage{},
			incoming:      []*base.TaskMessage{m1},
			wantProcessed: []*Task{t1},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)                        // clean up db before each test case.
		h.SeedEnqueuedQueue(t, r, tc.enqueued) // initialize default queue.

		// instantiate a new processor
		var mu sync.Mutex
		var processed []*Task
		handler := func(ctx context.Context, task *Task) error {
			mu.Lock()
			defer mu.Unlock()
			processed = append(processed, task)
			return nil
		}
		starting := make(chan *base.TaskMessage)
		finished := make(chan *base.TaskMessage)
		syncCh := make(chan *syncRequest)
		done := make(chan struct{})
		defer func() { close(done) }()
		go fakeHeartbeater(starting, finished, done)
		go fakeSyncer(syncCh, done)
		p := newProcessor(processorParams{
			logger:          testLogger,
			broker:          rdbClient,
			retryDelayFunc:  defaultDelayFunc,
			syncCh:          syncCh,
			cancelations:    base.NewCancelations(),
			concurrency:     10,
			queues:          defaultQueueConfig,
			strictPriority:  false,
			errHandler:      nil,
			shutdownTimeout: defaultShutdownTimeout,
			starting:        starting,
			finished:        finished,
		})
		p.handler = HandlerFunc(handler)

		p.start(&sync.WaitGroup{})
		for _, msg := range tc.incoming {
			err := rdbClient.Enqueue(msg)
			if err != nil {
				p.terminate()
				t.Fatal(err)
			}
		}
		time.Sleep(2 * time.Second) // wait for two second to allow all enqueued tasks to be processed.
		if l := r.LLen(base.InProgressQueue).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", base.InProgressQueue, l)
		}
		p.terminate()

		mu.Lock()
		if diff := cmp.Diff(tc.wantProcessed, processed, sortTaskOpt, cmp.AllowUnexported(Payload{})); diff != "" {
			t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
		}
		mu.Unlock()
	}
}

// https://github.com/hibiken/asynq/issues/166
func TestProcessTasksWithLargeNumberInPayload(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := h.NewTaskMessage("large_number", map[string]interface{}{"data": 111111111111111111})
	t1 := NewTask(m1.Type, m1.Payload)

	tests := []struct {
		enqueued      []*base.TaskMessage // initial default queue state
		wantProcessed []*Task             // tasks to be processed at the end
	}{
		{
			enqueued:      []*base.TaskMessage{m1},
			wantProcessed: []*Task{t1},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)                        // clean up db before each test case.
		h.SeedEnqueuedQueue(t, r, tc.enqueued) // initialize default queue.

		var mu sync.Mutex
		var processed []*Task
		handler := func(ctx context.Context, task *Task) error {
			mu.Lock()
			defer mu.Unlock()
			if data, err := task.Payload.GetInt("data"); err != nil {
				t.Errorf("coult not get data from payload: %v", err)
			} else {
				t.Logf("data == %d", data)
			}
			processed = append(processed, task)
			return nil
		}
		starting := make(chan *base.TaskMessage)
		finished := make(chan *base.TaskMessage)
		syncCh := make(chan *syncRequest)
		done := make(chan struct{})
		defer func() { close(done) }()
		go fakeHeartbeater(starting, finished, done)
		go fakeSyncer(syncCh, done)
		p := newProcessor(processorParams{
			logger:          testLogger,
			broker:          rdbClient,
			retryDelayFunc:  defaultDelayFunc,
			syncCh:          syncCh,
			cancelations:    base.NewCancelations(),
			concurrency:     10,
			queues:          defaultQueueConfig,
			strictPriority:  false,
			errHandler:      nil,
			shutdownTimeout: defaultShutdownTimeout,
			starting:        starting,
			finished:        finished,
		})
		p.handler = HandlerFunc(handler)

		p.start(&sync.WaitGroup{})
		time.Sleep(2 * time.Second) // wait for two second to allow all enqueued tasks to be processed.
		if l := r.LLen(base.InProgressQueue).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", base.InProgressQueue, l)
		}
		p.terminate()

		mu.Lock()
		if diff := cmp.Diff(tc.wantProcessed, processed, sortTaskOpt, cmpopts.IgnoreUnexported(Payload{})); diff != "" {
			t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
		}
		mu.Unlock()
	}
}

func TestProcessorRetry(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := h.NewTaskMessage("send_email", nil)
	m1.Retried = m1.Retry // m1 has reached its max retry count
	m2 := h.NewTaskMessage("gen_thumbnail", nil)
	m3 := h.NewTaskMessage("reindex", nil)
	m4 := h.NewTaskMessage("sync", nil)

	errMsg := "something went wrong"
	now := time.Now()

	tests := []struct {
		enqueued     []*base.TaskMessage // initial default queue state
		incoming     []*base.TaskMessage // tasks to be enqueued during run
		delay        time.Duration       // retry delay duration
		handler      Handler             // task handler
		wait         time.Duration       // wait duration between starting and stopping processor for this test case
		wantRetry    []base.Z            // tasks in retry queue at the end
		wantDead     []*base.TaskMessage // tasks in dead queue at the end
		wantErrCount int                 // number of times error handler should be called
	}{
		{
			enqueued: []*base.TaskMessage{m1, m2},
			incoming: []*base.TaskMessage{m3, m4},
			delay:    time.Minute,
			handler: HandlerFunc(func(ctx context.Context, task *Task) error {
				return fmt.Errorf(errMsg)
			}),
			wait: 2 * time.Second,
			wantRetry: []base.Z{
				{Message: h.TaskMessageAfterRetry(*m2, errMsg), Score: now.Add(time.Minute).Unix()},
				{Message: h.TaskMessageAfterRetry(*m3, errMsg), Score: now.Add(time.Minute).Unix()},
				{Message: h.TaskMessageAfterRetry(*m4, errMsg), Score: now.Add(time.Minute).Unix()},
			},
			wantDead:     []*base.TaskMessage{h.TaskMessageWithError(*m1, errMsg)},
			wantErrCount: 4,
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)                        // clean up db before each test case.
		h.SeedEnqueuedQueue(t, r, tc.enqueued) // initialize default queue.

		// instantiate a new processor
		delayFunc := func(n int, e error, t *Task) time.Duration {
			return tc.delay
		}
		var (
			mu sync.Mutex // guards n
			n  int        // number of times error handler is called
		)
		errHandler := func(ctx context.Context, t *Task, err error) {
			mu.Lock()
			defer mu.Unlock()
			n++
		}
		starting := make(chan *base.TaskMessage)
		finished := make(chan *base.TaskMessage)
		done := make(chan struct{})
		defer func() { close(done) }()
		go fakeHeartbeater(starting, finished, done)
		p := newProcessor(processorParams{
			logger:          testLogger,
			broker:          rdbClient,
			retryDelayFunc:  delayFunc,
			syncCh:          nil,
			cancelations:    base.NewCancelations(),
			concurrency:     10,
			queues:          defaultQueueConfig,
			strictPriority:  false,
			errHandler:      ErrorHandlerFunc(errHandler),
			shutdownTimeout: defaultShutdownTimeout,
			starting:        starting,
			finished:        finished,
		})
		p.handler = tc.handler

		p.start(&sync.WaitGroup{})
		for _, msg := range tc.incoming {
			err := rdbClient.Enqueue(msg)
			if err != nil {
				p.terminate()
				t.Fatal(err)
			}
		}
		time.Sleep(tc.wait) // FIXME: This makes test flaky.
		p.terminate()

		cmpOpt := cmpopts.EquateApprox(0, float64(time.Second)) // allow up to a second difference in zset score
		gotRetry := h.GetRetryEntries(t, r)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, h.SortZSetEntryOpt, cmpOpt); diff != "" {
			t.Errorf("mismatch found in %q after running processor; (-want, +got)\n%s", base.RetryQueue, diff)
		}

		gotDead := h.GetDeadMessages(t, r)
		if diff := cmp.Diff(tc.wantDead, gotDead, h.SortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after running processor; (-want, +got)\n%s", base.DeadQueue, diff)
		}

		if l := r.LLen(base.InProgressQueue).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", base.InProgressQueue, l)
		}

		if n != tc.wantErrCount {
			t.Errorf("error handler was called %d times, want %d", n, tc.wantErrCount)
		}
	}
}

func TestProcessorQueues(t *testing.T) {
	sortOpt := cmp.Transformer("SortStrings", func(in []string) []string {
		out := append([]string(nil), in...) // Copy input to avoid mutating it
		sort.Strings(out)
		return out
	})

	tests := []struct {
		queueCfg map[string]int
		want     []string
	}{
		{
			queueCfg: map[string]int{
				"high":    6,
				"default": 3,
				"low":     1,
			},
			want: []string{"high", "default", "low"},
		},
		{
			queueCfg: map[string]int{
				"default": 1,
			},
			want: []string{"default"},
		},
	}

	for _, tc := range tests {
		starting := make(chan *base.TaskMessage)
		finished := make(chan *base.TaskMessage)
		done := make(chan struct{})
		defer func() { close(done) }()
		go fakeHeartbeater(starting, finished, done)
		p := newProcessor(processorParams{
			logger:          testLogger,
			broker:          nil,
			retryDelayFunc:  defaultDelayFunc,
			syncCh:          nil,
			cancelations:    base.NewCancelations(),
			concurrency:     10,
			queues:          tc.queueCfg,
			strictPriority:  false,
			errHandler:      nil,
			shutdownTimeout: defaultShutdownTimeout,
			starting:        starting,
			finished:        finished,
		})
		got := p.queues()
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("with queue config: %v\n(*processor).queues() = %v, want %v\n(-want,+got):\n%s",
				tc.queueCfg, got, tc.want, diff)
		}
	}
}

func TestProcessorWithStrictPriority(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	m1 := h.NewTaskMessage("send_email", nil)
	m2 := h.NewTaskMessage("send_email", nil)
	m3 := h.NewTaskMessage("send_email", nil)
	m4 := h.NewTaskMessage("gen_thumbnail", nil)
	m5 := h.NewTaskMessage("gen_thumbnail", nil)
	m6 := h.NewTaskMessage("sync", nil)
	m7 := h.NewTaskMessage("sync", nil)

	t1 := NewTask(m1.Type, m1.Payload)
	t2 := NewTask(m2.Type, m2.Payload)
	t3 := NewTask(m3.Type, m3.Payload)
	t4 := NewTask(m4.Type, m4.Payload)
	t5 := NewTask(m5.Type, m5.Payload)
	t6 := NewTask(m6.Type, m6.Payload)
	t7 := NewTask(m7.Type, m7.Payload)

	tests := []struct {
		enqueued      map[string][]*base.TaskMessage // initial queues state
		wait          time.Duration                  // wait duration between starting and stopping processor for this test case
		wantProcessed []*Task                        // tasks to be processed at the end
	}{
		{
			enqueued: map[string][]*base.TaskMessage{
				base.DefaultQueueName: {m4, m5},
				"critical":            {m1, m2, m3},
				"low":                 {m6, m7},
			},
			wait:          time.Second,
			wantProcessed: []*Task{t1, t2, t3, t4, t5, t6, t7},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r) // clean up db before each test case.
		for qname, msgs := range tc.enqueued {
			h.SeedEnqueuedQueue(t, r, msgs, qname)
		}

		// instantiate a new processor
		var mu sync.Mutex
		var processed []*Task
		handler := func(ctx context.Context, task *Task) error {
			mu.Lock()
			defer mu.Unlock()
			processed = append(processed, task)
			return nil
		}
		queueCfg := map[string]int{
			"critical":            3,
			base.DefaultQueueName: 2,
			"low":                 1,
		}
		starting := make(chan *base.TaskMessage)
		finished := make(chan *base.TaskMessage)
		done := make(chan struct{})
		defer func() { close(done) }()
		go fakeHeartbeater(starting, finished, done)
		p := newProcessor(processorParams{
			logger:          testLogger,
			broker:          rdbClient,
			retryDelayFunc:  defaultDelayFunc,
			syncCh:          nil,
			cancelations:    base.NewCancelations(),
			concurrency:     1, // Set concurrency to 1 to make sure tasks are processed one at a time.
			queues:          queueCfg,
			strictPriority:  true,
			errHandler:      nil,
			shutdownTimeout: defaultShutdownTimeout,
			starting:        starting,
			finished:        finished,
		})
		p.handler = HandlerFunc(handler)

		p.start(&sync.WaitGroup{})
		time.Sleep(tc.wait)
		p.terminate()

		if diff := cmp.Diff(tc.wantProcessed, processed, cmp.AllowUnexported(Payload{})); diff != "" {
			t.Errorf("mismatch found in processed tasks; (-want, +got)\n%s", diff)
		}

		if l := r.LLen(base.InProgressQueue).Val(); l != 0 {
			t.Errorf("%q has %d tasks, want 0", base.InProgressQueue, l)
		}
	}
}

func TestPerform(t *testing.T) {
	tests := []struct {
		desc    string
		handler HandlerFunc
		task    *Task
		wantErr bool
	}{
		{
			desc: "handler returns nil",
			handler: func(ctx context.Context, t *Task) error {
				return nil
			},
			task:    NewTask("gen_thumbnail", map[string]interface{}{"src": "some/img/path"}),
			wantErr: false,
		},
		{
			desc: "handler returns error",
			handler: func(ctx context.Context, t *Task) error {
				return fmt.Errorf("something went wrong")
			},
			task:    NewTask("gen_thumbnail", map[string]interface{}{"src": "some/img/path"}),
			wantErr: true,
		},
		{
			desc: "handler panics",
			handler: func(ctx context.Context, t *Task) error {
				panic("something went terribly wrong")
			},
			task:    NewTask("gen_thumbnail", map[string]interface{}{"src": "some/img/path"}),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		got := perform(context.Background(), tc.task, tc.handler)
		if !tc.wantErr && got != nil {
			t.Errorf("%s: perform() = %v, want nil", tc.desc, got)
			continue
		}
		if tc.wantErr && got == nil {
			t.Errorf("%s: perform() = nil, want non-nil error", tc.desc)
			continue
		}
	}
}

func TestGCD(t *testing.T) {
	tests := []struct {
		input []int
		want  int
	}{
		{[]int{6, 2, 12}, 2},
		{[]int{3, 3, 3}, 3},
		{[]int{6, 3, 1}, 1},
		{[]int{1}, 1},
		{[]int{1, 0, 2}, 1},
		{[]int{8, 0, 4}, 4},
		{[]int{9, 12, 18, 30}, 3},
	}

	for _, tc := range tests {
		got := gcd(tc.input...)
		if got != tc.want {
			t.Errorf("gcd(%v) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestNormalizeQueues(t *testing.T) {
	tests := []struct {
		input map[string]int
		want  map[string]int
	}{
		{
			input: map[string]int{
				"high":    100,
				"default": 20,
				"low":     5,
			},
			want: map[string]int{
				"high":    20,
				"default": 4,
				"low":     1,
			},
		},
		{
			input: map[string]int{
				"default": 10,
			},
			want: map[string]int{
				"default": 1,
			},
		},
		{
			input: map[string]int{
				"critical": 5,
				"default":  1,
			},
			want: map[string]int{
				"critical": 5,
				"default":  1,
			},
		},
		{
			input: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      0,
			},
			want: map[string]int{
				"critical": 2,
				"default":  1,
				"low":      0,
			},
		},
	}

	for _, tc := range tests {
		got := normalizeQueues(tc.input)
		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("normalizeQueues(%v) = %v, want %v; (-want, +got):\n%s",
				tc.input, got, tc.want, diff)
		}
	}
}
