// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/testbroker"
	h "github.com/hibiken/asynq/internal/testutil"
)

// Regression: a sync request issued after a Redis failure must stay usable
// after the originating call has returned (its context must not be canceled).
func TestProcessorSyncRequestUsesLiveContext(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	tb := testbroker.NewTestBroker(rdbClient)

	newP := func(syncCh chan *syncRequest) *processor {
		return newProcessor(processorParams{
			logger:            testLogger,
			broker:            tb,
			baseCtxFn:         context.Background,
			retryDelayFunc:    func(int, error, *Task) time.Duration { return time.Second },
			taskCheckInterval: defaultTaskCheckInterval,
			isFailureFunc:     defaultIsFailureFunc,
			syncCh:            syncCh,
			cancelations:      base.NewCancelations(),
			concurrency:       1,
			queues:            defaultQueueConfig,
			shutdownTimeout:   defaultShutdownTimeout,
		})
	}

	tests := []struct {
		desc string
		run  func(p *processor, l *base.Lease, msg *base.TaskMessage)
	}{
		{"markAsDone", func(p *processor, l *base.Lease, msg *base.TaskMessage) { p.markAsDone(l, msg) }},
		{"markAsComplete", func(p *processor, l *base.Lease, msg *base.TaskMessage) {
			msg.Retention = 3600
			p.markAsComplete(l, msg)
		}},
		{"retry", func(p *processor, l *base.Lease, msg *base.TaskMessage) {
			msg.Retry = 3
			p.retry(l, msg, context.DeadlineExceeded, true)
		}},
		{"archive", func(p *processor, l *base.Lease, msg *base.TaskMessage) {
			p.archive(l, msg, context.DeadlineExceeded)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			h.FlushDB(t, r)
			msg := h.NewTaskMessage("send_email", nil)
			h.SeedActiveQueue(t, r, []*base.TaskMessage{msg}, base.DefaultQueueName)
			h.SeedLease(t, r, []base.Z{{Message: msg, Score: time.Now().Add(30 * time.Second).Unix()}}, base.DefaultQueueName)

			syncCh := make(chan *syncRequest, 1)
			p := newP(syncCh)
			lease := base.NewLease(time.Now().Add(30 * time.Second))

			tb.Sleep() // simulate redis outage during the first attempt
			tc.run(p, lease, msg)
			tb.Wakeup()

			var req *syncRequest
			select {
			case req = <-syncCh:
			default:
				t.Fatal("no sync request was issued")
			}
			if err := req.fn(); err != nil {
				t.Fatalf("sync retry failed: %v", err)
			}
			if got := h.GetActiveMessages(t, r, base.DefaultQueueName); len(got) != 0 {
				t.Errorf("active queue has %d messages after sync retry, want 0", len(got))
			}
		})
	}
}

// Regression: an idle processor sleeping between polls must react to stop
// promptly instead of waiting for the whole check interval.
func TestProcessorStopInterruptsIdleSleep(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	p := newProcessor(processorParams{
		logger:            testLogger,
		broker:            rdbClient,
		baseCtxFn:         context.Background,
		retryDelayFunc:    DefaultRetryDelayFunc,
		taskCheckInterval: 20 * time.Second,
		isFailureFunc:     defaultIsFailureFunc,
		syncCh:            make(chan *syncRequest),
		cancelations:      base.NewCancelations(),
		concurrency:       1,
		queues:            defaultQueueConfig,
		shutdownTimeout:   defaultShutdownTimeout,
	})
	var wg sync.WaitGroup
	p.start(&wg)
	time.Sleep(300 * time.Millisecond) // let it find the queue empty and go to sleep

	start := time.Now()
	finished := make(chan struct{})
	go func() {
		p.shutdown()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatalf("processor did not stop within 5s while idle (check interval 20s)")
	}
	t.Logf("processor stopped after %v", time.Since(start))
	wg.Wait()
}

type startTestHandler struct{ id int }

func (startTestHandler) ProcessTask(context.Context, *Task) error { return nil }

// Regression: a rejected second Start must not replace the running handler.
func TestServerStartTwiceKeepsOriginalHandler(t *testing.T) {
	srv := NewServer(getRedisConnOpt(t), Config{LogLevel: testLogLevel})
	h1, h2 := &startTestHandler{1}, &startTestHandler{2}
	if err := srv.Start(h1); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()
	if err := srv.Start(h2); err == nil {
		t.Fatal("second Start should have failed")
	}
	if srv.processor.handler != Handler(h1) {
		t.Errorf("handler was replaced by a rejected Start call")
	}
}

// Regression: sub-second timeouts must not silently fall back to the default timeout.
func TestEnqueueSubSecondTimeout(t *testing.T) {
	r := setup(t)
	defer r.Close()
	c := NewClient(getRedisConnOpt(t))
	defer c.Close()

	tests := []struct {
		timeout time.Duration
		want    int64
	}{
		{500 * time.Millisecond, 1},
		{1500 * time.Millisecond, 2},
		{2 * time.Second, 2},
	}
	for _, tc := range tests {
		h.FlushDB(t, r)
		if _, err := c.Enqueue(NewTask("t", nil), Timeout(tc.timeout)); err != nil {
			t.Fatal(err)
		}
		msgs := h.GetPendingMessages(t, r, base.DefaultQueueName)
		if len(msgs) != 1 {
			t.Fatalf("got %d pending messages, want 1", len(msgs))
		}
		if msgs[0].Timeout != tc.want {
			t.Errorf("Timeout(%v) stored as %d seconds, want %d", tc.timeout, msgs[0].Timeout, tc.want)
		}
	}
}

// End to end: a server configured with a TaskStateProber must construct
// without blocking and must report the state changes of processed tasks.
func TestServerWithTaskStateProber(t *testing.T) {
	r := setup(t)
	defer r.Close()

	var mu sync.Mutex
	seen := map[string]bool{}
	got := make(chan struct{}, 16)

	built := make(chan *Server, 1)
	go func() {
		built <- NewServer(getRedisConnOpt(t), Config{
			LogLevel: testLogLevel,
			TaskStateProber: &TaskStateProber{
				Handler: func(out map[string]any) {
					mu.Lock()
					if s, ok := out["state"].(string); ok {
						seen[s] = true
					}
					mu.Unlock()
					got <- struct{}{}
				},
			},
		})
	}()
	var srv *Server
	select {
	case srv = <-built:
	case <-time.After(5 * time.Second):
		t.Fatal("NewServer with TaskStateProber blocked")
	}

	if err := srv.Start(HandlerFunc(func(context.Context, *Task) error { return nil })); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	c := NewClient(getRedisConnOpt(t))
	defer c.Close()
	if _, err := c.Enqueue(NewTask("t", nil)); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		ok := seen["pending"]
		mu.Unlock()
		if ok {
			return
		}
		select {
		case <-got:
		case <-deadline:
			t.Fatalf("no pending event observed, seen=%v", seen)
		}
	}
}

// Regression: panics in user supplied callbacks must not crash the process
// and the task must still be retried.
func TestProcessorSurvivesCallbackPanics(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	msg := h.NewTaskMessage("boom", nil)
	msg.Retry = 3
	msg.Timeout = 60
	h.SeedPendingQueue(t, r, []*base.TaskMessage{msg}, base.DefaultQueueName)

	done := make(chan struct{})
	var once sync.Once
	handler := HandlerFunc(func(context.Context, *Task) error {
		once.Do(func() { defer close(done) })
		return fmt.Errorf("task failed")
	})

	p := newProcessor(processorParams{
		logger:            testLogger,
		broker:            rdbClient,
		baseCtxFn:         func() context.Context { panic("baseCtx boom") },
		retryDelayFunc:    func(int, error, *Task) time.Duration { panic("retryDelay boom") },
		taskCheckInterval: 100 * time.Millisecond,
		isFailureFunc:     func(error) bool { panic("isFailure boom") },
		syncCh:            make(chan *syncRequest, 8),
		cancelations:      base.NewCancelations(),
		concurrency:       1,
		queues:            defaultQueueConfig,
		errHandler:        ErrorHandlerFunc(func(context.Context, *Task, error) { panic("errHandler boom") }),
		shutdownTimeout:   defaultShutdownTimeout,
		starting:          nil,
		finished:          nil,
	})
	starting := make(chan *workerInfo)
	finished := make(chan *base.TaskMessage)
	stopHB := make(chan struct{})
	defer close(stopHB)
	go fakeHeartbeater(starting, finished, stopHB)
	p.starting, p.finished = starting, finished
	p.handler = handler

	var wg sync.WaitGroup
	p.start(&wg)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("task was never processed")
	}
	time.Sleep(500 * time.Millisecond) // let the failure path finish
	p.shutdown()
	wg.Wait()

	if got := h.GetRetryMessages(t, r, base.DefaultQueueName); len(got) != 1 {
		t.Errorf("retry set has %d messages, want 1 (task should be retried despite callback panics)", len(got))
	}
}

// Regression: a slow redis must not block the processor through the heartbeater.
func TestHeartbeaterDoesNotBlockWorkersWhileFlushing(t *testing.T) {
	r := setup(t)
	defer r.Close()
	tb := testbroker.NewTestBroker(rdb.NewRDB(r))
	tb.SetDelay(1500 * time.Millisecond)

	starting := make(chan *workerInfo)
	finished := make(chan *base.TaskMessage)
	hb := newHeartbeater(heartbeaterParams{
		logger:         testLogger,
		broker:         tb,
		interval:       100 * time.Millisecond,
		concurrency:    1,
		queues:         map[string]int{"default": 1},
		strictPriority: false,
		state:          &serverState{value: srvStateActive},
		starting:       starting,
		finished:       finished,
	})
	var wg sync.WaitGroup
	hb.start(&wg)
	defer func() {
		hb.shutdown()
		wg.Wait()
	}()

	time.Sleep(300 * time.Millisecond) // a flush is now stuck inside the slow broker

	msg := h.NewTaskMessage("t", nil)
	sent := make(chan struct{})
	go func() {
		starting <- &workerInfo{msg: msg, started: time.Now(), deadline: time.Now().Add(time.Minute), lease: base.NewLease(time.Now().Add(time.Minute))}
		finished <- msg
		close(sent)
	}()
	select {
	case <-sent:
	case <-time.After(700 * time.Millisecond):
		t.Fatal("heartbeater blocked starting/finished while redis was slow")
	}
}

// Regression: a panic inside PreEnqueueFunc must not crash the process and the
// scheduler must keep running its next tick.
func TestSchedulerSurvivesPreEnqueuePanic(t *testing.T) {
	r := setup(t)
	defer r.Close()

	var mu sync.Mutex
	calls := 0
	s := NewScheduler(getRedisConnOpt(t), &SchedulerOpts{
		LogLevel: testLogLevel,
		PreEnqueueFunc: func(*Task, []Option) {
			mu.Lock()
			calls++
			mu.Unlock()
			panic("pre-enqueue boom")
		},
	})
	if _, err := s.Register("@every 1s", NewTask("t", nil)); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	s.Shutdown()

	mu.Lock()
	defer mu.Unlock()
	if calls < 2 {
		t.Errorf("PreEnqueueFunc called %d times, want >= 2 (scheduler should survive panics)", calls)
	}
}

// Regression: the recoverer must still recover the readable tasks when one
// queue contains an entry that cannot be decoded.
func TestRecovererRecoversDespitePartialListFailure(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	ctx := context.Background()

	good := h.NewTaskMessageWithQueue("good", nil, "other")
	good.Retry = 3
	h.SeedActiveQueue(t, r, []*base.TaskMessage{good}, "other")
	h.SeedLease(t, r, []base.Z{{Message: good, Score: time.Now().Add(-time.Hour).Unix()}}, "other")

	// an undecodable lease entry in another queue
	r.HSet(ctx, base.TaskKey("default", "corrupt"), "msg", "garbage-\xff\xff")
	r.ZAdd(ctx, base.LeaseKey("default"), redis.Z{Score: float64(time.Now().Add(-time.Hour).Unix()), Member: "corrupt"})

	rc := newRecoverer(recovererParams{
		logger:         testLogger,
		broker:         rdbClient,
		queues:         []string{"default", "other"},
		interval:       time.Hour,
		retryDelayFunc: func(int, error, *Task) time.Duration { return time.Minute },
		isFailureFunc:  defaultIsFailureFunc,
	})
	rc.recoverLeaseExpiredTasks()

	if got := h.GetRetryMessages(t, r, "other"); len(got) != 1 || got[0].ID != good.ID {
		t.Errorf("good task was not recovered: retry set = %v", got)
	}
	if got := h.GetActiveMessages(t, r, "other"); len(got) != 0 {
		t.Errorf("good task is still active: %v", got)
	}
}

// Regression: non-positive page size / page number must not turn a list call
// into an unbounded scan or into a read from the end of the list.
func TestInspectorListOptionsDegenerateValues(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	const total = 100
	msgs := make([]*base.TaskMessage, total)
	for i := range msgs {
		msgs[i] = h.NewTaskMessage(fmt.Sprintf("task%03d", i), nil)
	}
	// SeedPendingQueue pushes in order, so msgs[0] is the next one to run.
	h.SeedPendingQueue(t, r, msgs, base.DefaultQueueName)

	tests := []struct {
		desc      string
		opts      []ListOption
		wantLen   int
		wantFirst string
	}{
		{"default options", nil, defaultPageSize, "task000"},
		{"PageSize(0) falls back to the default size", []ListOption{PageSize(0)}, defaultPageSize, "task000"},
		{"PageSize(-5) falls back to the default size", []ListOption{PageSize(-5)}, defaultPageSize, "task000"},
		{"PageSize(0) with Page(2) is still paginated", []ListOption{PageSize(0), Page(2)}, defaultPageSize, "task030"},
		{"Page(0) is the first page", []ListOption{Page(0), PageSize(10)}, 10, "task000"},
		{"Page(-3) is the first page", []ListOption{Page(-3), PageSize(10)}, 10, "task000"},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := inspector.ListPendingTasks(base.DefaultQueueName, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tc.wantLen {
				t.Fatalf("got %d tasks, want %d", len(got), tc.wantLen)
			}
			if got[0].Type != tc.wantFirst {
				t.Errorf("first task = %q, want %q", got[0].Type, tc.wantFirst)
			}
		})
	}
}

// slowForwardBroker 讓 ForwardIfReady 卡住，模擬 redis 回應緩慢。
type slowForwardBroker struct {
	base.Broker
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *slowForwardBroker) ForwardIfReady(qnames ...string) error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return nil
}

// 背景迴圈正在執行慢操作時，shutdown() 只應發出訊號，不能阻塞呼叫端，
// 否則 Server.Shutdown 會把各元件的等待時間逐個累加。
func TestBackgroundShutdownSignalDoesNotBlockOnBusyLoop(t *testing.T) {
	b := &slowForwardBroker{entered: make(chan struct{}), release: make(chan struct{})}
	f := newForwarder(forwarderParams{
		logger:   testLogger,
		broker:   b,
		queues:   []string{"default"},
		interval: 10 * time.Millisecond,
	})
	var wg sync.WaitGroup
	f.start(&wg)
	<-b.entered

	returned := make(chan struct{})
	go func() {
		f.shutdown()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(500 * time.Millisecond):
		close(b.release)
		wg.Wait()
		t.Fatal("forwarder.shutdown blocked while its loop was busy")
	}
	close(b.release)
	wg.Wait()
}
