// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"github.com/hibiken/asynq/internal/timeutil"
	"github.com/redis/go-redis/v9"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/testbroker"
	"github.com/hibiken/asynq/internal/testutil"
	"go.uber.org/goleak"
)

func TestServer(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)
	r, ok := redisConnOpt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		t.Fatalf("asynq: unsupported RedisConnOpt type %T", r)
	}

	c := NewClient(redisConnOpt)
	defer c.Close()

	const timeSlot = time.Millisecond * 100
	tests := []struct {
		queue            string
		delay            time.Duration
		taskNum          int
		concurrency      int
		queueConcurrency int
		wantActiveNum    int
	}{
		{
			queue:         "test-delay",
			delay:         timeSlot * 2,
			taskNum:       4,
			wantActiveNum: 0,
		},
		{
			queue:         "test-concurrency",
			taskNum:       4,
			concurrency:   6,
			wantActiveNum: 4,
		},
		{
			queue:         "test-concurrency-max",
			taskNum:       4,
			concurrency:   2,
			wantActiveNum: 2,
		},
		{
			queue:            "test-queue-concurrency",
			taskNum:          4,
			queueConcurrency: 2,
			wantActiveNum:    2,
		},
	}

	// no-op handler
	handle := func(ctx context.Context, task *Task) error {
		_ = timeutil.Sleep(ctx, timeSlot*2)
		return nil
	}

	for _, tc := range tests {
		t.Run(tc.queue, func(t *testing.T) {
			var err error
			testutil.FlushDB(t, r)
			for i := 0; i < tc.taskNum; i++ {
				_, err = c.Enqueue(NewTask("send_email",
					testutil.JSON(map[string]interface{}{"recipient_id": i + 123})),
					Queue(tc.queue),
					ProcessIn(tc.delay))
				if err != nil {
					t.Errorf("could not enqueue a task: %v", err)
				}
			}

			srv := NewServer(redisConnOpt, Config{
				Concurrency:      tc.concurrency,
				LogLevel:         testLogLevel,
				Queues:           map[string]int{tc.queue: 1},
				QueueConcurrency: map[string]int{tc.queue: tc.queueConcurrency},
			})

			err = srv.Start(HandlerFunc(handle))
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(timeSlot)
			inspector := NewInspector(redisConnOpt)
			tasks, err := inspector.ListActiveTasks(tc.queue)
			if err != nil {
				t.Errorf("could not list active tasks: %v", err)
			}
			if len(tasks) != tc.wantActiveNum {
				t.Errorf("%s queue has %d active tasks, want %d", tc.queue, len(tasks), tc.wantActiveNum)
			}

			srv.Shutdown()
		})
	}
}

func TestServerRun(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("because Sending Interrupt on Windows is not implemented")
		return
	}

	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	done := make(chan struct{})
	go func() {
		mux := NewServeMux()
		srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{LogLevel: testLogLevel})
		if err := srv.Run(mux); err != nil {
			t.Fatal(err)
		}
		done <- struct{}{}
	}()
	time.Sleep(1 * time.Second)

	// Make sure server exits when receiving TERM signal.
	go func() {
		p, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Error("FindProcess:", err)
			t.Error(err)
			return
		}
		err = p.Signal(syscall.SIGTERM)
		if err != nil {
			t.Error("Signal:", err)
			return
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-time.After(10 * time.Second):
			panic("server did not stop after receiving TERM signal")
		case <-done:
		}
	}()

	wg.Wait()
}

func TestServerShutdown(t *testing.T) {
	var err error

	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)
	r, ok := redisConnOpt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		t.Fatalf("asynq: unsupported RedisConnOpt type %T", r)
	}
	testutil.FlushDB(t, r)

	srv := NewServer(redisConnOpt, Config{LogLevel: testLogLevel, ShutdownTimeout: 6 * time.Second})
	done := make(chan struct{})
	mux := NewServeMux()
	mux.HandleFunc("send_email", func(ctx context.Context, task *Task) error {
		err := timeutil.Sleep(ctx, 10*time.Second)
		time.Sleep(1 * time.Second)
		done <- struct{}{}
		return err
	})
	if err := srv.Start(mux); err != nil {
		t.Fatal(err)
	}

	c := NewClient(redisConnOpt)
	defer c.Close()
	_, err = c.Enqueue(NewTask("send_email", testutil.JSON(map[string]interface{}{"recipient_id": 123})))
	if err != nil {
		t.Errorf("could not enqueue a task: %v", err)
	}

	// Make sure active tasks stops when server shutdown.
	go func() {
		time.Sleep(2 * time.Second)
		srv.Shutdown()
	}()
	select {
	case <-time.After(6 * time.Second):
		t.Error("active tasks did not stop after server shutdown")
	case <-done:
	}
}

func TestServerErrServerClosed(t *testing.T) {
	srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{LogLevel: testLogLevel})
	handler := NewServeMux()
	if err := srv.Start(handler); err != nil {
		t.Fatal(err)
	}
	srv.Shutdown()
	err := srv.Start(handler)
	if err != ErrServerClosed {
		t.Errorf("Restarting server: (*Server).Start(handler) = %v, want ErrServerClosed error", err)
	}
}

func TestServerErrNilHandler(t *testing.T) {
	srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{LogLevel: testLogLevel})
	err := srv.Start(nil)
	if err == nil {
		t.Error("Starting server with nil handler: (*Server).Start(nil) did not return error")
		srv.Shutdown()
	}
}

func TestServerErrServerRunning(t *testing.T) {
	srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{LogLevel: testLogLevel})
	handler := NewServeMux()
	if err := srv.Start(handler); err != nil {
		t.Fatal(err)
	}
	err := srv.Start(handler)
	if err == nil {
		t.Error("Calling (*Server).Start(handler) on already running server did not return error")
	}
	srv.Shutdown()
}

func TestServerWithRedisDown(t *testing.T) {
	// Make sure that server does not panic and exit if redis is down.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()
	r := rdb.NewRDB(setup(t))
	testBroker := testbroker.NewTestBroker(r)
	srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{LogLevel: testLogLevel})
	srv.broker = testBroker
	srv.forwarder.broker = testBroker
	srv.heartbeater.broker = testBroker
	srv.processor.broker = testBroker
	srv.subscriber.broker = testBroker
	testBroker.Sleep()

	// no-op handler
	h := func(ctx context.Context, task *Task) error {
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	srv.Shutdown()
}

func TestServerWithFlakyBroker(t *testing.T) {
	// Make sure that server does not panic and exit if redis is down.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()
	r := rdb.NewRDB(setup(t))
	testBroker := testbroker.NewTestBroker(r)
	redisConnOpt := getRedisConnOpt(t)
	srv := NewServer(redisConnOpt, Config{LogLevel: testLogLevel})
	srv.broker = testBroker
	srv.forwarder.broker = testBroker
	srv.heartbeater.broker = testBroker
	srv.processor.broker = testBroker
	srv.subscriber.broker = testBroker

	c := NewClient(redisConnOpt)

	h := func(ctx context.Context, task *Task) error {
		// force task retry.
		if task.Type() == "bad_task" {
			return fmt.Errorf("could not process %q", task.Type())
		}
		time.Sleep(2 * time.Second)
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err := c.Enqueue(NewTask("enqueued", nil), MaxRetry(i))
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Enqueue(NewTask("bad_task", nil))
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Enqueue(NewTask("scheduled", nil), ProcessIn(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}

	// simulate redis going down.
	testBroker.Sleep()

	time.Sleep(3 * time.Second)

	// simulate redis comes back online.
	testBroker.Wakeup()

	time.Sleep(3 * time.Second)

	srv.Shutdown()
}

func TestLogLevel(t *testing.T) {
	tests := []struct {
		flagVal string
		want    LogLevel
		wantStr string
	}{
		{"debug", DebugLevel, "debug"},
		{"Info", InfoLevel, "info"},
		{"WARN", WarnLevel, "warn"},
		{"warning", WarnLevel, "warn"},
		{"Error", ErrorLevel, "error"},
		{"fatal", FatalLevel, "fatal"},
	}

	for _, tc := range tests {
		level := new(LogLevel)
		if err := level.Set(tc.flagVal); err != nil {
			t.Fatal(err)
		}
		if *level != tc.want {
			t.Errorf("Set(%q): got %v, want %v", tc.flagVal, level, &tc.want)
			continue
		}
		if got := level.String(); got != tc.wantStr {
			t.Errorf("String() returned %q, want %q", got, tc.wantStr)
		}
	}
}
