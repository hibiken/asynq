// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestServer(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/go-redis/redis/v7/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNoLeaks(t, ignoreOpt)

	r := &RedisClientOpt{
		Addr: "localhost:6379",
		DB:   15,
	}
	c := NewClient(r)
	srv := NewServer(r, Config{
		Concurrency: 10,
	})

	// no-op handler
	h := func(ctx context.Context, task *Task) error {
		return nil
	}

	err := srv.Start(HandlerFunc(h))
	if err != nil {
		t.Fatal(err)
	}

	err = c.Enqueue(NewTask("send_email", map[string]interface{}{"recipient_id": 123}))
	if err != nil {
		t.Errorf("could not enqueue a task: %v", err)
	}

	err = c.EnqueueAt(time.Now().Add(time.Hour), NewTask("send_email", map[string]interface{}{"recipient_id": 456}))
	if err != nil {
		t.Errorf("could not enqueue a task: %v", err)
	}

	srv.Stop()
}

func TestServerRun(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/go-redis/redis/v7/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNoLeaks(t, ignoreOpt)

	srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{})

	done := make(chan struct{})
	// Make sure server exits when receiving TERM signal.
	go func() {
		time.Sleep(2 * time.Second)
		syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
		done <- struct{}{}
	}()

	go func() {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("server did not stop after receiving TERM signal")
		case <-done:
		}
	}()

	mux := NewServeMux()
	if err := srv.Run(mux); err != nil {
		t.Fatal(err)
	}
}

func TestServerErrServerStopped(t *testing.T) {
	srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{})
	handler := NewServeMux()
	if err := srv.Start(handler); err != nil {
		t.Fatal(err)
	}
	srv.Stop()
	err := srv.Start(handler)
	if err != ErrServerStopped {
		t.Errorf("Restarting server: (*Server).Start(handler) = %v, want ErrServerStopped error", err)
	}
}

func TestServerErrNilHandler(t *testing.T) {
	srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{})
	err := srv.Start(nil)
	if err == nil {
		t.Error("Starting server with nil handler: (*Server).Start(nil) did not return error")
		srv.Stop()
	}
}

func TestServerErrServerRunning(t *testing.T) {
	srv := NewServer(RedisClientOpt{Addr: ":6379"}, Config{})
	handler := NewServeMux()
	if err := srv.Start(handler); err != nil {
		t.Fatal(err)
	}
	err := srv.Start(handler)
	if err == nil {
		t.Error("Calling (*Server).Start(handler) on already running server did not return error")
	}
	srv.Stop()
}
