// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/rs/xid"
)

func TestQueueKey(t *testing.T) {
	tests := []struct {
		qname string
		want  string
	}{
		{"custom", "asynq:queues:custom"},
	}

	for _, tc := range tests {
		got := QueueKey(tc.qname)
		if got != tc.want {
			t.Errorf("QueueKey(%q) = %q, want %q", tc.qname, got, tc.want)
		}
	}
}

func TestProcessedKey(t *testing.T) {
	tests := []struct {
		input time.Time
		want  string
	}{
		{time.Date(2019, 11, 14, 10, 30, 1, 1, time.UTC), "asynq:processed:2019-11-14"},
		{time.Date(2020, 12, 1, 1, 0, 1, 1, time.UTC), "asynq:processed:2020-12-01"},
		{time.Date(2020, 1, 6, 15, 02, 1, 1, time.UTC), "asynq:processed:2020-01-06"},
	}

	for _, tc := range tests {
		got := ProcessedKey(tc.input)
		if got != tc.want {
			t.Errorf("ProcessedKey(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestFailureKey(t *testing.T) {
	tests := []struct {
		input time.Time
		want  string
	}{
		{time.Date(2019, 11, 14, 10, 30, 1, 1, time.UTC), "asynq:failure:2019-11-14"},
		{time.Date(2020, 12, 1, 1, 0, 1, 1, time.UTC), "asynq:failure:2020-12-01"},
		{time.Date(2020, 1, 6, 15, 02, 1, 1, time.UTC), "asynq:failure:2020-01-06"},
	}

	for _, tc := range tests {
		got := FailureKey(tc.input)
		if got != tc.want {
			t.Errorf("FailureKey(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestProcessInfoKey(t *testing.T) {
	tests := []struct {
		hostname string
		pid      int
		want     string
	}{
		{"localhost", 9876, "asynq:ps:localhost:9876"},
		{"127.0.0.1", 1234, "asynq:ps:127.0.0.1:1234"},
	}

	for _, tc := range tests {
		got := ProcessInfoKey(tc.hostname, tc.pid)
		if got != tc.want {
			t.Errorf("ProcessInfoKey(%q, %d) = %q, want %q", tc.hostname, tc.pid, got, tc.want)
		}
	}
}

func TestWorkersKey(t *testing.T) {
	tests := []struct {
		hostname string
		pid      int
		want     string
	}{
		{"localhost", 9876, "asynq:workers:localhost:9876"},
		{"127.0.0.1", 1234, "asynq:workers:127.0.0.1:1234"},
	}

	for _, tc := range tests {
		got := WorkersKey(tc.hostname, tc.pid)
		if got != tc.want {
			t.Errorf("WorkersKey(%q, %d) = %q, want = %q", tc.hostname, tc.pid, got, tc.want)
		}
	}
}

// Test for process state being accessed by multiple goroutines.
// Run with -race flag to check for data race.
func TestProcessStateConcurrentAccess(t *testing.T) {
	ps := NewProcessState("127.0.0.1", 1234, 10, map[string]int{"default": 1}, false)
	var wg sync.WaitGroup
	started := time.Now()
	msgs := []*TaskMessage{
		&TaskMessage{ID: xid.New(), Type: "type1", Payload: map[string]interface{}{"user_id": 42}},
		&TaskMessage{ID: xid.New(), Type: "type2"},
		&TaskMessage{ID: xid.New(), Type: "type3"},
	}

	// Simulate hearbeater calling SetStatus and SetStarted.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ps.SetStarted(started)
		ps.SetStatus(StatusRunning)
	}()

	// Simulate processor starting worker goroutines.
	for _, msg := range msgs {
		wg.Add(1)
		ps.AddWorkerStats(msg, time.Now())
		go func(msg *TaskMessage) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
			ps.DeleteWorkerStats(msg)
		}(msg)
	}

	// Simulate hearbeater calling Get and GetWorkers
	wg.Add(1)
	go func() {
		wg.Done()
		for i := 0; i < 5; i++ {
			ps.Get()
			ps.GetWorkers()
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}
	}()

	wg.Wait()

	want := &ProcessInfo{
		Host:              "127.0.0.1",
		PID:               1234,
		Concurrency:       10,
		Queues:            map[string]int{"default": 1},
		StrictPriority:    false,
		Status:            "running",
		Started:           started,
		ActiveWorkerCount: 0,
	}

	got := ps.Get()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("(*ProcessState).Get() = %+v, want %+v; (-want,+got)\n%s",
			got, want, diff)
	}
}

// Test for cancelations being accessed by multiple goroutines.
// Run with -race flag to check for data race.
func TestCancelationsConcurrentAccess(t *testing.T) {
	c := NewCancelations()

	_, cancel1 := context.WithCancel(context.Background())
	_, cancel2 := context.WithCancel(context.Background())
	_, cancel3 := context.WithCancel(context.Background())
	var key1, key2, key3 = "key1", "key2", "key3"

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Add(key1, cancel1)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Add(key2, cancel2)
		time.Sleep(200 * time.Millisecond)
		c.Delete(key2)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Add(key3, cancel3)
	}()

	wg.Wait()

	_, ok := c.Get(key1)
	if !ok {
		t.Errorf("(*Cancelations).Get(%q) = _, false, want <function>, true", key1)
	}

	_, ok = c.Get(key2)
	if ok {
		t.Errorf("(*Cancelations).Get(%q) = _, true, want <nil>, false", key2)
	}

	funcs := c.GetAll()
	if len(funcs) != 2 {
		t.Errorf("(*Cancelations).GetAll() returns %d functions, want 2", len(funcs))
	}
}
