// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

import (
	"sync"
	"testing"
	"time"
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
			t.Errorf("ProcessInfoKey(%s, %d) = %s, want %s", tc.hostname, tc.pid, got, tc.want)
		}
	}
}

// Note: Run this test with -race flag to check for data race.
func TestProcessInfoSetter(t *testing.T) {
	pi := NewProcessInfo("localhost", 1234, 8, map[string]uint{"default": 1}, false)

	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		pi.SetState("runnning")
		wg.Done()
	}()

	go func() {
		pi.SetStarted(time.Now())
		pi.IncrActiveWorkerCount(1)
		wg.Done()
	}()

	go func() {
		pi.SetState("stopped")
		wg.Done()
	}()

	wg.Wait()
}
