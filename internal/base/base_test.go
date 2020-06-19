// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

import (
	"context"
	"encoding/json"
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

func TestServerInfoKey(t *testing.T) {
	tests := []struct {
		hostname string
		pid      int
		sid      string
		want     string
	}{
		{"localhost", 9876, "server123", "asynq:servers:localhost:9876:server123"},
		{"127.0.0.1", 1234, "server987", "asynq:servers:127.0.0.1:1234:server987"},
	}

	for _, tc := range tests {
		got := ServerInfoKey(tc.hostname, tc.pid, tc.sid)
		if got != tc.want {
			t.Errorf("ServerInfoKey(%q, %d, %q) = %q, want %q",
				tc.hostname, tc.pid, tc.sid, got, tc.want)
		}
	}
}

func TestWorkersKey(t *testing.T) {
	tests := []struct {
		hostname string
		pid      int
		sid      string
		want     string
	}{
		{"localhost", 9876, "server1", "asynq:workers:localhost:9876:server1"},
		{"127.0.0.1", 1234, "server2", "asynq:workers:127.0.0.1:1234:server2"},
	}

	for _, tc := range tests {
		got := WorkersKey(tc.hostname, tc.pid, tc.sid)
		if got != tc.want {
			t.Errorf("WorkersKey(%q, %d, %q) = %q, want = %q",
				tc.hostname, tc.pid, tc.sid, got, tc.want)
		}
	}
}

func TestMessageEncoding(t *testing.T) {
	id := xid.New()
	tests := []struct {
		in  *TaskMessage
		out *TaskMessage
	}{
		{
			in: &TaskMessage{
				Type:     "task1",
				Payload:  map[string]interface{}{"a": 1, "b": "hello!", "c": true},
				ID:       id,
				Queue:    "default",
				Retry:    10,
				Retried:  0,
				Timeout:  1800,
				Deadline: 1692311100,
			},
			out: &TaskMessage{
				Type:     "task1",
				Payload:  map[string]interface{}{"a": json.Number("1"), "b": "hello!", "c": true},
				ID:       id,
				Queue:    "default",
				Retry:    10,
				Retried:  0,
				Timeout:  1800,
				Deadline: 1692311100,
			},
		},
	}

	for _, tc := range tests {
		encoded, err := EncodeMessage(tc.in)
		if err != nil {
			t.Errorf("EncodeMessage(msg) returned error: %v", err)
			continue
		}
		decoded, err := DecodeMessage(encoded)
		if err != nil {
			t.Errorf("DecodeMessage(encoded) returned error: %v", err)
			continue
		}
		if diff := cmp.Diff(tc.out, decoded); diff != "" {
			t.Errorf("Decoded message == %+v, want %+v;(-want,+got)\n%s",
				decoded, tc.out, diff)
		}
	}
}

// Test for status being accessed by multiple goroutines.
// Run with -race flag to check for data race.
func TestStatusConcurrentAccess(t *testing.T) {
	status := NewServerStatus(StatusIdle)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		status.Get()
		status.String()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		status.Set(StatusStopped)
		status.String()
	}()

	wg.Wait()
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
}
