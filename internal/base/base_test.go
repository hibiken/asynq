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
	"github.com/google/uuid"
)

func TestQueueKey(t *testing.T) {
	tests := []struct {
		qname string
		want  string
	}{
		{"default", "asynq:{default}"},
		{"custom", "asynq:{custom}"},
	}

	for _, tc := range tests {
		got := QueueKey(tc.qname)
		if got != tc.want {
			t.Errorf("QueueKey(%q) = %q, want %q", tc.qname, got, tc.want)
		}
	}
}

func TestInProgressKey(t *testing.T) {
	tests := []struct {
		qname string
		want  string
	}{
		{"default", "asynq:{default}:in_progress"},
		{"custom", "asynq:{custom}:in_progress"},
	}

	for _, tc := range tests {
		got := InProgressKey(tc.qname)
		if got != tc.want {
			t.Errorf("InProgressKey(%q) = %q, want %q", tc.qname, got, tc.want)
		}
	}
}

func TestDeadlinesKey(t *testing.T) {
	tests := []struct {
		qname string
		want  string
	}{
		{"default", "asynq:{default}:deadlines"},
		{"custom", "asynq:{custom}:deadlines"},
	}

	for _, tc := range tests {
		got := DeadlinesKey(tc.qname)
		if got != tc.want {
			t.Errorf("DeadlinesKey(%q) = %q, want %q", tc.qname, got, tc.want)
		}
	}
}

func TestScheduledKey(t *testing.T) {
	tests := []struct {
		qname string
		want  string
	}{
		{"default", "asynq:{default}:scheduled"},
		{"custom", "asynq:{custom}:scheduled"},
	}

	for _, tc := range tests {
		got := ScheduledKey(tc.qname)
		if got != tc.want {
			t.Errorf("ScheduledKey(%q) = %q, want %q", tc.qname, got, tc.want)
		}
	}
}

func TestRetryKey(t *testing.T) {
	tests := []struct {
		qname string
		want  string
	}{
		{"default", "asynq:{default}:retry"},
		{"custom", "asynq:{custom}:retry"},
	}

	for _, tc := range tests {
		got := RetryKey(tc.qname)
		if got != tc.want {
			t.Errorf("RetryKey(%q) = %q, want %q", tc.qname, got, tc.want)
		}
	}
}

func TestDeadKey(t *testing.T) {
	tests := []struct {
		qname string
		want  string
	}{
		{"default", "asynq:{default}:dead"},
		{"custom", "asynq:{custom}:dead"},
	}

	for _, tc := range tests {
		got := DeadKey(tc.qname)
		if got != tc.want {
			t.Errorf("DeadKey(%q) = %q, want %q", tc.qname, got, tc.want)
		}
	}
}

func TestPausedKey(t *testing.T) {
	tests := []struct {
		qname string
		want  string
	}{
		{"default", "asynq:{default}:paused"},
		{"custom", "asynq:{custom}:paused"},
	}

	for _, tc := range tests {
		got := PausedKey(tc.qname)
		if got != tc.want {
			t.Errorf("PausedKey(%q) = %q, want %q", tc.qname, got, tc.want)
		}
	}
}

func TestProcessedKey(t *testing.T) {
	tests := []struct {
		qname string
		input time.Time
		want  string
	}{
		{"default", time.Date(2019, 11, 14, 10, 30, 1, 1, time.UTC), "asynq:{default}:processed:2019-11-14"},
		{"critical", time.Date(2020, 12, 1, 1, 0, 1, 1, time.UTC), "asynq:{critical}:processed:2020-12-01"},
		{"default", time.Date(2020, 1, 6, 15, 02, 1, 1, time.UTC), "asynq:{default}:processed:2020-01-06"},
	}

	for _, tc := range tests {
		got := ProcessedKey(tc.qname, tc.input)
		if got != tc.want {
			t.Errorf("ProcessedKey(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestFailedKey(t *testing.T) {
	tests := []struct {
		qname string
		input time.Time
		want  string
	}{
		{"default", time.Date(2019, 11, 14, 10, 30, 1, 1, time.UTC), "asynq:{default}:failed:2019-11-14"},
		{"custom", time.Date(2020, 12, 1, 1, 0, 1, 1, time.UTC), "asynq:{custom}:failed:2020-12-01"},
		{"low", time.Date(2020, 1, 6, 15, 02, 1, 1, time.UTC), "asynq:{low}:failed:2020-01-06"},
	}

	for _, tc := range tests {
		got := FailedKey(tc.qname, tc.input)
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
		{"localhost", 9876, "server123", "asynq:servers:{localhost:9876:server123}"},
		{"127.0.0.1", 1234, "server987", "asynq:servers:{127.0.0.1:1234:server987}"},
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
		{"localhost", 9876, "server1", "asynq:workers:{localhost:9876:server1}"},
		{"127.0.0.1", 1234, "server2", "asynq:workers:{127.0.0.1:1234:server2}"},
	}

	for _, tc := range tests {
		got := WorkersKey(tc.hostname, tc.pid, tc.sid)
		if got != tc.want {
			t.Errorf("WorkersKey(%q, %d, %q) = %q, want = %q",
				tc.hostname, tc.pid, tc.sid, got, tc.want)
		}
	}
}

func TestUniqueKey(t *testing.T) {
	tests := []struct {
		desc     string
		qname    string
		tasktype string
		payload  map[string]interface{}
		want     string
	}{
		{
			"with primitive types",
			"default",
			"email:send",
			map[string]interface{}{"a": 123, "b": "hello", "c": true},
			"asynq:{default}:unique:email:send:a=123,b=hello,c=true",
		},
		{
			"with unsorted keys",
			"default",
			"email:send",
			map[string]interface{}{"b": "hello", "c": true, "a": 123},
			"asynq:{default}:unique:email:send:a=123,b=hello,c=true",
		},
		{
			"with composite types",
			"default",
			"email:send",
			map[string]interface{}{
				"address": map[string]string{"line": "123 Main St", "city": "Boston", "state": "MA"},
				"names":   []string{"bob", "mike", "rob"}},
			"asynq:{default}:unique:email:send:address=map[city:Boston line:123 Main St state:MA],names=[bob mike rob]",
		},
		{
			"with complex types",
			"default",
			"email:send",
			map[string]interface{}{
				"time":     time.Date(2020, time.July, 28, 0, 0, 0, 0, time.UTC),
				"duration": time.Hour},
			"asynq:{default}:unique:email:send:duration=1h0m0s,time=2020-07-28 00:00:00 +0000 UTC",
		},
		{
			"with nil payload",
			"default",
			"reindex",
			nil,
			"asynq:{default}:unique:reindex:nil",
		},
	}

	for _, tc := range tests {
		got := UniqueKey(tc.qname, tc.tasktype, tc.payload)
		if got != tc.want {
			t.Errorf("%s: UniqueKey(%q, %q, %v) = %q, want %q", tc.desc, tc.qname, tc.tasktype, tc.payload, got, tc.want)
		}
	}
}

func TestMessageEncoding(t *testing.T) {
	id := uuid.New()
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
