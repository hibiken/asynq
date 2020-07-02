// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
)

func TestCreateContextWithFutureDeadline(t *testing.T) {
	tests := []struct {
		deadline time.Time
	}{
		{time.Now().Add(time.Hour)},
	}

	for _, tc := range tests {
		msg := &base.TaskMessage{
			Type:    "something",
			ID:      uuid.New(),
			Payload: nil,
		}

		ctx, cancel := createContext(msg, tc.deadline)

		select {
		case x := <-ctx.Done():
			t.Errorf("<-ctx.Done() == %v, want nothing (it should block)", x)
		default:
		}

		got, ok := ctx.Deadline()
		if !ok {
			t.Errorf("ctx.Deadline() returned false, want deadline to be set")
		}
		if !cmp.Equal(tc.deadline, got) {
			t.Errorf("ctx.Deadline() returned %v, want %v", got, tc.deadline)
		}

		cancel()

		select {
		case <-ctx.Done():
		default:
			t.Errorf("ctx.Done() blocked, want it to be non-blocking")
		}
	}
}

func TestCreateContextWithPastDeadline(t *testing.T) {
	tests := []struct {
		deadline time.Time
	}{
		{time.Now().Add(-2 * time.Hour)},
	}

	for _, tc := range tests {
		msg := &base.TaskMessage{
			Type:    "something",
			ID:      uuid.New(),
			Payload: nil,
		}

		ctx, cancel := createContext(msg, tc.deadline)
		defer cancel()

		select {
		case <-ctx.Done():
		default:
			t.Errorf("ctx.Done() blocked, want it to be non-blocking")
		}

		got, ok := ctx.Deadline()
		if !ok {
			t.Errorf("ctx.Deadline() returned false, want deadline to be set")
		}
		if !cmp.Equal(tc.deadline, got) {
			t.Errorf("ctx.Deadline() returned %v, want %v", got, tc.deadline)
		}
	}
}

func TestGetTaskMetadataFromContext(t *testing.T) {
	tests := []struct {
		desc string
		msg  *base.TaskMessage
	}{
		{"with zero retried message", &base.TaskMessage{Type: "something", ID: uuid.New(), Retry: 25, Retried: 0, Timeout: 1800}},
		{"with non-zero retried message", &base.TaskMessage{Type: "something", ID: uuid.New(), Retry: 10, Retried: 5, Timeout: 1800}},
	}

	for _, tc := range tests {
		ctx, cancel := createContext(tc.msg, time.Now().Add(30*time.Minute))
		defer cancel()

		id, ok := GetTaskID(ctx)
		if !ok {
			t.Errorf("%s: GetTaskID(ctx) returned ok == false", tc.desc)
		}
		if ok && id != tc.msg.ID.String() {
			t.Errorf("%s: GetTaskID(ctx) returned id == %q, want %q", tc.desc, id, tc.msg.ID.String())
		}

		retried, ok := GetRetryCount(ctx)
		if !ok {
			t.Errorf("%s: GetRetryCount(ctx) returned ok == false", tc.desc)
		}
		if ok && retried != tc.msg.Retried {
			t.Errorf("%s: GetRetryCount(ctx) returned n == %d want %d", tc.desc, retried, tc.msg.Retried)
		}

		maxRetry, ok := GetMaxRetry(ctx)
		if !ok {
			t.Errorf("%s: GetMaxRetry(ctx) returned ok == false", tc.desc)
		}
		if ok && maxRetry != tc.msg.Retry {
			t.Errorf("%s: GetMaxRetry(ctx) returned n == %d want %d", tc.desc, maxRetry, tc.msg.Retry)
		}
	}
}

func TestGetTaskMetadataFromContextError(t *testing.T) {
	tests := []struct {
		desc string
		ctx  context.Context
	}{
		{"with background context", context.Background()},
	}

	for _, tc := range tests {
		if _, ok := GetTaskID(tc.ctx); ok {
			t.Errorf("%s: GetTaskID(ctx) returned ok == true", tc.desc)
		}
		if _, ok := GetRetryCount(tc.ctx); ok {
			t.Errorf("%s: GetRetryCount(ctx) returned ok == true", tc.desc)
		}
		if _, ok := GetMaxRetry(tc.ctx); ok {
			t.Errorf("%s: GetMaxRetry(ctx) returned ok == true", tc.desc)
		}
	}
}
