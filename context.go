// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

// A taskMetadata holds task scoped data to put in context.
type taskMetadata struct {
	id         string
	maxRetry   int
	retryCount int
}

// ctxKey type is unexported to prevent collisions with context keys defined in
// other packages.
type ctxKey int

// metadataCtxKey is the context key for the task metadata.
// Its value of zero is arbitrary.
const metadataCtxKey ctxKey = 0

// createContext returns a context and cancel function for a given task message.
func createContext(msg *base.TaskMessage) (ctx context.Context, cancel context.CancelFunc) {
	metadata := taskMetadata{
		id:         msg.ID.String(),
		maxRetry:   msg.Retry,
		retryCount: msg.Retried,
	}
	ctx = context.WithValue(context.Background(), metadataCtxKey, metadata)
	timeout, err := time.ParseDuration(msg.Timeout)
	if err == nil && timeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	deadline, err := time.Parse(time.RFC3339, msg.Deadline)
	if err == nil && !deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	}
	if cancel == nil {
		ctx, cancel = context.WithCancel(ctx)
	}
	return ctx, cancel
}

// GetTaskID extracts a task ID from a context, if any.
//
// ID of a task is guaranteed to be unique.
// ID of a task doesn't change if the task is being retried.
func GetTaskID(ctx context.Context) (id string, ok bool) {
	metadata, ok := ctx.Value(metadataCtxKey).(taskMetadata)
	if !ok {
		return "", false
	}
	return metadata.id, true
}

// GetRetryCount extracts retry count from a context, if any.
//
// Return value n indicates the number of times associated task has been
// retried so far.
func GetRetryCount(ctx context.Context) (n int, ok bool) {
	metadata, ok := ctx.Value(metadataCtxKey).(taskMetadata)
	if !ok {
		return 0, false
	}
	return metadata.retryCount, true
}

// GetMaxRetry extracts maximum retry from a context, if any.
//
// Return value n indicates the maximum number of times the assoicated task
// can be retried if ProcessTask returns a non-nil error.
func GetMaxRetry(ctx context.Context) (n int, ok bool) {
	metadata, ok := ctx.Value(metadataCtxKey).(taskMetadata)
	if !ok {
		return 0, false
	}
	return metadata.maxRetry, true
}
