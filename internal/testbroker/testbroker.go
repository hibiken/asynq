// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package testbroker exports a broker implementation that should be used in package testing.
package testbroker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq/internal/base"
)

var errRedisDown = errors.New("asynqtest: redis is down")

// TestBroker is a broker implementation which enables
// to simulate Redis failure in tests.
type TestBroker struct {
	mu       sync.Mutex
	sleeping bool

	// real broker
	real base.Broker
}

// Make sure TestBroker implements Broker interface at compile time.
var _ base.Broker = (*TestBroker)(nil)

func NewTestBroker(b base.Broker) *TestBroker {
	return &TestBroker{real: b}
}

func (tb *TestBroker) Sleep() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.sleeping = true
}

func (tb *TestBroker) Wakeup() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.sleeping = false
}

func (tb *TestBroker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Enqueue(ctx, msg)
}

func (tb *TestBroker) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.EnqueueUnique(ctx, msg, ttl)
}

func (tb *TestBroker) Dequeue(ctx context.Context, qnames ...string) (*base.TaskMessage, time.Time, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, time.Time{}, errRedisDown
	}
	return tb.real.Dequeue(ctx, qnames...)
}

func (tb *TestBroker) Done(ctx context.Context, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Done(ctx, msg)
}

func (tb *TestBroker) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Requeue(ctx, msg)
}

func (tb *TestBroker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Schedule(ctx, msg, processAt)
}

func (tb *TestBroker) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ScheduleUnique(ctx, msg, processAt, ttl)
}

func (tb *TestBroker) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Retry(ctx, msg, processAt, errMsg)
}

func (tb *TestBroker) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Archive(ctx, msg, errMsg)
}

func (tb *TestBroker) ForwardIfReady(ctx context.Context, qnames ...string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ForwardIfReady(ctx, qnames...)
}

func (tb *TestBroker) ListDeadlineExceeded(ctx context.Context, deadline time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.ListDeadlineExceeded(ctx, deadline, qnames...)
}

func (tb *TestBroker) WriteServerState(ctx context.Context, info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.WriteServerState(ctx, info, workers, ttl)
}

func (tb *TestBroker) ClearServerState(ctx context.Context, host string, pid int, serverID string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ClearServerState(ctx, host, pid, serverID)
}

func (tb *TestBroker) CancelationPubSub(ctx context.Context) (*redis.PubSub, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.CancelationPubSub(ctx)
}

func (tb *TestBroker) PublishCancelation(ctx context.Context, id string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.PublishCancelation(ctx, id)
}

func (tb *TestBroker) Ping(ctx context.Context) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Ping(ctx)
}

func (tb *TestBroker) Close() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Close()
}
