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

func (tb *TestBroker) Dequeue(qnames ...string) (*base.TaskMessage, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.Dequeue(qnames...)
}

func (tb *TestBroker) Done(msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Done(msg)
}

func (tb *TestBroker) MarkAsComplete(msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.MarkAsComplete(msg)
}

func (tb *TestBroker) Requeue(msg *base.TaskMessage) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Requeue(msg)
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

func (tb *TestBroker) Retry(msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Retry(msg, processAt, errMsg, isFailure)
}

func (tb *TestBroker) Archive(msg *base.TaskMessage, errMsg string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Archive(msg, errMsg)
}

func (tb *TestBroker) ForwardIfReady(qnames ...string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ForwardIfReady(qnames...)
}

func (tb *TestBroker) DeleteExpiredCompletedTasks(qname string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.DeleteExpiredCompletedTasks(qname)
}

func (tb *TestBroker) ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.ListLeaseExpired(cutoff, qnames...)
}

func (tb *TestBroker) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.WriteServerState(info, workers, ttl)
}

func (tb *TestBroker) ClearServerState(host string, pid int, serverID string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.ClearServerState(host, pid, serverID)
}

func (tb *TestBroker) CancelationPubSub() (*redis.PubSub, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.real.CancelationPubSub()
}

func (tb *TestBroker) PublishCancelation(id string) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.PublishCancelation(id)
}

func (tb *TestBroker) WriteResult(qname, id string, data []byte) (int, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return 0, errRedisDown
	}
	return tb.real.WriteResult(qname, id, data)
}

func (tb *TestBroker) Ping() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Ping()
}

func (tb *TestBroker) Close() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return errRedisDown
	}
	return tb.real.Close()
}
