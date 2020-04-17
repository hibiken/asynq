// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package testbroker exports a broker implementation that should be used in package testing.
package testbroker

import (
	"errors"
	"sync"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
)

var errRedisDown = errors.New("asynqtest: redis is down")

// TestBroker is a broker implementation which enables
// to simulate Redis failure in tests.
type TestBroker struct {
	mu       sync.Mutex
	sleeping bool

	*rdb.RDB
}

func NewTestBroker(r *rdb.RDB) *TestBroker {
	return &TestBroker{RDB: r}
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

func (tb *TestBroker) CancelationPubSub() (*redis.PubSub, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.sleeping {
		return nil, errRedisDown
	}
	return tb.RDB.CancelationPubSub()
}
