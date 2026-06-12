// Copyright 2024 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/rdb"
)

func TestHealthChecker_PanicRecovery(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	var mu sync.Mutex
	called := 0

	panicFn := func(err error) {
		mu.Lock()
		called++
		mu.Unlock()
		panic("deliberate panic in HealthCheckFunc")
	}

	hc := newHealthChecker(healthcheckerParams{
		logger:          testLogger,
		broker:          rdbClient,
		interval:        100 * time.Millisecond,
		healthcheckFunc: panicFn,
	})

	var wg sync.WaitGroup
	hc.start(&wg)

	time.Sleep(350 * time.Millisecond)

	mu.Lock()
	c := called
	mu.Unlock()

	// With per-call recovery, the goroutine survives each panic and
	// the healthcheck fires multiple times.
	if c < 2 {
		t.Errorf("HealthCheckFunc called %d times, want >= 2 (goroutine should survive panics)", c)
	}

	hc.shutdown()
	wg.Wait()
}

func TestHealthChecker_PanicRecovery_NilError(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	buggyFn := func(err error) {
		_ = err.Error() // panics when err is nil (healthy ping)
	}

	hc := newHealthChecker(healthcheckerParams{
		logger:          testLogger,
		broker:          rdbClient,
		interval:        100 * time.Millisecond,
		healthcheckFunc: buggyFn,
	})

	var wg sync.WaitGroup
	hc.start(&wg)

	time.Sleep(300 * time.Millisecond)

	hc.shutdown()
	wg.Wait()
}

func TestAggregator_PanicRecovery(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	panicAggregator := GroupAggregatorFunc(func(group string, tasks []*Task) *Task {
		panic("deliberate panic in GroupAggregator")
	})

	agg := newAggregator(aggregatorParams{
		logger:          testLogger,
		broker:          rdbClient,
		queues:          []string{"default"},
		groupAggregator: panicAggregator,
		gracePeriod:     1 * time.Minute,
		maxDelay:        10 * time.Minute,
		maxSize:         10,
	})

	var wg sync.WaitGroup
	agg.start(&wg)

	time.Sleep(200 * time.Millisecond)

	agg.shutdown()
	wg.Wait()
}
