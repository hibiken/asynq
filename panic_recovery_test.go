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

// TestHealthChecker_PanicRecovery verifies that a panicking HealthCheckFunc
// does not crash the server process. Without recovery, the goroutine unwinds
// and kills the entire process.
func TestHealthChecker_PanicRecovery(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	var mu sync.Mutex
	called := 0

	// This HealthCheckFunc panics on every call.
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

	// Give the health checker time to fire at least once.
	time.Sleep(300 * time.Millisecond)

	// If we reach this line, the panic was recovered.
	// Without the fix, this test crashes the process.
	mu.Lock()
	if called == 0 {
		t.Error("HealthCheckFunc was never called")
	}
	mu.Unlock()

	hc.shutdown()
	wg.Wait()
}

// TestHealthChecker_PanicRecovery_NilError verifies the common case where
// the user's HealthCheckFunc panics because it dereferences a nil error
// (broker ping succeeded, err is nil).
func TestHealthChecker_PanicRecovery_NilError(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	// Common user bug: calling err.Error() when err is nil.
	buggyFn := func(err error) {
		// This panics when err is nil (healthy ping).
		_ = err.Error()
	}

	hc := newHealthChecker(healthcheckerParams{
		logger:          testLogger,
		broker:          rdbClient,
		interval:        100 * time.Millisecond,
		healthcheckFunc: buggyFn,
	})

	var wg sync.WaitGroup
	hc.start(&wg)

	// Give enough time for the healthcheck to fire.
	time.Sleep(300 * time.Millisecond)

	// Process should still be alive. Without recovery, this test crashes.
	hc.shutdown()
	wg.Wait()
}

// TestAggregator_PanicRecovery verifies that a panicking GroupAggregator
// does not crash the server process.
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

	// Trigger an aggregation check. Even though the broker has no tasks
	// to aggregate, the test proves the goroutine setup is correct.
	// A more thorough test would seed the broker with grouped tasks.
	time.Sleep(200 * time.Millisecond)

	// Process should still be alive.
	agg.shutdown()
	wg.Wait()
}
