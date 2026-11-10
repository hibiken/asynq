// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/testbroker"
)

func TestHealthChecker(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	var (
		// mu guards called and e variables.
		mu     sync.Mutex
		called int
		e      error
	)
	checkFn := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		called++
		e = err
	}

	hc := newHealthChecker(healthcheckerParams{
		logger:          testLogger,
		broker:          rdbClient,
		interval:        1 * time.Second,
		healthcheckFunc: checkFn,
	})

	hc.start(&sync.WaitGroup{})

	time.Sleep(2 * time.Second)

	mu.Lock()
	if called == 0 {
		t.Errorf("Healthchecker did not call the provided HealthCheckFunc")
	}
	if e != nil {
		t.Errorf("HealthCheckFunc was called with non-nil error: %v", e)
	}
	mu.Unlock()

	hc.shutdown()
}

func TestHealthCheckerWhenRedisDown(t *testing.T) {
	// Make sure that healthchecker goroutine doesn't panic
	// if it cannot connect to redis.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()
	r := rdb.NewRDB(setup(t))
	defer r.Close()
	testBroker := testbroker.NewTestBroker(r)
	var (
		// mu guards called and e variables.
		mu     sync.Mutex
		called int
		e      error
	)
	checkFn := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		called++
		e = err
	}

	hc := newHealthChecker(healthcheckerParams{
		logger:          testLogger,
		broker:          testBroker,
		interval:        1 * time.Second,
		healthcheckFunc: checkFn,
	})

	testBroker.Sleep()
	hc.start(&sync.WaitGroup{})

	time.Sleep(2 * time.Second)

	mu.Lock()
	if called == 0 {
		t.Errorf("Healthchecker did not call the provided HealthCheckFunc")
	}
	if e == nil {
		t.Errorf("HealthCheckFunc was called with nil; want non-nil error")
	}
	mu.Unlock()

	hc.shutdown()
}

func TestHealthCheckerWhenHealthcheckFuncPanics(t *testing.T) {
	// Make sure that a panic in the user-provided HealthCheckFunc doesn't
	// crash the healthchecker goroutine (and with it, the process), and
	// that healthchecking keeps running on subsequent ticks.
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	var (
		// mu guards called.
		mu     sync.Mutex
		called int
	)
	checkFn := func(err error) {
		mu.Lock()
		called++
		mu.Unlock()
		panic("boom")
	}

	hc := newHealthChecker(healthcheckerParams{
		logger:          testLogger,
		broker:          rdbClient,
		interval:        1 * time.Second,
		healthcheckFunc: checkFn,
	})

	hc.start(&sync.WaitGroup{})

	// Wait long enough for multiple ticks; if the panic were not
	// recovered, the healthchecker goroutine would die after the first
	// tick and "called" would stop increasing (or, with -race, the test
	// binary would crash outright on an unrecovered panic).
	time.Sleep(3500 * time.Millisecond)

	mu.Lock()
	gotCalled := called
	mu.Unlock()

	if gotCalled < 2 {
		t.Errorf("HealthCheckFunc was called %d time(s) after panicking; want at least 2 (healthchecker should keep running)", gotCalled)
	}

	hc.shutdown()
}
