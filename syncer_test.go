// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

func TestSyncer(t *testing.T) {
	inProgress := []*base.TaskMessage{
		h.NewTaskMessage("send_email", nil),
		h.NewTaskMessage("reindex", nil),
		h.NewTaskMessage("gen_thumbnail", nil),
	}
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	h.SeedActiveQueue(t, r, inProgress, base.DefaultQueueName)

	const interval = time.Second
	syncRequestCh := make(chan *syncRequest)
	syncer := newSyncer(syncerParams{
		logger:     testLogger,
		requestsCh: syncRequestCh,
		interval:   interval,
	})
	var wg sync.WaitGroup
	syncer.start(&wg)
	defer syncer.shutdown()

	for _, msg := range inProgress {
		m := msg
		syncRequestCh <- &syncRequest{
			fn: func() error {
				return rdbClient.Done(context.Background(), m)
			},
			deadline: time.Now().Add(5 * time.Minute),
		}
	}

	time.Sleep(2 * interval) // ensure that syncer runs at least once

	gotActive := h.GetActiveMessages(t, r, base.DefaultQueueName)
	if l := len(gotActive); l != 0 {
		t.Errorf("%q has length %d; want 0", base.ActiveKey(base.DefaultQueueName), l)
	}
}

func TestSyncerRetry(t *testing.T) {
	const interval = time.Second
	syncRequestCh := make(chan *syncRequest)
	syncer := newSyncer(syncerParams{
		logger:     testLogger,
		requestsCh: syncRequestCh,
		interval:   interval,
	})

	var wg sync.WaitGroup
	syncer.start(&wg)
	defer syncer.shutdown()

	var (
		mu      sync.Mutex
		counter int
	)

	// Increment the counter for each call.
	// Initial call will fail and second call will succeed.
	requestFunc := func() error {
		mu.Lock()
		defer mu.Unlock()
		if counter == 0 {
			counter++
			return fmt.Errorf("zero")
		}
		counter++
		return nil
	}

	syncRequestCh <- &syncRequest{
		fn:       requestFunc,
		errMsg:   "error",
		deadline: time.Now().Add(5 * time.Minute),
	}

	// allow syncer to retry
	time.Sleep(3 * interval)

	mu.Lock()
	if counter != 2 {
		t.Errorf("counter = %d, want 2", counter)
	}
	mu.Unlock()
}

func TestSyncerDropsStaleRequests(t *testing.T) {
	const interval = time.Second
	syncRequestCh := make(chan *syncRequest)
	syncer := newSyncer(syncerParams{
		logger:     testLogger,
		requestsCh: syncRequestCh,
		interval:   interval,
	})
	var wg sync.WaitGroup
	syncer.start(&wg)

	var (
		mu sync.Mutex
		n  int // number of times request has been processed
	)

	for i := range 10 {
		syncRequestCh <- &syncRequest{
			fn: func() error {
				mu.Lock()
				n++
				mu.Unlock()
				return nil
			},
			deadline: time.Now().Add(time.Duration(-i) * time.Second), // already exceeded deadline
		}
	}

	time.Sleep(2 * interval) // ensure that syncer runs at least once
	syncer.shutdown()

	mu.Lock()
	if n != 0 {
		t.Errorf("requests has been processed %d times, want 0", n)
	}
	mu.Unlock()
}

// Regression: retries must keep happening on schedule even while new sync
// requests keep arriving more often than the interval. Re-creating the timer
// on every loop iteration starves the periodic branch.
func TestSyncerRetriesWhileRequestsKeepArriving(t *testing.T) {
	const interval = 300 * time.Millisecond
	ch := make(chan *syncRequest)
	s := newSyncer(syncerParams{logger: testLogger, requestsCh: ch, interval: interval})
	var wg sync.WaitGroup
	s.start(&wg)
	defer func() { s.shutdown(); wg.Wait() }()

	var mu sync.Mutex
	attempts := 0
	ch <- &syncRequest{
		fn: func() error {
			mu.Lock()
			defer mu.Unlock()
			attempts++
			if attempts == 1 {
				return fmt.Errorf("first attempt fails")
			}
			return nil
		},
		deadline: time.Now().Add(time.Minute),
	}

	// A steady trickle of unrelated requests, faster than the interval.
	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				select {
				case ch <- &syncRequest{fn: func() error { return nil }, deadline: time.Now().Add(time.Minute)}:
				case <-stop:
					return
				}
			}
		}
	}()
	defer close(stop)

	deadline := time.After(5 * interval)
	for {
		mu.Lock()
		n := attempts
		mu.Unlock()
		if n >= 2 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("failed request was not retried within %v while new requests kept arriving (attempts=%d)", 5*interval, n)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Regression: the final flush on shutdown must not run requests whose deadline
// has passed (they would act on tasks that were already recovered elsewhere).
func TestSyncerShutdownSkipsStaleRequests(t *testing.T) {
	ch := make(chan *syncRequest)
	s := newSyncer(syncerParams{logger: testLogger, requestsCh: ch, interval: time.Hour})
	var wg sync.WaitGroup
	s.start(&wg)

	var mu sync.Mutex
	var ran []string
	mk := func(name string, deadline time.Time) *syncRequest {
		return &syncRequest{
			fn: func() error {
				mu.Lock()
				defer mu.Unlock()
				ran = append(ran, name)
				return nil
			},
			deadline: deadline,
		}
	}
	ch <- mk("stale", time.Now().Add(-time.Second))
	ch <- mk("fresh", time.Now().Add(time.Minute))
	s.shutdown()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 1 || ran[0] != "fresh" {
		t.Errorf("shutdown flush ran %v, want only [fresh]", ran)
	}
}
