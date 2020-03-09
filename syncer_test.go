// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"sync"
	"testing"
	"time"

	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestSyncer(t *testing.T) {
	inProgress := []*base.TaskMessage{
		h.NewTaskMessage("send_email", nil),
		h.NewTaskMessage("reindex", nil),
		h.NewTaskMessage("gen_thumbnail", nil),
	}
	r := setup(t)
	rdbClient := rdb.NewRDB(r)
	h.SeedInProgressQueue(t, r, inProgress)

	const interval = time.Second
	syncRequestCh := make(chan *syncRequest)
	syncer := newSyncer(testLogger, syncRequestCh, interval)
	var wg sync.WaitGroup
	syncer.start(&wg)
	defer syncer.terminate()

	for _, msg := range inProgress {
		m := msg
		syncRequestCh <- &syncRequest{
			fn: func() error {
				return rdbClient.Done(m)
			},
		}
	}

	time.Sleep(2 * interval) // ensure that syncer runs at least once

	gotInProgress := h.GetInProgressMessages(t, r)
	if l := len(gotInProgress); l != 0 {
		t.Errorf("%q has length %d; want 0", base.InProgressQueue, l)
	}
}

func TestSyncerRetry(t *testing.T) {
	const interval = time.Second
	syncRequestCh := make(chan *syncRequest)
	syncer := newSyncer(testLogger, syncRequestCh, interval)

	var wg sync.WaitGroup
	syncer.start(&wg)
	defer syncer.terminate()

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
		fn:     requestFunc,
		errMsg: "error",
	}

	// allow syncer to retry
	time.Sleep(3 * interval)

	mu.Lock()
	if counter != 2 {
		t.Errorf("counter = %d, want 2", counter)
	}
	mu.Unlock()
}
