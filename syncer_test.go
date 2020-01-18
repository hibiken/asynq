// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
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
	syncer := newSyncer(syncRequestCh, interval)
	syncer.start()
	defer syncer.terminate()

	for _, msg := range inProgress {
		m := msg
		syncRequestCh <- &syncRequest{
			fn: func() error {
				return rdbClient.Done(m)
			},
		}
	}

	time.Sleep(interval) // ensure that syncer runs at least once

	gotInProgress := h.GetInProgressMessages(t, r)
	if l := len(gotInProgress); l != 0 {
		t.Errorf("%q has length %d; want 0", base.InProgressQueue, l)
	}
}

func TestSyncerRetry(t *testing.T) {
	inProgress := []*base.TaskMessage{
		h.NewTaskMessage("send_email", nil),
		h.NewTaskMessage("reindex", nil),
		h.NewTaskMessage("gen_thumbnail", nil),
	}
	goodClient := setup(t)
	h.SeedInProgressQueue(t, goodClient, inProgress)

	// Simulate the situation where redis server is down
	// by connecting to a wrong port.
	badClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6390",
	})
	rdbClient := rdb.NewRDB(badClient)

	const interval = time.Second
	syncRequestCh := make(chan *syncRequest)
	syncer := newSyncer(syncRequestCh, interval)
	syncer.start()
	defer syncer.terminate()

	for _, msg := range inProgress {
		m := msg
		syncRequestCh <- &syncRequest{
			fn: func() error {
				return rdbClient.Done(m)
			},
		}
	}

	time.Sleep(interval) // ensure that syncer runs at least once

	// Sanity check to ensure that message was not successfully deleted
	// from in-progress list.
	gotInProgress := h.GetInProgressMessages(t, goodClient)
	if l := len(gotInProgress); l != len(inProgress) {
		t.Errorf("%q has length %d; want %d", base.InProgressQueue, l, len(inProgress))
	}

	// simualate failover.
	rdbClient = rdb.NewRDB(goodClient)

	time.Sleep(interval) // ensure that syncer runs at least once

	gotInProgress = h.GetInProgressMessages(t, goodClient)
	if l := len(gotInProgress); l != 0 {
		t.Errorf("%q has length %d; want 0", base.InProgressQueue, l)
	}
}
