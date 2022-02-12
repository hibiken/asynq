// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/testbroker"
	"github.com/hibiken/asynq/internal/timeutil"
)

// Test goes through a few phases.
//
// Phase1: Simulate Server startup; Simulate starting tasks listed in startedTasks
// Phase2: Simluate finishing tasks listed in finishedTasks
// Phase3: Simulate Server shutdown;
func TestHeartbeater(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	now := time.Now()
	const elapsedTime = 42 * time.Second // simulated time elapsed between phase1 and phase2

	t1 := h.NewTaskMessageWithQueue("task1", nil, "default")
	t2 := h.NewTaskMessageWithQueue("task2", nil, "default")
	t3 := h.NewTaskMessageWithQueue("task3", nil, "default")

	tests := []struct {
		// Interval between heartbeats.
		interval time.Duration

		// Server info.
		host        string
		pid         int
		queues      map[string]int
		concurrency int

		active        map[string][]*base.TaskMessage // initial active set state
		lease         map[string][]base.Z            // initial lease set state
		wantLease1    map[string][]base.Z            // expected lease set state after starting all startedTasks
		wantLease2    map[string][]base.Z            // expected lease set state after finishing all finishedTasks
		startedTasks  []*base.TaskMessage            // tasks to send via the started channel
		finishedTasks []*base.TaskMessage            // tasks to send via the finished channel

		startTime   time.Time     // simulated start time
		elapsedTime time.Duration // simulated time elapsed between starting and finishing processing tasks
	}{
		{
			interval:    2 * time.Second,
			host:        "localhost",
			pid:         45678,
			queues:      map[string]int{"default": 1}, // TODO: Test with multple queues
			concurrency: 10,
			active: map[string][]*base.TaskMessage{
				"default": {t1, t2, t3},
			},
			lease: map[string][]base.Z{
				"default": {
					{Message: t1, Score: now.Add(10 * time.Second).Unix()},
					{Message: t2, Score: now.Add(10 * time.Second).Unix()},
					{Message: t3, Score: now.Add(10 * time.Second).Unix()},
				},
			},
			startedTasks:  []*base.TaskMessage{t1, t2, t3},
			finishedTasks: []*base.TaskMessage{t1, t2},
			wantLease1: map[string][]base.Z{
				"default": {
					{Message: t1, Score: now.Add(rdb.LeaseDuration).Unix()},
					{Message: t2, Score: now.Add(rdb.LeaseDuration).Unix()},
					{Message: t3, Score: now.Add(rdb.LeaseDuration).Unix()},
				},
			},
			wantLease2: map[string][]base.Z{
				"default": {
					{Message: t3, Score: now.Add(elapsedTime).Add(rdb.LeaseDuration).Unix()},
				},
			},
			startTime:   now,
			elapsedTime: elapsedTime,
		},
	}

	timeCmpOpt := cmpopts.EquateApproxTime(10 * time.Millisecond)
	ignoreOpt := cmpopts.IgnoreUnexported(base.ServerInfo{})
	ignoreFieldOpt := cmpopts.IgnoreFields(base.ServerInfo{}, "ServerID")
	for _, tc := range tests {
		h.FlushDB(t, r)
		h.SeedAllActiveQueues(t, r, tc.active)
		h.SeedAllLease(t, r, tc.lease)

		clock := timeutil.NewSimulatedClock(tc.startTime)
		rdbClient.SetClock(clock)

		srvState := &serverState{}
		startingCh := make(chan *workerInfo)
		finishedCh := make(chan *base.TaskMessage)
		hb := newHeartbeater(heartbeaterParams{
			logger:         testLogger,
			broker:         rdbClient,
			interval:       tc.interval,
			concurrency:    tc.concurrency,
			queues:         tc.queues,
			strictPriority: false,
			state:          srvState,
			starting:       startingCh,
			finished:       finishedCh,
		})

		// Change host and pid fields for testing purpose.
		hb.host = tc.host
		hb.pid = tc.pid

		//===================
		// Start Phase1
		//===================

		srvState.mu.Lock()
		srvState.value = srvStateActive // simulating Server.Start
		srvState.mu.Unlock()

		var wg sync.WaitGroup
		hb.start(&wg)

		// Simulate processor starting to work on tasks.
		for _, msg := range tc.startedTasks {
			startingCh <- &workerInfo{
				msg:      msg,
				started:  now,
				deadline: now.Add(30 * time.Minute),
			}
		}

		// Wait for heartbeater to write to redis
		time.Sleep(tc.interval * 2)

		ss, err := rdbClient.ListServers()
		if err != nil {
			t.Errorf("could not read server info from redis: %v", err)
			hb.shutdown()
			continue
		}

		if len(ss) != 1 {
			t.Errorf("(*RDB).ListServers returned %d server info, want 1", len(ss))
			hb.shutdown()
			continue
		}

		wantInfo := &base.ServerInfo{
			Host:              tc.host,
			PID:               tc.pid,
			Queues:            tc.queues,
			Concurrency:       tc.concurrency,
			Started:           now,
			Status:            "active",
			ActiveWorkerCount: len(tc.startedTasks),
		}
		if diff := cmp.Diff(wantInfo, ss[0], timeCmpOpt, ignoreOpt, ignoreFieldOpt); diff != "" {
			t.Errorf("redis stored server status %+v, want %+v; (-want, +got)\n%s", ss[0], wantInfo, diff)
			hb.shutdown()
			continue
		}

		for qname, wantLease := range tc.wantLease1 {
			gotLease := h.GetLeaseEntries(t, r, qname)
			if diff := cmp.Diff(wantLease, gotLease, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.LeaseKey(qname), diff)
			}
		}

		//===================
		// Start Phase2
		//===================

		clock.AdvanceTime(tc.elapsedTime)
		// Simulate processor finished processing tasks.
		for _, msg := range tc.finishedTasks {
			if err := rdbClient.Done(msg); err != nil {
				t.Fatalf("RDB.Done failed: %v", err)
			}
			finishedCh <- msg
		}
		// Wait for heartbeater to write to redis
		time.Sleep(tc.interval * 2)

		for qname, wantLease := range tc.wantLease2 {
			gotLease := h.GetLeaseEntries(t, r, qname)
			if diff := cmp.Diff(wantLease, gotLease, h.SortZSetEntryOpt); diff != "" {
				t.Errorf("mismatch found in %q: (-want,+got):\n%s", base.LeaseKey(qname), diff)
			}
		}

		//===================
		// Start Phase3
		//===================

		// Server state change; simulating Server.Shutdown
		srvState.mu.Lock()
		srvState.value = srvStateClosed
		srvState.mu.Unlock()

		// Wait for heartbeater to write to redis
		time.Sleep(tc.interval * 2)

		wantInfo = &base.ServerInfo{
			Host:              tc.host,
			PID:               tc.pid,
			Queues:            tc.queues,
			Concurrency:       tc.concurrency,
			Started:           now,
			Status:            "closed",
			ActiveWorkerCount: len(tc.startedTasks) - len(tc.finishedTasks),
		}
		ss, err = rdbClient.ListServers()
		if err != nil {
			t.Errorf("could not read server status from redis: %v", err)
			hb.shutdown()
			continue
		}

		if len(ss) != 1 {
			t.Errorf("(*RDB).ListServers returned %d server info, want 1", len(ss))
			hb.shutdown()
			continue
		}

		if diff := cmp.Diff(wantInfo, ss[0], timeCmpOpt, ignoreOpt, ignoreFieldOpt); diff != "" {
			t.Errorf("redis stored process status %+v, want %+v; (-want, +got)\n%s", ss[0], wantInfo, diff)
			hb.shutdown()
			continue
		}

		hb.shutdown()
	}
}

func TestHeartbeaterWithRedisDown(t *testing.T) {
	// Make sure that heartbeater goroutine doesn't panic
	// if it cannot connect to redis.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic occurred: %v", r)
		}
	}()
	r := rdb.NewRDB(setup(t))
	defer r.Close()
	testBroker := testbroker.NewTestBroker(r)
	state := &serverState{value: srvStateActive}
	hb := newHeartbeater(heartbeaterParams{
		logger:         testLogger,
		broker:         testBroker,
		interval:       time.Second,
		concurrency:    10,
		queues:         map[string]int{"default": 1},
		strictPriority: false,
		state:          state,
		starting:       make(chan *workerInfo),
		finished:       make(chan *base.TaskMessage),
	})

	testBroker.Sleep()
	var wg sync.WaitGroup
	hb.start(&wg)

	// wait for heartbeater to try writing data to redis
	time.Sleep(2 * time.Second)

	hb.shutdown()
}
