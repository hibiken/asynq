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
)

func TestHeartbeater(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	tests := []struct {
		interval    time.Duration
		host        string
		pid         int
		queues      map[string]int
		concurrency int
	}{
		{time.Second, "localhost", 45678, map[string]int{"default": 1}, 10},
	}

	timeCmpOpt := cmpopts.EquateApproxTime(10 * time.Millisecond)
	ignoreOpt := cmpopts.IgnoreUnexported(base.ProcessInfo{})
	for _, tc := range tests {
		h.FlushDB(t, r)

		stateCh := make(chan string)
		workerCh := make(chan int)
		hb := newHeartbeater(rdbClient, tc.host, tc.pid, tc.concurrency, tc.queues, false, tc.interval, stateCh, workerCh)

		var wg sync.WaitGroup
		hb.start(&wg)

		want := &base.ProcessInfo{
			Host:        tc.host,
			PID:         tc.pid,
			Queues:      tc.queues,
			Concurrency: tc.concurrency,
			Started:     time.Now(),
			State:       "running",
		}

		// allow for heartbeater to write to redis
		time.Sleep(tc.interval * 2)

		ps, err := rdbClient.ListProcesses()
		if err != nil {
			t.Errorf("could not read process status from redis: %v", err)
			hb.terminate()
			continue
		}

		if len(ps) != 1 {
			t.Errorf("(*RDB).ListProcesses returned %d process info, want 1", len(ps))
			hb.terminate()
			continue
		}

		if diff := cmp.Diff(want, ps[0], timeCmpOpt, ignoreOpt); diff != "" {
			t.Errorf("redis stored process status %+v, want %+v; (-want, +got)\n%s", ps[0], want, diff)
			hb.terminate()
			continue
		}

		// state change
		stateCh <- "stopped"

		// allow for heartbeater to write to redis
		time.Sleep(tc.interval * 2)

		want.State = "stopped"
		ps, err = rdbClient.ListProcesses()
		if err != nil {
			t.Errorf("could not read process status from redis: %v", err)
			hb.terminate()
			continue
		}

		if len(ps) != 1 {
			t.Errorf("(*RDB).ListProcesses returned %d process info, want 1", len(ps))
			hb.terminate()
			continue
		}

		if diff := cmp.Diff(want, ps[0], timeCmpOpt, ignoreOpt); diff != "" {
			t.Errorf("redis stored process status %+v, want %+v; (-want, +got)\n%s", ps[0], want, diff)
			hb.terminate()
			continue
		}

		hb.terminate()
	}
}
