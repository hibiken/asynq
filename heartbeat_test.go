// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
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
		queues      map[string]uint
		concurrency int
	}{
		{time.Second, "some.address.ec2.aws.com", 45678, map[string]uint{"default": 1}, 10},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)

		hb := newHeartbeater(rdbClient, tc.interval, tc.host, tc.pid, tc.queues, tc.concurrency)

		want := &base.ProcessStatus{
			Host:        tc.host,
			PID:         tc.pid,
			Queues:      tc.queues,
			Concurrency: tc.concurrency,
			Started:     time.Now(),
			State:       "running",
		}
		hb.start()

		// allow for heartbeater to write to redis
		time.Sleep(tc.interval * 2)

		got, err := rdbClient.ReadProcessStatus(tc.host, tc.pid)
		if err != nil {
			t.Errorf("could not read process status from redis: %v", err)
			hb.terminate()
			continue
		}

		var timeCmpOpt = cmpopts.EquateApproxTime(10 * time.Millisecond)
		if diff := cmp.Diff(want, got, timeCmpOpt); diff != "" {
			t.Errorf("redis stored process status %+v, want %+v; (-want, +got)\n%s", got, want, diff)
			hb.terminate()
			continue
		}

		// state change
		hb.setState("stopped")

		// allow for heartbeater to write to redis
		time.Sleep(tc.interval * 2)

		want.State = "stopped"
		got, err = rdbClient.ReadProcessStatus(tc.host, tc.pid)
		if err != nil {
			t.Errorf("could not read process status from redis: %v", err)
			hb.terminate()
			continue
		}

		if diff := cmp.Diff(want, got, timeCmpOpt); diff != "" {
			t.Errorf("redis stored process status %+v, want %+v; (-want, +got)\n%s", got, want, diff)
			hb.terminate()
			continue
		}

		hb.terminate()
	}
}
