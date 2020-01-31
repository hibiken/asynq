// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

// heartbeater is responsible for writing process status to redis periodically to
// indicate that the background worker process is up.
type heartbeater struct {
	rdb *rdb.RDB

	mu sync.Mutex
	ps *base.ProcessStatus

	// channel to communicate back to the long running "heartbeater" goroutine.
	done chan struct{}

	// interval between heartbeats.
	interval time.Duration
}

func newHeartbeater(rdb *rdb.RDB, interval time.Duration, host string, pid int, queues map[string]uint, n int) *heartbeater {
	ps := &base.ProcessStatus{
		Concurrency: n,
		Queues:      queues,
		Host:        host,
		PID:         pid,
	}
	return &heartbeater{
		rdb:      rdb,
		ps:       ps,
		done:     make(chan struct{}),
		interval: interval,
	}
}

func (h *heartbeater) terminate() {
	logger.info("Heartbeater shutting down...")
	// Signal the heartbeater goroutine to stop.
	h.done <- struct{}{}
}

func (h *heartbeater) setState(state string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ps.State = state
}

func (h *heartbeater) start() {
	h.ps.Started = time.Now()
	h.ps.State = "running"
	go func() {
		for {
			select {
			case <-h.done:
				logger.info("Heartbeater done")
				return
			case <-time.After(h.interval):
				// Note: Set TTL to be long enough value so that it won't expire before we write again
				// and short enough to expire quickly once process is shut down.
				h.mu.Lock()
				err := h.rdb.WriteProcessStatus(h.ps, h.interval*2)
				h.mu.Unlock()
				if err != nil {
					logger.error("could not write heartbeat data: %v", err)
				}
			}
		}
	}()
}
