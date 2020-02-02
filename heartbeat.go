// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

// heartbeater is responsible for writing process info to redis periodically to
// indicate that the background worker process is up.
type heartbeater struct {
	rdb *rdb.RDB

	pinfo *base.ProcessInfo

	// channel to communicate back to the long running "heartbeater" goroutine.
	done chan struct{}

	// interval between heartbeats.
	interval time.Duration
}

func newHeartbeater(rdb *rdb.RDB, pinfo *base.ProcessInfo, interval time.Duration) *heartbeater {
	return &heartbeater{
		rdb:      rdb,
		pinfo:    pinfo,
		done:     make(chan struct{}),
		interval: interval,
	}
}

func (h *heartbeater) terminate() {
	logger.info("Heartbeater shutting down...")
	// Signal the heartbeater goroutine to stop.
	h.done <- struct{}{}
}

func (h *heartbeater) start() {
	h.pinfo.SetStarted(time.Now())
	h.pinfo.SetState("running")
	go func() {
		h.beat()
		for {
			select {
			case <-h.done:
				logger.info("Heartbeater done")
				return
			case <-time.After(h.interval):
				h.beat()
			}
		}
	}()
}

func (h *heartbeater) beat() {
	// Note: Set TTL to be long enough so that it won't expire before we write again
	// and short enough to expire quickly once the process is shut down or killed.
	err := h.rdb.WriteProcessInfo(h.pinfo, h.interval*2)
	if err != nil {
		logger.error("could not write heartbeat data: %v", err)
	}
}
