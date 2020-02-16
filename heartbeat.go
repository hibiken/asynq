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

// heartbeater is responsible for writing process info to redis periodically to
// indicate that the background worker process is up.
type heartbeater struct {
	rdb *rdb.RDB

	pinfo *base.ProcessInfo

	// channel to communicate back to the long running "heartbeater" goroutine.
	done chan struct{}

	// channel to receive updates on process state.
	stateCh <-chan string

	// channel to recieve updates on workers count.
	workerCh <-chan int

	// interval between heartbeats.
	interval time.Duration
}

func newHeartbeater(rdb *rdb.RDB, host string, pid, concurrency int, queues map[string]int, strict bool,
	interval time.Duration, stateCh <-chan string, workerCh <-chan int) *heartbeater {
	return &heartbeater{
		rdb:      rdb,
		pinfo:    base.NewProcessInfo(host, pid, concurrency, queues, strict),
		done:     make(chan struct{}),
		stateCh:  stateCh,
		workerCh: workerCh,
		interval: interval,
	}
}

func (h *heartbeater) terminate() {
	logger.info("Heartbeater shutting down...")
	// Signal the heartbeater goroutine to stop.
	h.done <- struct{}{}
}

func (h *heartbeater) start(wg *sync.WaitGroup) {
	h.pinfo.Started = time.Now()
	h.pinfo.State = "running"
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.beat()
		timer := time.NewTimer(h.interval)
		for {
			select {
			case <-h.done:
				h.rdb.ClearProcessInfo(h.pinfo)
				logger.info("Heartbeater done")
				return
			case state := <-h.stateCh:
				h.pinfo.State = state
			case delta := <-h.workerCh:
				h.pinfo.ActiveWorkerCount += delta
			case <-timer.C:
				h.beat()
				timer.Reset(h.interval)
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
