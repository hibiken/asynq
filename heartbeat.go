// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// heartbeater is responsible for writing process info to redis periodically to
// indicate that the background worker process is up.
type heartbeater struct {
	logger *log.Logger
	broker base.Broker

	ss *base.ServerState

	// channel to communicate back to the long running "heartbeater" goroutine.
	done chan struct{}

	// interval between heartbeats.
	interval time.Duration
}

type heartbeaterParams struct {
	logger      *log.Logger
	broker      base.Broker
	serverState *base.ServerState
	interval    time.Duration
}

func newHeartbeater(params heartbeaterParams) *heartbeater {
	return &heartbeater{
		logger:   params.logger,
		broker:   params.broker,
		ss:       params.serverState,
		done:     make(chan struct{}),
		interval: params.interval,
	}
}

func (h *heartbeater) terminate() {
	h.logger.Debug("Heartbeater shutting down...")
	// Signal the heartbeater goroutine to stop.
	h.done <- struct{}{}
}

func (h *heartbeater) start(wg *sync.WaitGroup) {
	h.ss.SetStarted(time.Now())
	h.ss.SetStatus(base.StatusRunning)
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.beat()
		for {
			select {
			case <-h.done:
				h.broker.ClearServerState(h.ss)
				h.logger.Debug("Heartbeater done")
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
	err := h.broker.WriteServerState(h.ss, h.interval*2)
	if err != nil {
		h.logger.Errorf("could not write heartbeat data: %v", err)
	}
}
