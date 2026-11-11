// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/log"
)

// syncer is responsible for queuing up failed requests to redis and retry
// those requests to sync state between the background process and redis.
type syncer struct {
	logger *log.Logger

	requestsCh <-chan *syncRequest

	// channel to communicate back to the long running "syncer" goroutine.
	done chan struct{}

	// interval between sync operations.
	interval time.Duration
}

type syncRequest struct {
	fn       func() error // sync operation
	errMsg   string       // error message
	deadline time.Time    // request should be dropped if deadline has been exceeded
}

type syncerParams struct {
	logger     *log.Logger
	requestsCh <-chan *syncRequest
	interval   time.Duration
}

func newSyncer(params syncerParams) *syncer {
	return &syncer{
		logger:     params.logger,
		requestsCh: params.requestsCh,
		done:       make(chan struct{}),
		interval:   params.interval,
	}
}

func (s *syncer) shutdown() {
	s.logger.Debug("Syncer shutting down...")
	// Signal the syncer goroutine to stop.
	s.done <- struct{}{}
}

func (s *syncer) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		// A ticker keeps a fixed cadence. Re-creating a timer on every loop
		// iteration (time.After) would be reset by each incoming request, so
		// a steady stream of requests could starve the periodic retry.
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		var requests []*syncRequest
		for {
			select {
			case <-s.done:
				// Try sync one last time before shutting down.
				for _, req := range requests {
					if req.deadline.Before(time.Now()) {
						continue // drop stale request
					}
					if err := req.fn(); err != nil {
						s.logger.Error(req.errMsg)
					}
				}
				s.logger.Debug("Syncer done")
				return
			case req := <-s.requestsCh:
				requests = append(requests, req)
			case <-ticker.C:
				var temp []*syncRequest
				for _, req := range requests {
					if req.deadline.Before(time.Now()) {
						continue // drop stale request
					}
					if err := req.fn(); err != nil {
						temp = append(temp, req)
					}
				}
				requests = temp
			}
		}
	}()
}
