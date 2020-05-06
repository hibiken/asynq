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
	fn     func() error // sync operation
	errMsg string       // error message
}

func newSyncer(l *log.Logger, requestsCh <-chan *syncRequest, interval time.Duration) *syncer {
	return &syncer{
		logger:     l,
		requestsCh: requestsCh,
		done:       make(chan struct{}),
		interval:   interval,
	}
}

func (s *syncer) terminate() {
	s.logger.Info("Syncer shutting down...")
	// Signal the syncer goroutine to stop.
	s.done <- struct{}{}
}

func (s *syncer) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var requests []*syncRequest
		for {
			select {
			case <-s.done:
				// Try sync one last time before shutting down.
				for _, req := range requests {
					if err := req.fn(); err != nil {
						s.logger.Error(req.errMsg)
					}
				}
				s.logger.Info("Syncer done")
				return
			case req := <-s.requestsCh:
				requests = append(requests, req)
			case <-time.After(s.interval):
				var temp []*syncRequest
				for _, req := range requests {
					if err := req.fn(); err != nil {
						temp = append(temp, req)
					}
				}
				requests = temp
			}
		}
	}()
}
