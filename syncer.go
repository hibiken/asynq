// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"log"
	"time"
)

// syncer is responsible for queuing up failed requests to redis and retry
// those requests to sync state between the background process and redis.
type syncer struct {
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

func newSyncer(requestsCh <-chan *syncRequest, interval time.Duration) *syncer {
	return &syncer{
		requestsCh: requestsCh,
		done:       make(chan struct{}),
		interval:   interval,
	}
}

func (s *syncer) terminate() {
	log.Println("[INFO] Syncer shutting down...")
	// Signal the syncer goroutine to stop.
	s.done <- struct{}{}
}

func (s *syncer) start() {
	go func() {
		var requests []*syncRequest
		for {
			select {
			case <-s.done:
				// Try sync one last time before shutting down.
				for _, req := range requests {
					if err := req.fn(); err != nil {
						log.Printf("[ERROR] %s\n", req.errMsg)
					}
				}
				log.Println("[INFO] Syncer done.")
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
