// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

type subscriber struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "subscriber" goroutine.
	done chan struct{}

	// cancelations hold cancel functions for all in-progress tasks.
	cancelations *base.Cancelations

	// time to wait before retrying to connect to redis.
	retryTimeout time.Duration
}

func newSubscriber(l *log.Logger, b base.Broker, cancelations *base.Cancelations) *subscriber {
	return &subscriber{
		logger:       l,
		broker:       b,
		done:         make(chan struct{}),
		cancelations: cancelations,
		retryTimeout: 5 * time.Second,
	}
}

func (s *subscriber) terminate() {
	s.logger.Info("Subscriber shutting down...")
	// Signal the subscriber goroutine to stop.
	s.done <- struct{}{}
}

func (s *subscriber) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		var (
			pubsub *redis.PubSub
			err    error
		)
		// Try until successfully connect to Redis.
		for {
			pubsub, err = s.broker.CancelationPubSub()
			if err != nil {
				s.logger.Errorf("cannot subscribe to cancelation channel: %v", err)
				select {
				case <-time.After(s.retryTimeout):
					continue
				case <-s.done:
					s.logger.Info("Subscriber done")
					return
				}
			}
			break
		}
		cancelCh := pubsub.Channel()
		for {
			select {
			case <-s.done:
				pubsub.Close()
				s.logger.Info("Subscriber done")
				return
			case msg := <-cancelCh:
				cancel, ok := s.cancelations.Get(msg.Payload)
				if ok {
					cancel()
				}
			}
		}
	}()
}
