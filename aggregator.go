// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// An aggregator is responsible for checking groups and aggregate into one task
// if any of the grouping condition is met.
type aggregator struct {
	logger *log.Logger
	broker base.Broker
	client *Client

	// channel to communicate back to the long running "aggregator" goroutine.
	done chan struct{}

	// list of queue names to check and aggregate.
	queues []string

	// Group configurations
	gracePeriod time.Duration
	maxDelay    time.Duration
	maxSize     int

	// Aggregation function
	aggregateFunc func(gname string, tasks []*Task) *Task
}

type aggregatorParams struct {
	logger        *log.Logger
	broker        base.Broker
	queues        []string
	gracePeriod   time.Duration
	maxDelay      time.Duration
	maxSize       int
	aggregateFunc func(gname string, msgs []*Task) *Task
}

func newAggregator(params aggregatorParams) *aggregator {
	return &aggregator{
		logger:        params.logger,
		broker:        params.broker,
		client:        &Client{broker: params.broker},
		done:          make(chan struct{}),
		queues:        params.queues,
		gracePeriod:   params.gracePeriod,
		maxDelay:      params.maxDelay,
		maxSize:       params.maxSize,
		aggregateFunc: params.aggregateFunc,
	}
}

func (a *aggregator) shutdown() {
	a.logger.Debug("Aggregator shutting down...")
	// Signal the aggregator goroutine to stop.
	a.done <- struct{}{}
}

func (a *aggregator) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(a.gracePeriod)
		for {
			select {
			case <-a.done:
				a.logger.Debug("Aggregator done")
				return
			case <-timer.C:
				a.exec()
				timer.Reset(a.gracePeriod)
			}
		}
	}()
}

func (a *aggregator) exec() {
	for _, qname := range a.queues {
		groups, err := a.broker.ListGroups(qname)
		if err != nil {
			a.logger.Errorf("Failed to list groups in queue: %q", qname)
			continue
		}
		for _, gname := range groups {
			aggregationSetID, err := a.broker.AggregationCheck(qname, gname)
			if err != nil {
				a.logger.Errorf("Failed to run aggregation check: queue=%q group=%q", qname, gname)
				continue
			}
			if aggregationSetID == "" {
				a.logger.Debugf("No aggregation needed at this time: queue=%q group=%q", qname, gname)
				continue
			}

			// Aggregate and enqueue.
			msgs, deadline, err := a.broker.ReadAggregationSet(qname, gname, aggregationSetID)
			if err != nil {
				a.logger.Errorf("Failed to read aggregation set: queue=%q, group=%q, setID=%q",
					qname, gname, aggregationSetID)
				continue
			}
			tasks := make([]*Task, len(msgs))
			for i, m := range msgs {
				tasks[i] = NewTask(m.Type, m.Payload)
			}
			aggregatedTask := a.aggregateFunc(gname, tasks)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			if _, err := a.client.EnqueueContext(ctx, aggregatedTask); err != nil {
				a.logger.Errorf("Failed to enqueue aggregated task: %v", err)
				cancel()
				continue
			}
			if err := a.broker.DeleteAggregationSet(ctx, qname, gname, aggregationSetID); err != nil {
				a.logger.Warnf("Failed to delete aggregation set: queue=%q, group=%q, setID=%q",
					qname, gname, aggregationSetID)
			}
			cancel()
		}
	}
}
