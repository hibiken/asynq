// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// heartbeater is responsible for writing process info to redis periodically to
// indicate that the background worker process is up.
type heartbeater struct {
	logger *log.Logger
	broker base.Broker

	// channel to communicate back to the long running "heartbeater" goroutine.
	done chan struct{}

	// interval between heartbeats.
	interval time.Duration

	// following fields are initialized at construction time and are immutable.
	host           string
	pid            int
	serverID       string
	concurrency    int
	queues         map[string]int
	strictPriority bool

	// following fields are mutable and should be accessed only by the
	// heartbeater goroutine. In other words, confine these variables
	// to this goroutine only.
	started time.Time
	workers map[string]workerStat

	// status is shared with other goroutine but is concurrency safe.
	status *base.ServerStatus

	// channels to receive updates on active workers.
	starting <-chan *base.TaskMessage
	finished <-chan *base.TaskMessage
}

type heartbeaterParams struct {
	logger         *log.Logger
	broker         base.Broker
	interval       time.Duration
	concurrency    int
	queues         map[string]int
	strictPriority bool
	status         *base.ServerStatus
	starting       <-chan *base.TaskMessage
	finished       <-chan *base.TaskMessage
}

func newHeartbeater(params heartbeaterParams) *heartbeater {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown-host"
	}

	return &heartbeater{
		logger:   params.logger,
		broker:   params.broker,
		done:     make(chan struct{}),
		interval: params.interval,

		host:           host,
		pid:            os.Getpid(),
		serverID:       uuid.New().String(),
		concurrency:    params.concurrency,
		queues:         params.queues,
		strictPriority: params.strictPriority,

		status:   params.status,
		workers:  make(map[string]workerStat),
		starting: params.starting,
		finished: params.finished,
	}
}

func (h *heartbeater) terminate() {
	h.logger.Debug("Heartbeater shutting down...")
	// Signal the heartbeater goroutine to stop.
	h.done <- struct{}{}
}

// A workerStat records the message a worker is working on
// and the time the worker has started processing the message.
type workerStat struct {
	started time.Time
	msg     *base.TaskMessage
}

func (h *heartbeater) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		h.started = time.Now()

		h.beat()

		timer := time.NewTimer(h.interval)
		for {
			select {
			case <-h.done:
				h.broker.ClearServerState(h.host, h.pid, h.serverID)
				h.logger.Debug("Heartbeater done")
				timer.Stop()
				return

			case <-timer.C:
				h.beat()
				timer.Reset(h.interval)

			case msg := <-h.starting:
				h.workers[msg.ID.String()] = workerStat{time.Now(), msg}

			case msg := <-h.finished:
				delete(h.workers, msg.ID.String())
			}
		}
	}()
}

func (h *heartbeater) beat() {
	info := base.ServerInfo{
		Host:              h.host,
		PID:               h.pid,
		ServerID:          h.serverID,
		Concurrency:       h.concurrency,
		Queues:            h.queues,
		StrictPriority:    h.strictPriority,
		Status:            h.status.String(),
		Started:           h.started,
		ActiveWorkerCount: len(h.workers),
	}

	var ws []*base.WorkerInfo
	for id, stat := range h.workers {
		ws = append(ws, &base.WorkerInfo{
			Host:    h.host,
			PID:     h.pid,
			ID:      id,
			Type:    stat.msg.Type,
			Queue:   stat.msg.Queue,
			Payload: stat.msg.Payload,
			Started: stat.started,
		})
	}

	// Note: Set TTL to be long enough so that it won't expire before we write again
	// and short enough to expire quickly once the process is shut down or killed.
	if err := h.broker.WriteServerState(&info, ws, h.interval*2); err != nil {
		h.logger.Errorf("could not write server state data: %v", err)
	}
}
