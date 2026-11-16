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
	"github.com/hibiken/asynq/internal/timeutil"
)

// heartbeater is responsible for writing process info to redis periodically to
// indicate that the background worker process is up.
type heartbeater struct {
	logger *log.Logger
	broker base.Broker
	clock  timeutil.Clock

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
	workers map[string]*workerInfo

	// state is shared with other goroutine but is concurrency safe.
	state *serverState

	// channels to receive updates on active workers.
	starting <-chan *workerInfo
	finished <-chan *base.TaskMessage
}

type heartbeaterParams struct {
	logger         *log.Logger
	broker         base.Broker
	interval       time.Duration
	concurrency    int
	queues         map[string]int
	strictPriority bool
	state          *serverState
	starting       <-chan *workerInfo
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
		clock:    timeutil.NewRealClock(),
		done:     make(chan struct{}),
		interval: params.interval,

		host:           host,
		pid:            os.Getpid(),
		serverID:       uuid.New().String(),
		concurrency:    params.concurrency,
		queues:         params.queues,
		strictPriority: params.strictPriority,

		state:    params.state,
		workers:  make(map[string]*workerInfo),
		starting: params.starting,
		finished: params.finished,
	}
}

func (h *heartbeater) shutdown() {
	h.logger.Debug("Heartbeater shutting down...")
	// Signal the heartbeater goroutine to stop.
	h.done <- struct{}{}
}

// A workerInfo holds an active worker information.
type workerInfo struct {
	// the task message the worker is processing.
	msg *base.TaskMessage
	// the time the worker has started processing the message.
	started time.Time
	// deadline the worker has to finish processing the task by.
	deadline time.Time
	// lease the worker holds for the task.
	lease *base.Lease
}

func (h *heartbeater) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		h.started = h.clock.Now()

		// At most one asynchronous flush is in flight at any time. The redis
		// round trips happen outside this loop so that a slow redis cannot
		// block the starting/finished channels (and with them the processor).
		flushing := false
		flushDone := make(chan struct{}, 1)
		startFlush := func() {
			if flushing {
				h.logger.Warn("Previous heartbeat is still in progress; skipping this beat")
				return
			}
			flushing = true
			s := h.snapshot()
			go func() {
				h.flush(s)
				flushDone <- struct{}{}
			}()
		}

		startFlush() // initial heartbeat, so the server shows up right away

		timer := time.NewTimer(h.interval)
		for {
			select {
			case <-h.done:
				if flushing {
					<-flushDone // do not write server state after clearing it
				}
				if err := h.broker.ClearServerState(h.host, h.pid, h.serverID); err != nil {
					h.logger.Errorf("Failed to clear server state: %v", err)
				}
				h.logger.Debug("Heartbeater done")
				timer.Stop()
				return

			case <-timer.C:
				startFlush()
				timer.Reset(h.interval)

			case <-flushDone:
				flushing = false

			case w := <-h.starting:
				h.workers[w.msg.ID] = w

			case msg := <-h.finished:
				delete(h.workers, msg.ID)
			}
		}
	}()
}

// beat extends lease for workers and writes server/worker info to redis.
// It performs the redis round trips synchronously; the heartbeater goroutine
// uses snapshot/flush separately so that a slow redis does not block it.
func (h *heartbeater) beat() {
	h.flush(h.snapshot())
}

// leaseRef pairs a task ID with the lease the worker holds for it.
type leaseRef struct {
	id    string
	lease *base.Lease
}

// beatSnapshot is a point-in-time copy of everything needed for one heartbeat,
// so the redis I/O can run without touching heartbeater-confined state.
type beatSnapshot struct {
	info base.ServerInfo
	ws   []*base.WorkerInfo
	// leases to extend, grouped by queue.
	leases map[string][]leaseRef
}

// snapshot collects the heartbeat data. It does no I/O and must be called
// from the heartbeater goroutine.
func (h *heartbeater) snapshot() *beatSnapshot {
	h.state.mu.Lock()
	srvStatus := h.state.value.String()
	h.state.mu.Unlock()

	s := &beatSnapshot{
		info: base.ServerInfo{
			Host:              h.host,
			PID:               h.pid,
			ServerID:          h.serverID,
			Concurrency:       h.concurrency,
			Queues:            h.queues,
			StrictPriority:    h.strictPriority,
			Status:            srvStatus,
			Started:           h.started,
			ActiveWorkerCount: len(h.workers),
		},
		leases: make(map[string][]leaseRef),
	}

	for id, w := range h.workers {
		s.ws = append(s.ws, &base.WorkerInfo{
			Host:     h.host,
			PID:      h.pid,
			ServerID: h.serverID,
			ID:       id,
			Type:     w.msg.Type,
			Queue:    w.msg.Queue,
			Payload:  w.msg.Payload,
			Started:  w.started,
			Deadline: w.deadline,
		})
		// Check lease before adding to the set to make sure not to extend the lease if the lease is already expired.
		if w.lease.IsValid() {
			s.leases[w.msg.Queue] = append(s.leases[w.msg.Queue], leaseRef{id: id, lease: w.lease})
		} else {
			w.lease.NotifyExpiration() // notify processor if the lease is expired
		}
	}
	return s
}

// flush writes the snapshot to redis and extends the leases.
// It only uses the snapshot, so it is safe to run outside the heartbeater goroutine.
func (h *heartbeater) flush(s *beatSnapshot) {
	// Note: Set TTL to be long enough so that it won't expire before we write again
	// and short enough to expire quickly once the process is shut down or killed.
	if err := h.broker.WriteServerState(&s.info, s.ws, h.interval*2); err != nil {
		h.logger.Errorf("Failed to write server state data: %v", err)
	}

	for qname, refs := range s.leases {
		ids := make([]string, len(refs))
		for i, ref := range refs {
			ids[i] = ref.id
		}
		expirationTime, err := h.broker.ExtendLease(qname, ids...)
		if err != nil {
			h.logger.Errorf("Failed to extend lease for tasks %v: %v", ids, err)
			continue
		}
		for _, ref := range refs {
			if !ref.lease.Reset(expirationTime) {
				h.logger.Warnf("Lease reset failed for %s; lease deadline: %v", ref.id, ref.lease.Deadline())
			}
		}
	}
}
