package asynq

import (
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

// Background is a top-level entity for the background-task processing.
type Background struct {
	// running indicates whether processor and poller are both running.
	running bool
	mu      sync.Mutex

	poller    *poller
	processor *processor
}

// NewBackground returns a new Background instance.
func NewBackground(numWorkers int, opt *RedisOpt) *Background {
	client := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	rdb := newRDB(client)
	poller := newPoller(rdb, 5*time.Second, []string{scheduled, retry})
	processor := newProcessor(rdb, numWorkers, nil)
	return &Background{
		poller:    poller,
		processor: processor,
	}
}

// TaskHandler handles a given task and reports any error.
type TaskHandler func(*Task) error

// Start starts the background-task processing.
func (bg *Background) Start(handler TaskHandler) {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if bg.running {
		return
	}
	bg.running = true
	bg.processor.handler = handler

	bg.poller.start()
	bg.processor.start()
}

// Stop stops the background-task processing.
func (bg *Background) Stop() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if !bg.running {
		return
	}
	bg.running = false
	bg.processor.handler = nil

	bg.poller.terminate()
	bg.processor.terminate()
}
