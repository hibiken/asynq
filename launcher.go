package asynq

import (
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

// Launcher starts the processor and poller.
type Launcher struct {
	// running indicates whether processor and poller are both running.
	running bool
	mu      sync.Mutex

	poller    *poller
	processor *processor
}

// NewLauncher creates and returns a new Launcher.
func NewLauncher(poolSize int, opt *RedisOpt) *Launcher {
	client := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	rdb := newRDB(client)
	poller := newPoller(rdb, 5*time.Second, []string{scheduled, retry})
	processor := newProcessor(rdb, poolSize, nil)
	return &Launcher{
		poller:    poller,
		processor: processor,
	}
}

// TaskHandler handles a given task and report any error.
type TaskHandler func(*Task) error

// Start starts the processor and poller.
func (l *Launcher) Start(handler TaskHandler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.running {
		return
	}
	l.running = true
	l.processor.handler = handler

	l.poller.start()
	l.processor.start()
}

// Stop stops both processor and poller.
func (l *Launcher) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.running {
		return
	}
	l.running = false

	l.poller.terminate()
	l.processor.terminate()
}
