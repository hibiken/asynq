package asynq

import (
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

// Launcher starts the manager and poller.
type Launcher struct {
	// running indicates whether manager and poller are both running.
	running bool
	mu      sync.Mutex

	poller *poller

	manager *manager
}

// NewLauncher creates and returns a new Launcher.
func NewLauncher(poolSize int, opt *RedisOpt) *Launcher {
	client := redis.NewClient(&redis.Options{Addr: opt.Addr, Password: opt.Password})
	rdb := newRDB(client)
	poller := newPoller(rdb, 5*time.Second, []string{scheduled, retry})
	manager := newManager(rdb, poolSize, nil)
	return &Launcher{
		poller:  poller,
		manager: manager,
	}
}

// TaskHandler handles a given task and report any error.
type TaskHandler func(*Task) error

// Start starts the manager and poller.
func (l *Launcher) Start(handler TaskHandler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.running {
		return
	}
	l.running = true
	l.manager.handler = handler

	l.poller.start()
	l.manager.start()
}

// Stop stops both manager and poller.
func (l *Launcher) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.running {
		return
	}
	l.running = false

	l.poller.terminate()
	l.manager.terminate()
}
