package asynq

import (
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
)

// Background is a top-level entity for the background-task processing.
type Background struct {
	mu      sync.Mutex
	running bool

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

// Run starts the background-task processing and blocks until
// an os signal to exit the program is received. Once it receives
// a signal, it gracefully shuts down all pending workers and other
// goroutines to process the tasks.
func (bg *Background) Run(handler TaskHandler) {
	bg.start(handler)
	defer bg.stop()

	// Wait for a signal to exit.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)
	<-sigs
}

// starts the background-task processing.
func (bg *Background) start(handler TaskHandler) {
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

// stops the background-task processing.
func (bg *Background) stop() {
	bg.mu.Lock()
	defer bg.mu.Unlock()
	if !bg.running {
		return
	}

	bg.poller.terminate()
	bg.processor.terminate()

	bg.processor.handler = nil
	bg.running = false
}
