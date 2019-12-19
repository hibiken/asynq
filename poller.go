package asynq

import (
	"log"
	"time"

	"github.com/hibiken/asynq/internal/rdb"
)

type poller struct {
	rdb *rdb.RDB

	// channel to communicate back to the long running "poller" goroutine.
	done chan struct{}

	// poll interval on average
	avgInterval time.Duration
}

func newPoller(r *rdb.RDB, avgInterval time.Duration) *poller {
	return &poller{
		rdb:         r,
		done:        make(chan struct{}),
		avgInterval: avgInterval,
	}
}

func (p *poller) terminate() {
	log.Println("[INFO] Poller shutting down...")
	// Signal the poller goroutine to stop polling.
	p.done <- struct{}{}
}

// start starts the "poller" goroutine.
func (p *poller) start() {
	go func() {
		for {
			select {
			case <-p.done:
				log.Println("[INFO] Poller done.")
				return
			case <-time.After(p.avgInterval):
				p.exec()
			}
		}
	}()
}

func (p *poller) exec() {
	if err := p.rdb.CheckAndEnqueue(); err != nil {
		log.Printf("[ERROR] could not forward scheduled tasks: %v\n", err)
	}
}
