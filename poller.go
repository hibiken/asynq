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

	// redis ZSETs to poll
	zsets []string
}

func newPoller(r *rdb.RDB, avgInterval time.Duration, zsets []string) *poller {
	return &poller{
		rdb:         r,
		done:        make(chan struct{}),
		avgInterval: avgInterval,
		zsets:       zsets,
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
			default:
				p.exec()
				time.Sleep(p.avgInterval)
			}
		}
	}()
}

func (p *poller) exec() {
	for _, zset := range p.zsets {
		if err := p.rdb.Forward(zset); err != nil {
			log.Printf("[ERROR] could not forward scheduled tasks from %q: %v\n", zset, err)
		}
	}
}
