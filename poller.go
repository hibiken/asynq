package asynq

import (
	"fmt"
	"log"
	"time"
)

type poller struct {
	rdb *rdb

	// channel to communicate back to the long running "poller" goroutine.
	done chan struct{}

	// poll interval on average
	avgInterval time.Duration

	// redis ZSETs to poll
	zsets []string
}

func newPoller(rdb *rdb, avgInterval time.Duration, zsets []string) *poller {
	return &poller{
		rdb:         rdb,
		done:        make(chan struct{}),
		avgInterval: avgInterval,
		zsets:       zsets,
	}
}

func (p *poller) terminate() {
	// Signal the poller goroutine to stop polling.
	p.done <- struct{}{}
}

// start starts the "poller" goroutine.
func (p *poller) start() {
	go func() {
		for {
			select {
			case <-p.done:
				fmt.Println("-------------[Poller]---------------")
				fmt.Println("Poller shutting down...")
				fmt.Println("------------------------------------")
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
		if err := p.rdb.forward(zset); err != nil {
			log.Printf("[ERROR] could not forward scheduled tasks from %q: %v\n", zset, err)
		}
	}
}
