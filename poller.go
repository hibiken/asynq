package asynq

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis/v7"
)

type poller struct {
	rdb *redis.Client

	done chan struct{}

	// poll interval on average
	avgInterval time.Duration

	// redis ZSETs to poll
	zsets []string
}

func (p *poller) terminate() {
	p.done <- struct{}{}
}

func (p *poller) start() {
	go func() {
		for {
			select {
			case <-p.done:
				p.shutdown()
			default:
				p.enqueue()
				time.Sleep(p.avgInterval)
			}
		}
	}()
}

func (p *poller) enqueue() {
	for _, zset := range p.zsets {
		// Get next items in the queue with scores (time to execute) <= now.
		now := time.Now().Unix()
		fmt.Printf("[DEBUG] polling ZSET %q\n", zset)
		jobs, err := p.rdb.ZRangeByScore(zset,
			&redis.ZRangeBy{
				Min: "-inf",
				Max: strconv.Itoa(int(now))}).Result()
		fmt.Printf("len(jobs) = %d\n", len(jobs))
		if err != nil {
			log.Printf("radis command ZRANGEBYSCORE failed: %v\n", err)
			continue
		}
		if len(jobs) == 0 {
			fmt.Println("jobs empty")
			continue
		}

		for _, j := range jobs {
			fmt.Printf("[debug] j = %v\n", j)
			var msg taskMessage
			err = json.Unmarshal([]byte(j), &msg)
			if err != nil {
				fmt.Println("unmarshal failed")
				continue
			}

			fmt.Println("[debug] ZREM")
			if p.rdb.ZRem(zset, j).Val() > 0 {
				err = push(p.rdb, &msg)
				if err != nil {
					log.Printf("could not push task to queue %q: %v", msg.Queue, err)
					// TODO(hibiken): Handle this error properly. Add back to scheduled ZSET?
					continue
				}
			}
		}
	}
}

func (p *poller) shutdown() {
	// TODO(hibiken): implement this. Gracefully shutdown all active goroutines.
	fmt.Println("-------------[Poller]---------------")
	fmt.Println("Poller shutting down...")
	fmt.Println("------------------------------------")
}
