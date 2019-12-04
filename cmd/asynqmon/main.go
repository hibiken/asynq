package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
)

var pollInterval = flag.Duration("interval", 3*time.Second, "polling interval")

func main() {
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   2,
	})
	r := rdb.NewRDB(c)

	for {
		stats, err := r.CurrentStats()
		if err != nil {
			log.Fatal(err)
		}
		printStats(stats)
		fmt.Println()
		time.Sleep(*pollInterval)
	}
}

func printStats(s *rdb.Stats) {
	format := strings.Repeat("%v\t", 5) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "Enqueued", "InProgress", "Scheduled", "Retry", "Dead")
	fmt.Fprintf(tw, format, "--------", "----------", "---------", "-----", "----")
	fmt.Fprintf(tw, format, s.Queued, s.InProgress, s.Scheduled, s.Retry, s.Dead)
	tw.Flush()
}
