package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq/internal/rdb"
)

// Example usage: watch -n5 asynqmon

func main() {
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   2,
	})
	r := rdb.NewRDB(c)

	stats, err := r.CurrentStats()
	if err != nil {
		log.Fatal(err)
	}
	printStats(stats)
	fmt.Println()
}

func printStats(s *rdb.Stats) {
	format := strings.Repeat("%v\t", 5) + "\n"
	tw := new(tabwriter.Writer).Init(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintf(tw, format, "Enqueued", "InProgress", "Scheduled", "Retry", "Dead")
	fmt.Fprintf(tw, format, "--------", "----------", "---------", "-----", "----")
	fmt.Fprintf(tw, format, s.Enqueued, s.InProgress, s.Scheduled, s.Retry, s.Dead)
	tw.Flush()
}
