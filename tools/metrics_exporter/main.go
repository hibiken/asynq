package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/Kua-Fu/asynq"
	"github.com/Kua-Fu/asynq/x/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Declare command-line flags.
// These variables are binded to flags in init().
var (
	flagRedisAddr     string
	flagRedisDB       int
	flagRedisPassword string
	flagRedisUsername string
	flagPort          int
)

func init() {
	flag.StringVar(&flagRedisAddr, "redis-addr", "127.0.0.1:6379", "host:port of redis server to connect to")
	flag.IntVar(&flagRedisDB, "redis-db", 0, "redis DB number to use")
	flag.StringVar(&flagRedisPassword, "redis-password", "", "password used to connect to redis server")
	flag.StringVar(&flagRedisUsername, "redis-username", "", "username used to connect to redis server")
	flag.IntVar(&flagPort, "port", 9876, "port to use for the HTTP server")
}

func main() {
	flag.Parse()
	// Using NewPedanticRegistry here to test the implementation of Collectors and Metrics.
	reg := prometheus.NewPedanticRegistry()

	inspector := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     flagRedisAddr,
		DB:       flagRedisDB,
		Password: flagRedisPassword,
		Username: flagRedisUsername,
	})

	reg.MustRegister(
		metrics.NewQueueMetricsCollector(inspector),
		// Add the standard process and go metrics to the registry
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Printf("exporter server is listening on port: %d\n", flagPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", flagPort), nil))
}
