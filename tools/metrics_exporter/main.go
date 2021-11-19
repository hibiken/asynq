package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
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

// Namespace used in fully-qualified metrics names.
const namespace = "asynq"

// BrokerMetricsCollector gathers broker metrics.
// It implements prometheus.Collector interface.
type BrokerMetricsCollector struct {
	inspector *asynq.Inspector
}

// metricsBundle is a structure that bundles all of the collected metrics data.
type metricsBundle struct {
	tasksQueued float64
}

// collectMetrics gathers all metrics data from the broker and updates the metrics values.
// Since this operation is expensive, it must be called once per collection.
func (bmc *BrokerMetricsCollector) collectMetrics() (*metricsBundle, error) {
	// TODO: This is where we do expensive collection of metrics.
	return &metricsBundle{tasksQueued: 1024.00}, nil
}

// Descriptors used by BrokerMetricsCollector
var (
	tasksQueuedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "tasks_enqueued_total"),
		"Number of tasks enqueued.",
		[]string{"queue"}, nil,
	)
)

func (bmc *BrokerMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(bmc, ch)
}

func (bmc *BrokerMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	data, err := bmc.collectMetrics()
	if err != nil {
		log.Printf("Failed to collect metrics data: %v", err)
	}
	ch <- prometheus.MustNewConstMetric(
		tasksQueuedDesc,
		prometheus.GaugeValue,
		data.tasksQueued,
		"default_queue", // TODO: We need to figure out how to dynamicallly do this.
	)
}

func NewBrokerMetricsCollector(inspector *asynq.Inspector) *BrokerMetricsCollector {
	return &BrokerMetricsCollector{inspector: inspector}
}

func init() {
	flag.StringVar(&flagRedisAddr, "redis-addr", "127.0.0.1:9763", "host:port of redis server to connect to")
	flag.IntVar(&flagRedisDB, "redis-db", 0, "redis DB number to use")
	flag.StringVar(&flagRedisPassword, "redis-password", "", "password used to connect to redis server")
	flag.StringVar(&flagRedisUsername, "redis-username", "", "username used to connect to redis server")
	flag.IntVar(&flagPort, "port", 9876, "port to use for the HTTP server")
}

func main() {
	// Using NewPedanticRegistry here to test the implementation of Collectors and Metrics.
	reg := prometheus.NewPedanticRegistry()

	inspector := asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     flagRedisAddr,
		DB:       flagRedisDB,
		Password: flagRedisPassword,
		Username: flagRedisUsername,
	})

	reg.MustRegister(
		NewBrokerMetricsCollector(inspector),
		// Add the standard process and go metrics to the registry
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
		prometheus.NewGoCollector(),
	)

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Fatal(http.ListenAndServe(":9876", nil))
}
