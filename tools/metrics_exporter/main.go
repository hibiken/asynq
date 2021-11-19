package main

import (
	"log"
	"net/http"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

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

func NewBrokerMetricsCollector() *BrokerMetricsCollector {
	return &BrokerMetricsCollector{}
}

func main() {
	reg := prometheus.NewPedanticRegistry()

	bmc := NewBrokerMetricsCollector()
	reg.MustRegister(
		bmc,
		// TODO: Add the standard process and go metrics to the registry
	)

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Fatal(http.ListenAndServe(":9876", nil))
}
