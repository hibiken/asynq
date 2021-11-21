package main

import (
	"flag"
	"fmt"
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

// collectQueueInfo gathers QueueInfo of all queues.
// Since this operation is expensive, it must be called once per collection.
func (bmc *BrokerMetricsCollector) collectQueueInfo() ([]*asynq.QueueInfo, error) {
	qnames, err := bmc.inspector.Queues()
	if err != nil {
		return nil, fmt.Errorf("failed to get queue names: %v", err)
	}
	infos := make([]*asynq.QueueInfo, len(qnames))
	for i, qname := range qnames {
		qinfo, err := bmc.inspector.GetQueueInfo(qname)
		if err != nil {
			return nil, fmt.Errorf("failed to get queue info: %v", err)
		}
		infos[i] = qinfo
	}
	return infos, nil
}

// Descriptors used by BrokerMetricsCollector
var (
	tasksQueuedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "tasks_enqueued_total"),
		"Number of tasks enqueued; broken down by queue and state.",
		[]string{"queue", "state"}, nil,
	)

	tasksProcessed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "tasks_processed_total"),
		"Number of tasks processed (succedeed or failed); broken down by queue.",
		[]string{"queue"}, nil,
	)

	tasksFailed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "tasks_failed_total"),
		"Number of tasks failed; broken down by queue.",
		[]string{"queue"}, nil,
	)

	pausedQueues = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "queue_paused_total"),
		"Number of queues paused",
		[]string{"queue"}, nil,
	)
)

func (bmc *BrokerMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(bmc, ch)
}

func (bmc *BrokerMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	queueInfos, err := bmc.collectQueueInfo()
	if err != nil {
		log.Printf("Failed to collect metrics data: %v", err)
	}
	for _, info := range queueInfos {
		ch <- prometheus.MustNewConstMetric(
			tasksQueuedDesc,
			prometheus.GaugeValue,
			float64(info.Active),
			info.Queue,
			"active",
		)
		ch <- prometheus.MustNewConstMetric(
			tasksQueuedDesc,
			prometheus.GaugeValue,
			float64(info.Pending),
			info.Queue,
			"pending",
		)
		ch <- prometheus.MustNewConstMetric(
			tasksQueuedDesc,
			prometheus.GaugeValue,
			float64(info.Scheduled),
			info.Queue,
			"scheduled",
		)
		ch <- prometheus.MustNewConstMetric(
			tasksQueuedDesc,
			prometheus.GaugeValue,
			float64(info.Retry),
			info.Queue,
			"retry",
		)
		ch <- prometheus.MustNewConstMetric(
			tasksQueuedDesc,
			prometheus.GaugeValue,
			float64(info.Archived),
			info.Queue,
			"archived",
		)
		ch <- prometheus.MustNewConstMetric(
			tasksQueuedDesc,
			prometheus.GaugeValue,
			float64(info.Completed),
			info.Queue,
			"completed",
		)
		ch <- prometheus.MustNewConstMetric(
			tasksProcessed,
			prometheus.CounterValue,
			float64(info.Processed),
			info.Queue,
		)
		ch <- prometheus.MustNewConstMetric(
			tasksFailed,
			prometheus.CounterValue,
			float64(info.Failed),
			info.Queue,
		)
		pausedValue := 0 // zero to indicate "not paused"
		if info.Paused {
			pausedValue = 1
		}
		ch <- prometheus.MustNewConstMetric(
			pausedQueues,
			prometheus.GaugeValue,
			float64(pausedValue),
			info.Queue,
		)
	}
}

func NewBrokerMetricsCollector(inspector *asynq.Inspector) *BrokerMetricsCollector {
	return &BrokerMetricsCollector{inspector: inspector}
}

func init() {
	flag.StringVar(&flagRedisAddr, "redis-addr", "127.0.0.1:6379", "host:port of redis server to connect to")
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
	log.Printf("exporter server is listening on port: %d\n", flagPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", flagPort), nil))
}
