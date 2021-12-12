// TODO: Package description
// TODO: Should the package name singular (metric)?
package metrics

import (
	"fmt"
	"log"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
)

// Namespace used in fully-qualified metrics names.
const namespace = "asynq"

// QueueMetricsCollector gathers broker metrics.
// It implements prometheus.Collector interface.
type QueueMetricsCollector struct {
	inspector *asynq.Inspector
}

// collectQueueInfo gathers QueueInfo of all queues.
// Since this operation is expensive, it must be called once per collection.
func (qmc *QueueMetricsCollector) collectQueueInfo() ([]*asynq.QueueInfo, error) {
	qnames, err := qmc.inspector.Queues()
	if err != nil {
		return nil, fmt.Errorf("failed to get queue names: %v", err)
	}
	infos := make([]*asynq.QueueInfo, len(qnames))
	for i, qname := range qnames {
		qinfo, err := qmc.inspector.GetQueueInfo(qname)
		if err != nil {
			return nil, fmt.Errorf("failed to get queue info: %v", err)
		}
		infos[i] = qinfo
	}
	return infos, nil
}

// Descriptors used by QueueMetricsCollector
var (
	tasksQueuedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "tasks_enqueued_total"),
		"Number of tasks enqueued; broken down by queue and state.",
		[]string{"queue", "state"}, nil,
	)

	queueSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "queue_size"),
		"Number of tasks in a queue",
		[]string{"queue"}, nil,
	)

	queueLatencyDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "queue_latency_seconds"),
		"Number of seconds the oldest pending task is waiting in pending state to be processed.",
		[]string{"queue"}, nil,
	)

	pausedQueues = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "queue_paused_total"),
		"Number of queues paused",
		[]string{"queue"}, nil,
	)
)

func (qmc *QueueMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(qmc, ch)
}

func (qmc *QueueMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	queueInfos, err := qmc.collectQueueInfo()
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
			queueSizeDesc,
			prometheus.GaugeValue,
			float64(info.Size),
			info.Queue,
		)

		ch <- prometheus.MustNewConstMetric(
			queueLatencyDesc,
			prometheus.GaugeValue,
			info.Latency.Seconds(),
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

func NewQueueMetricsCollector(inspector *asynq.Inspector) *QueueMetricsCollector {
	return &QueueMetricsCollector{inspector: inspector}
}
