package base

import "time"

type GroupStat struct {
	// Name of the group.
	Group string

	// Size of the group.
	Size int
}

// Stats represents a state of queues at a certain time.
type Stats struct {
	// Name of the queue (e.g. "default", "critical").
	Queue string
	// MemoryUsage is the total number of bytes the queue and its tasks require
	// to be stored in redis. It is an approximate memory usage value in bytes
	// since the value is computed by sampling.
	MemoryUsage int64
	// Paused indicates whether the queue is paused.
	// If true, tasks in the queue should not be processed.
	Paused bool
	// Size is the total number of tasks in the queue.
	Size int

	// Groups is the total number of groups in the queue.
	Groups int

	// Number of tasks in each state.
	Pending     int
	Active      int
	Scheduled   int
	Retry       int
	Archived    int
	Completed   int
	Aggregating int

	// Number of tasks processed within the current date.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Number of tasks failed within the current date.
	Failed int

	// Total number of tasks processed (both succeeded and failed) from this queue.
	ProcessedTotal int
	// Total number of tasks failed.
	FailedTotal int

	// Latency of the queue, measured by the oldest pending task in the queue.
	Latency time.Duration
	// Time this stats was taken.
	Timestamp time.Time
}

// DailyStats holds aggregate data for a given day.
type DailyStats struct {
	// Name of the queue (e.g. "default", "critical").
	Queue string
	// Total number of tasks processed during the given day.
	// The number includes both succeeded and failed tasks.
	Processed int
	// Total number of tasks failed during the given day.
	Failed int
	// Date this stats was taken.
	Time time.Time
}
