// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"math/rand"

	"github.com/hibiken/asynq"
)

func fetchQueueInfo(i *asynq.Inspector, queuesCh chan<- []*asynq.QueueInfo, errorCh chan<- error, opts Options) {
	if !opts.UseRealData {
		n := rand.Intn(100)
		queuesCh <- []*asynq.QueueInfo{
			{Queue: "default", Size: 1800 + n, Pending: 700 + n, Active: 300, Aggregating: 300, Scheduled: 200, Retry: 100, Archived: 200},
			{Queue: "critical", Size: 2300 + n, Pending: 1000 + n, Active: 500, Retry: 400, Completed: 400},
			{Queue: "low", Size: 900 + n, Pending: n, Active: 300, Scheduled: 400, Completed: 200},
		}
		return
	}
	queues, err := i.Queues()
	if err != nil {
		errorCh <- err
		return
	}
	var res []*asynq.QueueInfo
	for _, q := range queues {
		info, err := i.GetQueueInfo(q)
		if err != nil {
			errorCh <- err
			return
		}
		res = append(res, info)
	}
	queuesCh <- res

}

func fetchRedisInfo(redisInfoCh chan<- *redisInfo, errorCh chan<- error) {
	n := rand.Intn(1000)
	redisInfoCh <- &redisInfo{
		version:         "6.2.6",
		uptime:          "9 days",
		memoryUsage:     n,
		peakMemoryUsage: n + 123,
	}
}

func fetchTasks(i *asynq.Inspector, qname string, taskState asynq.TaskState, tasksCh chan<- []*asynq.TaskInfo, errorCh chan<- error) {
	var (
		tasks []*asynq.TaskInfo
		err   error
	)
	switch taskState {
	case asynq.TaskStateActive:
		tasks, err = i.ListActiveTasks(qname)
	case asynq.TaskStatePending:
		tasks, err = i.ListPendingTasks(qname)
	case asynq.TaskStateScheduled:
		tasks, err = i.ListScheduledTasks(qname)
	case asynq.TaskStateRetry:
		tasks, err = i.ListRetryTasks(qname)
	case asynq.TaskStateArchived:
		tasks, err = i.ListArchivedTasks(qname)
	case asynq.TaskStateCompleted:
		tasks, err = i.ListCompletedTasks(qname)
	}
	if err != nil {
		errorCh <- err
		return
	}
	tasksCh <- tasks
}
