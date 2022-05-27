// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"math/rand"

	"github.com/gdamore/tcell/v2"
	"github.com/hibiken/asynq"
)

type fetcher interface {
	// Fetch retries data required by the given state of the dashboard.
	Fetch(state *State)
}

type dataFetcher struct {
	inspector *asynq.Inspector
	opts      Options
	s         tcell.Screen

	errorCh     chan<- error
	queueCh     chan<- *asynq.QueueInfo
	taskCh      chan<- *asynq.TaskInfo
	queuesCh    chan<- []*asynq.QueueInfo
	groupsCh    chan<- []*asynq.GroupInfo
	tasksCh     chan<- []*asynq.TaskInfo
	redisInfoCh chan<- *redisInfo
}

func (f *dataFetcher) Fetch(state *State) {
	switch state.view {
	case viewTypeQueues:
		f.fetchQueues()
	case viewTypeQueueDetails:
		if shouldShowGroupTable(state) {
			f.fetchGroups(state.selectedQueue.Queue)
		} else if state.taskState == asynq.TaskStateAggregating {
			f.fetchAggregatingTasks(state.selectedQueue.Queue, state.selectedGroup.Group, taskPageSize(f.s), state.pageNum)
		} else {
			f.fetchTasks(state.selectedQueue.Queue, state.taskState, taskPageSize(f.s), state.pageNum)
		}
		// if the task modal is open, additionally fetch the selected task's info
		if state.taskID != "" {
			f.fetchTaskInfo(state.selectedQueue.Queue, state.taskID)
		}
	case viewTypeRedis:
		f.fetchRedisInfo()
	}
}

func (f *dataFetcher) fetchQueues() {
	var (
		inspector = f.inspector
		queuesCh  = f.queuesCh
		errorCh   = f.errorCh
		opts      = f.opts
	)
	go fetchQueues(inspector, queuesCh, errorCh, opts)
}

func fetchQueues(i *asynq.Inspector, queuesCh chan<- []*asynq.QueueInfo, errorCh chan<- error, opts Options) {
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

func fetchQueueInfo(i *asynq.Inspector, qname string, queueCh chan<- *asynq.QueueInfo, errorCh chan<- error) {
	q, err := i.GetQueueInfo(qname)
	if err != nil {
		errorCh <- err
		return
	}
	queueCh <- q
}

func (f *dataFetcher) fetchRedisInfo() {
	go fetchRedisInfo(f.redisInfoCh, f.errorCh)
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

func (f *dataFetcher) fetchGroups(qname string) {
	var (
		i        = f.inspector
		groupsCh = f.groupsCh
		errorCh  = f.errorCh
		queueCh  = f.queueCh
	)
	go fetchGroups(i, qname, groupsCh, errorCh)
	go fetchQueueInfo(i, qname, queueCh, errorCh)
}

func fetchGroups(i *asynq.Inspector, qname string, groupsCh chan<- []*asynq.GroupInfo, errorCh chan<- error) {
	groups, err := i.Groups(qname)
	if err != nil {
		errorCh <- err
		return
	}
	groupsCh <- groups
}

func (f *dataFetcher) fetchAggregatingTasks(qname, group string, pageSize, pageNum int) {
	var (
		i       = f.inspector
		tasksCh = f.tasksCh
		errorCh = f.errorCh
		queueCh = f.queueCh
	)
	go fetchAggregatingTasks(i, qname, group, pageSize, pageNum, tasksCh, errorCh)
	go fetchQueueInfo(i, qname, queueCh, errorCh)
}

func fetchAggregatingTasks(i *asynq.Inspector, qname, group string, pageSize, pageNum int,
	tasksCh chan<- []*asynq.TaskInfo, errorCh chan<- error) {
	tasks, err := i.ListAggregatingTasks(qname, group, asynq.PageSize(pageSize), asynq.Page(pageNum))
	if err != nil {
		errorCh <- err
		return
	}
	tasksCh <- tasks
}

func (f *dataFetcher) fetchTasks(qname string, taskState asynq.TaskState, pageSize, pageNum int) {
	var (
		i       = f.inspector
		tasksCh = f.tasksCh
		errorCh = f.errorCh
		queueCh = f.queueCh
	)
	go fetchTasks(i, qname, taskState, pageSize, pageNum, tasksCh, errorCh)
	go fetchQueueInfo(i, qname, queueCh, errorCh)
}

func fetchTasks(i *asynq.Inspector, qname string, taskState asynq.TaskState, pageSize, pageNum int,
	tasksCh chan<- []*asynq.TaskInfo, errorCh chan<- error) {
	var (
		tasks []*asynq.TaskInfo
		err   error
	)
	opts := []asynq.ListOption{asynq.PageSize(pageSize), asynq.Page(pageNum)}
	switch taskState {
	case asynq.TaskStateActive:
		tasks, err = i.ListActiveTasks(qname, opts...)
	case asynq.TaskStatePending:
		tasks, err = i.ListPendingTasks(qname, opts...)
	case asynq.TaskStateScheduled:
		tasks, err = i.ListScheduledTasks(qname, opts...)
	case asynq.TaskStateRetry:
		tasks, err = i.ListRetryTasks(qname, opts...)
	case asynq.TaskStateArchived:
		tasks, err = i.ListArchivedTasks(qname, opts...)
	case asynq.TaskStateCompleted:
		tasks, err = i.ListCompletedTasks(qname, opts...)
	}
	if err != nil {
		errorCh <- err
		return
	}
	tasksCh <- tasks
}

func (f *dataFetcher) fetchTaskInfo(qname, taskID string) {
	var (
		i       = f.inspector
		taskCh  = f.taskCh
		errorCh = f.errorCh
	)
	go fetchTaskInfo(i, qname, taskID, taskCh, errorCh)
}

func fetchTaskInfo(i *asynq.Inspector, qname, taskID string, taskCh chan<- *asynq.TaskInfo, errorCh chan<- error) {
	info, err := i.GetTaskInfo(qname, taskID)
	if err != nil {
		errorCh <- err
		return
	}
	taskCh <- info
}
