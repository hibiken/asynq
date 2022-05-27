// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/hibiken/asynq"
)

// viewType is an enum for dashboard views.
type viewType int

const (
	viewTypeQueues viewType = iota
	viewTypeQueueDetails
	viewTypeServers
	viewTypeSchedulers
	viewTypeRedis
	viewTypeHelp
)

// State holds dashboard state.
type State struct {
	queues    []*asynq.QueueInfo
	tasks     []*asynq.TaskInfo
	groups    []*asynq.GroupInfo
	redisInfo redisInfo
	err       error

	// Note: index zero corresponds to the table header; index=1 correctponds to the first element
	queueTableRowIdx int             // highlighted row in queue table
	taskTableRowIdx  int             // highlighted row in task table
	groupTableRowIdx int             // highlighted row in group table
	taskState        asynq.TaskState // highlighted task state in queue details view
	taskID           string          // selected task ID

	selectedQueue *asynq.QueueInfo // queue shown on queue details view
	selectedGroup *asynq.GroupInfo
	selectedTask  *asynq.TaskInfo

	pageNum int // pagination page number

	view     viewType // current view type
	prevView viewType // to support "go back"
}

type redisInfo struct {
	version         string
	uptime          string
	memoryUsage     int
	peakMemoryUsage int
}

type Options struct {
	DebugMode    bool
	UseRealData  bool
	PollInterval time.Duration
}

var baseStyle = tcell.StyleDefault.Background(tcell.ColorReset).Foreground(tcell.ColorReset)

func Run(opts Options) {
	s, err := tcell.NewScreen()
	if err != nil {
		fmt.Printf("failed to create a screen: %v\n", err)
		os.Exit(1)
	}
	if err := s.Init(); err != nil {
		fmt.Printf("failed to initialize screen: %v\n", err)
		os.Exit(1)
	}
	s.SetStyle(baseStyle) // set default text style

	var (
		state = State{} // confined in this goroutine only; DO NOT SHARE

		inspector = asynq.NewInspector(asynq.RedisClientOpt{Addr: ":6379"})
		ticker    = time.NewTicker(opts.PollInterval)

		eventCh = make(chan tcell.Event)
		done    = make(chan struct{})

		// channels to send/receive data fetched asynchronously
		errorCh     = make(chan error)
		queueCh     = make(chan *asynq.QueueInfo)
		taskCh      = make(chan *asynq.TaskInfo)
		queuesCh    = make(chan []*asynq.QueueInfo)
		groupsCh    = make(chan []*asynq.GroupInfo)
		tasksCh     = make(chan []*asynq.TaskInfo)
		redisInfoCh = make(chan *redisInfo)
	)
	defer ticker.Stop()

	f := dataFetcher{
		inspector,
		opts,
		errorCh,
		queueCh,
		taskCh,
		queuesCh,
		groupsCh,
		tasksCh,
		redisInfoCh,
	}

	d := dashDrawer{
		s,
		opts,
	}

	h := keyEventHandler{
		s:            s,
		fetcher:      &f,
		drawer:       &d,
		state:        &state,
		done:         done,
		ticker:       ticker,
		pollInterval: opts.PollInterval,
	}

	go fetchQueues(inspector, queuesCh, errorCh, opts)
	go s.ChannelEvents(eventCh, done) // TODO: Double check that we are not leaking goroutine with this one.
	d.draw(&state)                    // draw initial screen

	for {
		// Update screen
		s.Show()

		select {
		case ev := <-eventCh:
			// Process event
			switch ev := ev.(type) {
			case *tcell.EventResize:
				s.Sync()
			case *tcell.EventKey:
				h.HandleKeyEvent(ev)
			}

		case <-ticker.C:
			switch state.view {
			case viewTypeQueues:
				go fetchQueues(inspector, queuesCh, errorCh, opts)
			case viewTypeQueueDetails:
				go fetchQueueInfo(inspector, state.selectedQueue.Queue, queueCh, errorCh)
				if shouldShowGroupTable(&state) {
					go fetchGroups(inspector, state.selectedQueue.Queue, groupsCh, errorCh)
				} else if state.taskState == asynq.TaskStateAggregating {
					go fetchAggregatingTasks(inspector, state.selectedQueue.Queue, state.selectedGroup.Group,
						taskPageSize(s), state.pageNum, tasksCh, errorCh)
				} else {
					go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState,
						taskPageSize(s), state.pageNum, tasksCh, errorCh)
				}
				// if the task modal is open, fetch the selected task's info
				if state.taskID != "" {
					go fetchTaskInfo(inspector, state.selectedQueue.Queue, state.taskID, taskCh, errorCh)
				}

			case viewTypeRedis:
				go fetchRedisInfo(redisInfoCh, errorCh)
			}

		case queues := <-queuesCh:
			state.queues = queues
			state.err = nil
			if len(queues) < state.queueTableRowIdx {
				state.queueTableRowIdx = len(queues)
			}
			d.draw(&state)

		case q := <-queueCh:
			state.selectedQueue = q
			state.err = nil
			d.draw(&state)

		case groups := <-groupsCh:
			state.groups = groups
			state.err = nil
			if len(groups) < state.groupTableRowIdx {
				state.groupTableRowIdx = len(groups)
			}
			d.draw(&state)

		case tasks := <-tasksCh:
			state.tasks = tasks
			state.err = nil
			if len(tasks) < state.taskTableRowIdx {
				state.taskTableRowIdx = len(tasks)
			}
			d.draw(&state)

		case t := <-taskCh:
			state.selectedTask = t
			state.err = nil
			d.draw(&state)

		case redisInfo := <-redisInfoCh:
			state.redisInfo = *redisInfo
			state.err = nil
			d.draw(&state)

		case err := <-errorCh:
			if errors.Is(err, asynq.ErrTaskNotFound) {
				state.selectedTask = nil
			} else {
				state.err = err
			}
			d.draw(&state)
		}
	}

}
