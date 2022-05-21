// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
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

	queueTableRowIdx int             // highlighted row in queue table
	taskTableRowIdx  int             // highlighted row in task table
	groupTableRowIdx int             // highlighted row in group table
	taskState        asynq.TaskState // highlighted task state in queue details view

	selectedQueue *asynq.QueueInfo // queue shown on queue details view
	selectedGroup *asynq.GroupInfo

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
	// Set default text style
	s.SetStyle(baseStyle)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: ":6379"})

	// channels to send/receive data fetched asynchronously
	var (
		errorCh     = make(chan error)
		queueCh     = make(chan *asynq.QueueInfo)
		queuesCh    = make(chan []*asynq.QueueInfo)
		groupsCh    = make(chan []*asynq.GroupInfo)
		tasksCh     = make(chan []*asynq.TaskInfo)
		redisInfoCh = make(chan *redisInfo)
	)
	go fetchQueues(inspector, queuesCh, errorCh, opts)

	var state State // contained in this goroutine only; do not share

	// draw initial screen
	drawDash(s, &state, opts)

	eventCh := make(chan tcell.Event)
	done := make(chan struct{})
	opts.PollInterval = 2 * time.Second
	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()

	f := dataFetcher{
		ticker,
		inspector,
		opts,
		errorCh,
		queueCh,
		queuesCh,
		groupsCh,
		tasksCh,
		redisInfoCh,
	}

	h := keyEventHandler{
		s:       s,
		fetcher: &f,
		state:   &state,
		opts:    opts,
		done:    done,
	}

	// TODO: Double check that we are not leaking goroutine with this one.
	go s.ChannelEvents(eventCh, done)

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
			case viewTypeRedis:
				go fetchRedisInfo(redisInfoCh, errorCh)
			}

		case queues := <-queuesCh:
			state.queues = queues
			state.err = nil
			drawDash(s, &state, opts)

		case q := <-queueCh:
			state.selectedQueue = q
			state.err = nil
			drawDash(s, &state, opts)

		case groups := <-groupsCh:
			state.groups = groups
			state.err = nil
			drawDash(s, &state, opts)

		case tasks := <-tasksCh:
			state.tasks = tasks
			state.err = nil
			drawDash(s, &state, opts)

		case redisInfo := <-redisInfoCh:
			state.redisInfo = *redisInfo
			state.err = nil
			drawDash(s, &state, opts)

		case err := <-errorCh:
			state.err = err
			drawDash(s, &state, opts)
		}
	}

}
