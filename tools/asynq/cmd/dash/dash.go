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
	redisInfo redisInfo
	err       error

	queueTableRowIdx int             // highlighted row in queue table
	taskTableRowIdx  int             // highlighted row in task table
	taskState        asynq.TaskState // highlighted task state in queue details view

	selectedQueue *asynq.QueueInfo // queue shown on queue details view

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
	DebugMode   bool
	UseRealData bool
}

func Run(opts Options) {
	s, err := tcell.NewScreen()
	if err != nil {
		fmt.Println("failed to create a screen: %v", err)
		os.Exit(1)
	}
	if err := s.Init(); err != nil {
		fmt.Println("failed to initialize screen: %v", err)
		os.Exit(1)
	}

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: ":6379"})

	// Set default text style
	baseStyle := tcell.StyleDefault.Background(tcell.ColorReset).Foreground(tcell.ColorReset)
	s.SetStyle(baseStyle)

	// channels to send/receive data fetched asynchronously
	var (
		errorCh     = make(chan error)
		queuesCh    = make(chan []*asynq.QueueInfo)
		tasksCh     = make(chan []*asynq.TaskInfo)
		redisInfoCh = make(chan *redisInfo)
	)

	go fetchQueueInfo(inspector, queuesCh, errorCh, opts)

	var state State // contained in this goroutine only; do not share

	// draw initial screen
	drawDash(s, baseStyle, &state, opts)

	eventCh := make(chan tcell.Event)
	done := make(chan struct{})
	const interval = 2 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// TODO: Double check that we are not leaking goroutine with this one.
	go s.ChannelEvents(eventCh, done)

	quit := func() {
		s.Fini()
		close(done)
		os.Exit(0)
	}
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
				// Esc and 'q' key have "go back" semantics
				if ev.Key() == tcell.KeyEscape || ev.Rune() == 'q' {
					if state.view == viewTypeHelp {
						state.view = state.prevView // exit help
						drawDash(s, baseStyle, &state, opts)
					} else if state.view == viewTypeQueueDetails {
						state.view = viewTypeQueues
						drawDash(s, baseStyle, &state, opts)
					} else {
						quit()
					}
				} else if ev.Key() == tcell.KeyCtrlC {
					quit()
				} else if ev.Key() == tcell.KeyCtrlL {
					s.Sync()
				} else if (ev.Key() == tcell.KeyDown || ev.Rune() == 'j') && state.view == viewTypeQueues {
					if state.queueTableRowIdx < len(state.queues) {
						state.queueTableRowIdx++
					} else {
						state.queueTableRowIdx = 0 // loop back
					}
					drawDash(s, baseStyle, &state, opts)
				} else if (ev.Key() == tcell.KeyUp || ev.Rune() == 'k') && state.view == viewTypeQueues {
					if state.queueTableRowIdx == 0 {
						state.queueTableRowIdx = len(state.queues)
					} else {
						state.queueTableRowIdx--
					}
					drawDash(s, baseStyle, &state, opts)
				} else if (ev.Key() == tcell.KeyDown || ev.Rune() == 'j') && state.view == viewTypeQueueDetails {
					if state.taskTableRowIdx < len(state.tasks) {
						state.taskTableRowIdx++
					} else {
						state.taskTableRowIdx = 0 // loop back
					}
					drawDash(s, baseStyle, &state, opts)
				} else if (ev.Key() == tcell.KeyUp || ev.Rune() == 'k') && state.view == viewTypeQueueDetails {
					if state.taskTableRowIdx == 0 {
						state.taskTableRowIdx = len(state.tasks)
					} else {
						state.taskTableRowIdx--
					}
					drawDash(s, baseStyle, &state, opts)
				} else if ev.Key() == tcell.KeyEnter {
					if state.view == viewTypeQueues && state.queueTableRowIdx != 0 {
						state.selectedQueue = state.queues[state.queueTableRowIdx-1]
						state.view = viewTypeQueueDetails
						state.taskState = asynq.TaskStateActive
						state.tasks = nil
						go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState, tasksCh, errorCh)
						drawDash(s, baseStyle, &state, opts)
					}
				} else if ev.Rune() == '?' {
					state.prevView = state.view
					state.view = viewTypeHelp
					drawDash(s, baseStyle, &state, opts)
				} else if ev.Key() == tcell.KeyF1 && state.view != viewTypeQueues {
					go fetchQueueInfo(inspector, queuesCh, errorCh, opts)
					ticker.Reset(interval)
					state.view = viewTypeQueues
					drawDash(s, baseStyle, &state, opts)
				} else if ev.Key() == tcell.KeyF2 && state.view != viewTypeServers {
					//TODO Start data fetch and reset ticker
					state.view = viewTypeServers
					drawDash(s, baseStyle, &state, opts)
				} else if ev.Key() == tcell.KeyF3 && state.view != viewTypeSchedulers {
					//TODO Start data fetch and reset ticker
					state.view = viewTypeSchedulers
					drawDash(s, baseStyle, &state, opts)
				} else if ev.Key() == tcell.KeyF4 && state.view != viewTypeRedis {
					go fetchRedisInfo(redisInfoCh, errorCh)
					ticker.Reset(interval)
					state.view = viewTypeRedis
					drawDash(s, baseStyle, &state, opts)
				} else if (ev.Key() == tcell.KeyRight || ev.Rune() == 'l') && state.view == viewTypeQueueDetails {
					state.taskState = nextTaskState(state.taskState)
					state.tasks = nil
					go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState, tasksCh, errorCh)
					drawDash(s, baseStyle, &state, opts)
				} else if (ev.Key() == tcell.KeyLeft || ev.Rune() == 'h') && state.view == viewTypeQueueDetails {
					state.taskState = prevTaskState(state.taskState)
					state.tasks = nil
					go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState, tasksCh, errorCh)
					drawDash(s, baseStyle, &state, opts)
				}
			}

		case <-ticker.C:
			switch state.view {
			case viewTypeQueues:
				go fetchQueueInfo(inspector, queuesCh, errorCh, opts)
			case viewTypeRedis:
				go fetchRedisInfo(redisInfoCh, errorCh)
			}

		case queues := <-queuesCh:
			state.queues = queues
			state.err = nil
			drawDash(s, baseStyle, &state, opts)

		case tasks := <-tasksCh:
			state.tasks = tasks
			state.err = nil
			drawDash(s, baseStyle, &state, opts)

		case redisInfo := <-redisInfoCh:
			state.redisInfo = *redisInfo
			state.err = nil
			drawDash(s, baseStyle, &state, opts)

		case err := <-errorCh:
			state.err = err
			drawDash(s, baseStyle, &state, opts)
		}
	}

}
