// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Kua-Fu/asynq"
	"github.com/gdamore/tcell/v2"
)

// viewType is an enum for dashboard views.
type viewType int

const (
	viewTypeQueues viewType = iota
	viewTypeQueueDetails
	viewTypeHelp
)

// State holds dashboard state.
type State struct {
	queues []*asynq.QueueInfo
	tasks  []*asynq.TaskInfo
	groups []*asynq.GroupInfo
	err    error

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

func (s *State) DebugString() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("len(queues)=%d ", len(s.queues)))
	b.WriteString(fmt.Sprintf("len(tasks)=%d ", len(s.tasks)))
	b.WriteString(fmt.Sprintf("len(groups)=%d ", len(s.groups)))
	b.WriteString(fmt.Sprintf("err=%v ", s.err))

	if s.taskState != 0 {
		b.WriteString(fmt.Sprintf("taskState=%s ", s.taskState.String()))
	} else {
		b.WriteString(fmt.Sprintf("taskState=0"))
	}
	b.WriteString(fmt.Sprintf("taskID=%s ", s.taskID))

	b.WriteString(fmt.Sprintf("queueTableRowIdx=%d ", s.queueTableRowIdx))
	b.WriteString(fmt.Sprintf("taskTableRowIdx=%d ", s.taskTableRowIdx))
	b.WriteString(fmt.Sprintf("groupTableRowIdx=%d ", s.groupTableRowIdx))

	if s.selectedQueue != nil {
		b.WriteString(fmt.Sprintf("selectedQueue={Queue:%s} ", s.selectedQueue.Queue))
	} else {
		b.WriteString("selectedQueue=nil ")
	}

	if s.selectedGroup != nil {
		b.WriteString(fmt.Sprintf("selectedGroup={Group:%s} ", s.selectedGroup.Group))
	} else {
		b.WriteString("selectedGroup=nil ")
	}

	if s.selectedTask != nil {
		b.WriteString(fmt.Sprintf("selectedTask={ID:%s} ", s.selectedTask.ID))
	} else {
		b.WriteString("selectedTask=nil ")
	}

	b.WriteString(fmt.Sprintf("pageNum=%d", s.pageNum))
	return b.String()
}

type Options struct {
	DebugMode    bool
	PollInterval time.Duration
	RedisConnOpt asynq.RedisConnOpt
}

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

		inspector = asynq.NewInspector(opts.RedisConnOpt)
		ticker    = time.NewTicker(opts.PollInterval)

		eventCh = make(chan tcell.Event)
		done    = make(chan struct{})

		// channels to send/receive data fetched asynchronously
		errorCh  = make(chan error)
		queueCh  = make(chan *asynq.QueueInfo)
		taskCh   = make(chan *asynq.TaskInfo)
		queuesCh = make(chan []*asynq.QueueInfo)
		groupsCh = make(chan []*asynq.GroupInfo)
		tasksCh  = make(chan []*asynq.TaskInfo)
	)
	defer ticker.Stop()

	f := dataFetcher{
		inspector,
		opts,
		s,
		errorCh,
		queueCh,
		taskCh,
		queuesCh,
		groupsCh,
		tasksCh,
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
	d.Draw(&state)                    // draw initial screen

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
			f.Fetch(&state)

		case queues := <-queuesCh:
			state.queues = queues
			state.err = nil
			if len(queues) < state.queueTableRowIdx {
				state.queueTableRowIdx = len(queues)
			}
			d.Draw(&state)

		case q := <-queueCh:
			state.selectedQueue = q
			state.err = nil
			d.Draw(&state)

		case groups := <-groupsCh:
			state.groups = groups
			state.err = nil
			if len(groups) < state.groupTableRowIdx {
				state.groupTableRowIdx = len(groups)
			}
			d.Draw(&state)

		case tasks := <-tasksCh:
			state.tasks = tasks
			state.err = nil
			if len(tasks) < state.taskTableRowIdx {
				state.taskTableRowIdx = len(tasks)
			}
			d.Draw(&state)

		case t := <-taskCh:
			state.selectedTask = t
			state.err = nil
			d.Draw(&state)

		case err := <-errorCh:
			if errors.Is(err, asynq.ErrTaskNotFound) {
				state.selectedTask = nil
			} else {
				state.err = err
			}
			d.Draw(&state)
		}
	}

}
