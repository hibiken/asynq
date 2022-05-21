// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"os"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/hibiken/asynq"
)

type keyEventHandler struct {
	s     tcell.Screen
	state *State
	opts  Options
	done  chan struct{}

	ticker    *time.Ticker
	inspector *asynq.Inspector

	errorCh     chan error
	queueCh     chan *asynq.QueueInfo
	queuesCh    chan []*asynq.QueueInfo
	groupsCh    chan []*asynq.GroupInfo
	tasksCh     chan []*asynq.TaskInfo
	redisInfoCh chan *redisInfo
}

func (h *keyEventHandler) quit() {
	h.s.Fini()
	close(h.done)
	os.Exit(0)
}

func (h *keyEventHandler) HandleKeyEvent(ev *tcell.EventKey) {
	if ev.Key() == tcell.KeyEscape || ev.Rune() == 'q' {
		h.goBack() // Esc and 'q' key have "go back" semantics
	} else if ev.Key() == tcell.KeyCtrlC {
		h.quit()
	} else if ev.Key() == tcell.KeyCtrlL {
		h.s.Sync()
	} else if ev.Key() == tcell.KeyDown || ev.Rune() == 'j' {
		h.handleDownKey()
	} else if ev.Key() == tcell.KeyUp || ev.Rune() == 'k' {
		h.handleUpKey()
	} else if ev.Key() == tcell.KeyRight || ev.Rune() == 'l' {
		h.handleRightKey()
	} else if ev.Key() == tcell.KeyLeft || ev.Rune() == 'h' {
		h.handleLeftKey()
	} else if ev.Key() == tcell.KeyEnter {
		h.handleEnterKey()
	} else if ev.Rune() == '?' {
		h.showHelp()
	} else if ev.Key() == tcell.KeyF1 {
		h.showQueues()
	} else if ev.Key() == tcell.KeyF2 {
		h.showServers()
	} else if ev.Key() == tcell.KeyF3 {
		h.showSchedulers()
	} else if ev.Key() == tcell.KeyF4 {
		h.showRedisInfo()
	} else if ev.Rune() == 'n' {
		h.nextPage()
	} else if ev.Rune() == 'p' {
		h.prevPage()
	}
}

func (h *keyEventHandler) goBack() {
	var (
		s     = h.s
		state = h.state
		opts  = h.opts
	)
	if state.view == viewTypeHelp {
		state.view = state.prevView // exit help
		drawDash(s, state, opts)
	} else if state.view == viewTypeQueueDetails {
		state.view = viewTypeQueues
		drawDash(s, state, opts)
	} else {
		h.quit()
	}
}

func (h *keyEventHandler) handleDownKey() {
	switch h.state.view {
	case viewTypeQueues:
		h.downKeyQueues()
	case viewTypeQueueDetails:
		h.downKeyQueueDetails()
	}
}

func (h *keyEventHandler) downKeyQueues() {
	if h.state.queueTableRowIdx < len(h.state.queues) {
		h.state.queueTableRowIdx++
	} else {
		h.state.queueTableRowIdx = 0 // loop back
	}
	drawDash(h.s, h.state, h.opts)
}

func (h *keyEventHandler) downKeyQueueDetails() {
	s, state, opts := h.s, h.state, h.opts
	if shouldShowGroupTable(state) {
		if state.groupTableRowIdx < groupPageSize(s) {
			state.groupTableRowIdx++
		} else {
			state.groupTableRowIdx = 0 // loop back
		}
	} else {
		if state.taskTableRowIdx < len(state.tasks) {
			state.taskTableRowIdx++
		} else {
			state.taskTableRowIdx = 0 // loop back
		}
	}
	drawDash(s, state, opts)
}

func (h *keyEventHandler) handleUpKey() {
	switch h.state.view {
	case viewTypeQueues:
		h.upKeyQueues()
	case viewTypeQueueDetails:
		h.upKeyQueueDetails()
	}
}

func (h *keyEventHandler) upKeyQueues() {
	s, state, opts := h.s, h.state, h.opts
	if state.queueTableRowIdx == 0 {
		state.queueTableRowIdx = len(state.queues)
	} else {
		state.queueTableRowIdx--
	}
	drawDash(s, state, opts)
}

func (h *keyEventHandler) upKeyQueueDetails() {
	s, state, opts := h.s, h.state, h.opts
	if shouldShowGroupTable(state) {
		if state.groupTableRowIdx == 0 {
			state.groupTableRowIdx = groupPageSize(s)
		} else {
			state.groupTableRowIdx--
		}
	} else {
		if state.taskTableRowIdx == 0 {
			state.taskTableRowIdx = len(state.tasks)
		} else {
			state.taskTableRowIdx--
		}
	}
	drawDash(s, state, opts)
}

func (h *keyEventHandler) handleEnterKey() {
	switch h.state.view {
	case viewTypeQueues:
		h.enterKeyQueues()
	case viewTypeQueueDetails:
		h.enterKeyQueueDetails()
	}
}

func (h *keyEventHandler) enterKeyQueues() {
	var (
		s         = h.s
		state     = h.state
		opts      = h.opts
		inspector = h.inspector
		ticker    = h.ticker
		errorCh   = h.errorCh
		tasksCh   = h.tasksCh
	)
	if state.queueTableRowIdx != 0 {
		state.selectedQueue = state.queues[state.queueTableRowIdx-1]
		state.view = viewTypeQueueDetails
		state.taskState = asynq.TaskStateActive
		state.tasks = nil
		state.pageNum = 1
		go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState,
			taskPageSize(s), state.pageNum, tasksCh, errorCh)
		ticker.Reset(opts.PollInterval)
		drawDash(s, state, opts)
	}
}

func (h *keyEventHandler) enterKeyQueueDetails() {
	var (
		s         = h.s
		state     = h.state
		opts      = h.opts
		inspector = h.inspector
		ticker    = h.ticker
		errorCh   = h.errorCh
		tasksCh   = h.tasksCh
	)
	if shouldShowGroupTable(state) && state.groupTableRowIdx != 0 {
		state.selectedGroup = state.groups[state.groupTableRowIdx-1]
		state.tasks = nil
		state.pageNum = 1
		go fetchAggregatingTasks(inspector, state.selectedQueue.Queue, state.selectedGroup.Group,
			taskPageSize(s), state.pageNum, tasksCh, errorCh)
		ticker.Reset(opts.PollInterval)
		drawDash(s, state, opts)
	}
}

func (h *keyEventHandler) handleLeftKey() {
	var (
		s         = h.s
		state     = h.state
		opts      = h.opts
		inspector = h.inspector
		ticker    = h.ticker
		errorCh   = h.errorCh
		tasksCh   = h.tasksCh
		groupsCh  = h.groupsCh
	)
	if state.view == viewTypeQueueDetails {
		state.taskState = prevTaskState(state.taskState)
		state.pageNum = 1
		state.taskTableRowIdx = 0
		state.tasks = nil
		state.selectedGroup = nil
		if shouldShowGroupTable(state) {
			go fetchGroups(inspector, state.selectedQueue.Queue, groupsCh, errorCh)
		} else {
			go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState,
				taskPageSize(s), state.pageNum, tasksCh, errorCh)
		}
		ticker.Reset(opts.PollInterval)
		drawDash(s, state, opts)
	}
}

func (h *keyEventHandler) handleRightKey() {
	var (
		s         = h.s
		state     = h.state
		opts      = h.opts
		inspector = h.inspector
		ticker    = h.ticker
		errorCh   = h.errorCh
		tasksCh   = h.tasksCh
		groupsCh  = h.groupsCh
	)
	if state.view == viewTypeQueueDetails {
		state.taskState = nextTaskState(state.taskState)
		state.pageNum = 1
		state.taskTableRowIdx = 0
		state.tasks = nil
		state.selectedGroup = nil
		if shouldShowGroupTable(state) {
			go fetchGroups(inspector, state.selectedQueue.Queue, groupsCh, errorCh)
		} else {
			go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState,
				taskPageSize(s), state.pageNum, tasksCh, errorCh)
		}
		ticker.Reset(opts.PollInterval)
		drawDash(s, state, opts)
	}
}

func (h *keyEventHandler) nextPage() {
	var (
		s         = h.s
		state     = h.state
		opts      = h.opts
		inspector = h.inspector
		ticker    = h.ticker
		errorCh   = h.errorCh
		tasksCh   = h.tasksCh
	)
	if state.view == viewTypeQueueDetails {
		if shouldShowGroupTable(state) {
			pageSize := groupPageSize(s)
			total := len(state.groups)
			start := (state.pageNum - 1) * pageSize
			end := start + pageSize
			if end <= total {
				state.pageNum++
				drawDash(s, state, opts)
			}
		} else {
			pageSize := taskPageSize(s)
			totalCount := getTaskCount(state.selectedQueue, state.taskState)
			if (state.pageNum-1)*pageSize+len(state.tasks) < totalCount {
				state.pageNum++
				go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState,
					pageSize, state.pageNum, tasksCh, errorCh)
				ticker.Reset(opts.PollInterval)
			}
		}
	}
}

func (h *keyEventHandler) prevPage() {
	var (
		s         = h.s
		state     = h.state
		opts      = h.opts
		inspector = h.inspector
		ticker    = h.ticker
		errorCh   = h.errorCh
		tasksCh   = h.tasksCh
	)
	if state.view == viewTypeQueueDetails {
		if shouldShowGroupTable(state) {
			pageSize := groupPageSize(s)
			start := (state.pageNum - 1) * pageSize
			if start > 0 {
				state.pageNum--
				drawDash(s, state, opts)
			}
		} else {
			if state.pageNum > 1 {
				state.pageNum--
				go fetchTasks(inspector, state.selectedQueue.Queue, state.taskState,
					taskPageSize(s), state.pageNum, tasksCh, errorCh)
				ticker.Reset(opts.PollInterval)
			}
		}
	}
}

func (h *keyEventHandler) showQueues() {
	var (
		s         = h.s
		state     = h.state
		inspector = h.inspector
		queuesCh  = h.queuesCh
		errorCh   = h.errorCh
		opts      = h.opts
		ticker    = h.ticker
	)
	if state.view != viewTypeQueues {
		go fetchQueues(inspector, queuesCh, errorCh, opts)
		ticker.Reset(opts.PollInterval)
		state.view = viewTypeQueues
		drawDash(s, state, opts)
	}
}

func (h *keyEventHandler) showServers() {
	var (
		s     = h.s
		state = h.state
		opts  = h.opts
	)
	if state.view != viewTypeServers {
		//TODO Start data fetch and reset ticker
		state.view = viewTypeServers
		drawDash(s, state, opts)
	}
}

func (h *keyEventHandler) showSchedulers() {
	var (
		s     = h.s
		state = h.state
		opts  = h.opts
	)
	if state.view != viewTypeSchedulers {
		//TODO Start data fetch and reset ticker
		state.view = viewTypeSchedulers
		drawDash(s, state, opts)
	}
}

func (h *keyEventHandler) showRedisInfo() {
	var (
		s           = h.s
		state       = h.state
		opts        = h.opts
		redisInfoCh = h.redisInfoCh
		errorCh     = h.errorCh
		ticker      = h.ticker
	)
	if state.view != viewTypeRedis {
		go fetchRedisInfo(redisInfoCh, errorCh)
		ticker.Reset(opts.PollInterval)
		state.view = viewTypeRedis
		drawDash(s, state, opts)
	}
}

func (h *keyEventHandler) showHelp() {
	var (
		s     = h.s
		state = h.state
		opts  = h.opts
	)
	if state.view != viewTypeHelp {
		state.prevView = state.view
		state.view = viewTypeHelp
		drawDash(s, state, opts)
	}
}
