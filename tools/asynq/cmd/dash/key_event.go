// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"os"

	"github.com/gdamore/tcell/v2"
	"github.com/hibiken/asynq"
)

// keyEventHandler handles keyboard events and updates the state.
// It delegates data fetching to fetcher and UI rendering to drawer.
type keyEventHandler struct {
	s     tcell.Screen
	state *State
	done  chan struct{}

	fetcher fetcher
	drawer  drawer
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
		state = h.state
		d     = h.drawer
	)
	if state.view == viewTypeHelp {
		state.view = state.prevView // exit help
		d.draw(state)
	} else if state.view == viewTypeQueueDetails {
		// if task modal is open close it; otherwise go back to the previous view
		if state.taskID != "" {
			state.taskID = ""
			state.selectedTask = nil
			d.draw(state)
		} else {
			state.view = viewTypeQueues
			d.draw(state)
		}
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
	h.drawer.draw(h.state)
}

func (h *keyEventHandler) downKeyQueueDetails() {
	s, state := h.s, h.state
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
	h.drawer.draw(state)
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
	state := h.state
	if state.queueTableRowIdx == 0 {
		state.queueTableRowIdx = len(state.queues)
	} else {
		state.queueTableRowIdx--
	}
	h.drawer.draw(state)
}

func (h *keyEventHandler) upKeyQueueDetails() {
	s, state := h.s, h.state
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
	h.drawer.draw(state)
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
		s     = h.s
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.queueTableRowIdx != 0 {
		state.selectedQueue = state.queues[state.queueTableRowIdx-1]
		state.view = viewTypeQueueDetails
		state.taskState = asynq.TaskStateActive
		state.tasks = nil
		state.pageNum = 1
		f.fetchTasks(state.selectedQueue.Queue, state.taskState, taskPageSize(s), state.pageNum)
		d.draw(state)
	}
}

func (h *keyEventHandler) enterKeyQueueDetails() {
	var (
		s     = h.s
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if shouldShowGroupTable(state) && state.groupTableRowIdx != 0 {
		state.selectedGroup = state.groups[state.groupTableRowIdx-1]
		state.tasks = nil
		state.pageNum = 1
		f.fetchAggregatingTasks(state.selectedQueue.Queue, state.selectedGroup.Group, taskPageSize(s), state.pageNum)
		d.draw(state)
	} else if !shouldShowGroupTable(state) && state.taskTableRowIdx != 0 {
		task := state.tasks[state.taskTableRowIdx-1]
		state.taskID = task.ID
		state.selectedTask = task
		// TODO: go fetch task info
		d.draw(state)
	}

}

func (h *keyEventHandler) handleLeftKey() {
	var (
		s     = h.s
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.view == viewTypeQueueDetails {
		state.taskState = prevTaskState(state.taskState)
		state.pageNum = 1
		state.taskTableRowIdx = 0
		state.tasks = nil
		state.selectedGroup = nil
		if shouldShowGroupTable(state) {
			f.fetchGroups(state.selectedQueue.Queue)
		} else {
			f.fetchTasks(state.selectedQueue.Queue, state.taskState, taskPageSize(s), state.pageNum)
		}
		d.draw(state)
	}
}

func (h *keyEventHandler) handleRightKey() {
	var (
		s     = h.s
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.view == viewTypeQueueDetails {
		state.taskState = nextTaskState(state.taskState)
		state.pageNum = 1
		state.taskTableRowIdx = 0
		state.tasks = nil
		state.selectedGroup = nil
		if shouldShowGroupTable(state) {
			f.fetchGroups(state.selectedQueue.Queue)
		} else {
			f.fetchTasks(state.selectedQueue.Queue, state.taskState, taskPageSize(s), state.pageNum)
		}
		d.draw(state)
	}
}

func (h *keyEventHandler) nextPage() {
	var (
		s     = h.s
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.view == viewTypeQueueDetails {
		if shouldShowGroupTable(state) {
			pageSize := groupPageSize(s)
			total := len(state.groups)
			start := (state.pageNum - 1) * pageSize
			end := start + pageSize
			if end <= total {
				state.pageNum++
				d.draw(state)
			}
		} else {
			pageSize := taskPageSize(s)
			totalCount := getTaskCount(state.selectedQueue, state.taskState)
			if (state.pageNum-1)*pageSize+len(state.tasks) < totalCount {
				state.pageNum++
				f.fetchTasks(state.selectedQueue.Queue, state.taskState, pageSize, state.pageNum)
			}
		}
	}
}

func (h *keyEventHandler) prevPage() {
	var (
		s     = h.s
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.view == viewTypeQueueDetails {
		if shouldShowGroupTable(state) {
			pageSize := groupPageSize(s)
			start := (state.pageNum - 1) * pageSize
			if start > 0 {
				state.pageNum--
				d.draw(state)
			}
		} else {
			if state.pageNum > 1 {
				state.pageNum--
				f.fetchTasks(state.selectedQueue.Queue, state.taskState, taskPageSize(s), state.pageNum)
			}
		}
	}
}

func (h *keyEventHandler) showQueues() {
	var (
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.view != viewTypeQueues {
		f.fetchQueues()
		state.view = viewTypeQueues
		d.draw(state)
	}
}

func (h *keyEventHandler) showServers() {
	var (
		state = h.state
		d     = h.drawer
	)
	if state.view != viewTypeServers {
		//TODO Start data fetch and reset ticker
		state.view = viewTypeServers
		d.draw(state)
	}
}

func (h *keyEventHandler) showSchedulers() {
	var (
		state = h.state
		d     = h.drawer
	)
	if state.view != viewTypeSchedulers {
		//TODO Start data fetch and reset ticker
		state.view = viewTypeSchedulers
		d.draw(state)
	}
}

func (h *keyEventHandler) showRedisInfo() {
	var (
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.view != viewTypeRedis {
		f.fetchRedisInfo()
		state.view = viewTypeRedis
		d.draw(state)
	}
}

func (h *keyEventHandler) showHelp() {
	var (
		state = h.state
		d     = h.drawer
	)
	if state.view != viewTypeHelp {
		state.prevView = state.view
		state.view = viewTypeHelp
		d.draw(state)
	}
}
