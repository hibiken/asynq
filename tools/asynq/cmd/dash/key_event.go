// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"os"
	"time"

	"github.com/Kua-Fu/asynq"
	"github.com/gdamore/tcell/v2"
)

// keyEventHandler handles keyboard events and updates the state.
// It delegates data fetching to fetcher and UI rendering to drawer.
type keyEventHandler struct {
	s     tcell.Screen
	state *State
	done  chan struct{}

	fetcher fetcher
	drawer  drawer

	ticker       *time.Ticker
	pollInterval time.Duration
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
		f     = h.fetcher
	)
	if state.view == viewTypeHelp {
		state.view = state.prevView // exit help
		f.Fetch(state)
		h.resetTicker()
		d.Draw(state)
	} else if state.view == viewTypeQueueDetails {
		// if task modal is open close it; otherwise go back to the previous view
		if state.taskID != "" {
			state.taskID = ""
			state.selectedTask = nil
			d.Draw(state)
		} else {
			state.view = viewTypeQueues
			f.Fetch(state)
			h.resetTicker()
			d.Draw(state)
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
	h.drawer.Draw(h.state)
}

func (h *keyEventHandler) downKeyQueueDetails() {
	s, state := h.s, h.state
	if shouldShowGroupTable(state) {
		if state.groupTableRowIdx < groupPageSize(s) {
			state.groupTableRowIdx++
		} else {
			state.groupTableRowIdx = 0 // loop back
		}
	} else if state.taskID == "" {
		if state.taskTableRowIdx < len(state.tasks) {
			state.taskTableRowIdx++
		} else {
			state.taskTableRowIdx = 0 // loop back
		}
	}
	h.drawer.Draw(state)
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
	h.drawer.Draw(state)
}

func (h *keyEventHandler) upKeyQueueDetails() {
	s, state := h.s, h.state
	if shouldShowGroupTable(state) {
		if state.groupTableRowIdx == 0 {
			state.groupTableRowIdx = groupPageSize(s)
		} else {
			state.groupTableRowIdx--
		}
	} else if state.taskID == "" {
		if state.taskTableRowIdx == 0 {
			state.taskTableRowIdx = len(state.tasks)
		} else {
			state.taskTableRowIdx--
		}
	}
	h.drawer.Draw(state)
}

func (h *keyEventHandler) handleEnterKey() {
	switch h.state.view {
	case viewTypeQueues:
		h.enterKeyQueues()
	case viewTypeQueueDetails:
		h.enterKeyQueueDetails()
	}
}

func (h *keyEventHandler) resetTicker() {
	h.ticker.Reset(h.pollInterval)
}

func (h *keyEventHandler) enterKeyQueues() {
	var (
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
		f.Fetch(state)
		h.resetTicker()
		d.Draw(state)
	}
}

func (h *keyEventHandler) enterKeyQueueDetails() {
	var (
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if shouldShowGroupTable(state) && state.groupTableRowIdx != 0 {
		state.selectedGroup = state.groups[state.groupTableRowIdx-1]
		state.tasks = nil
		state.pageNum = 1
		f.Fetch(state)
		h.resetTicker()
		d.Draw(state)
	} else if !shouldShowGroupTable(state) && state.taskTableRowIdx != 0 {
		task := state.tasks[state.taskTableRowIdx-1]
		state.selectedTask = task
		state.taskID = task.ID
		f.Fetch(state)
		h.resetTicker()
		d.Draw(state)
	}

}

func (h *keyEventHandler) handleLeftKey() {
	var (
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.view == viewTypeQueueDetails && state.taskID == "" {
		state.taskState = prevTaskState(state.taskState)
		state.pageNum = 1
		state.taskTableRowIdx = 0
		state.tasks = nil
		state.selectedGroup = nil
		f.Fetch(state)
		h.resetTicker()
		d.Draw(state)
	}
}

func (h *keyEventHandler) handleRightKey() {
	var (
		state = h.state
		f     = h.fetcher
		d     = h.drawer
	)
	if state.view == viewTypeQueueDetails && state.taskID == "" {
		state.taskState = nextTaskState(state.taskState)
		state.pageNum = 1
		state.taskTableRowIdx = 0
		state.tasks = nil
		state.selectedGroup = nil
		f.Fetch(state)
		h.resetTicker()
		d.Draw(state)
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
				d.Draw(state)
			}
		} else {
			pageSize := taskPageSize(s)
			totalCount := getTaskCount(state.selectedQueue, state.taskState)
			if (state.pageNum-1)*pageSize+len(state.tasks) < totalCount {
				state.pageNum++
				f.Fetch(state)
				h.resetTicker()
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
				d.Draw(state)
			}
		} else {
			if state.pageNum > 1 {
				state.pageNum--
				f.Fetch(state)
				h.resetTicker()
			}
		}
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
		d.Draw(state)
	}
}
