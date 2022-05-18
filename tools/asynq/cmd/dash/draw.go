// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/hibiken/asynq"
	"github.com/mattn/go-runewidth"
)

func drawDash(s tcell.Screen, style tcell.Style, state *State, opts Options) {
	s.Clear()
	// Simulate data update on every render
	d := NewScreenDrawer(s)
	switch state.view {
	case viewTypeQueues:
		d.Println("=== Queues ===", style.Bold(true))
		d.NL()
		drawQueueSizeGraphs(d, style, state)
		d.NL()
		drawQueueTable(d, style, state)
	case viewTypeQueueDetails:
		d.Println("=== Queue Summary ===", style.Bold(true))
		d.NL()
		drawQueueSummary(d, style, state)
		d.NL()
		d.NL()
		d.Println("=== Tasks ===", style.Bold(true))
		d.NL()
		drawTaskStateBreakdown(d, style, state)
		d.NL()
		drawTaskTable(d, style, state)
	case viewTypeServers:
		d.Println("=== Servers ===", style.Bold(true))
		d.NL()
		// TODO: Draw body
	case viewTypeSchedulers:
		d.Println("=== Schedulers === ", style.Bold(true))
		d.NL()
		// TODO: Draw body
	case viewTypeRedis:
		d.Println("=== Redis Info === ", style.Bold(true))
		d.NL()
		d.Println(fmt.Sprintf("Version: %s", state.redisInfo.version), style)
		d.Println(fmt.Sprintf("Uptime: %s", state.redisInfo.uptime), style)
		d.Println(fmt.Sprintf("Memory Usage: %s", ByteCount(int64(state.redisInfo.memoryUsage))), style)
		d.Println(fmt.Sprintf("Peak Memory Usage: %s", ByteCount(int64(state.redisInfo.peakMemoryUsage))), style)
	case viewTypeHelp:
		d.Println("=== HELP ===", style.Bold(true))
		d.NL()
		// TODO: Draw HELP body
	}
	if opts.DebugMode {
		d.Println(fmt.Sprintf("DEBUG: rowIdx = %d", state.queueTableRowIdx), style)
		d.Println(fmt.Sprintf("DEBUG: selectedQueue = %s", state.selectedQueue.Queue), style)
		d.Println(fmt.Sprintf("DEBUG: view = %v", state.view), style)
	}
	d.GoToBottom()
	drawFooter(d, style, state)
}

func drawQueueSizeGraphs(d *ScreenDrawer, style tcell.Style, state *State) {
	var (
		activeStyle      = tcell.StyleDefault.Foreground(tcell.GetColor("blue")).Background(tcell.ColorReset)
		pendingStyle     = tcell.StyleDefault.Foreground(tcell.GetColor("green")).Background(tcell.ColorReset)
		aggregatingStyle = tcell.StyleDefault.Foreground(tcell.GetColor("lightgreen")).Background(tcell.ColorReset)
		scheduledStyle   = tcell.StyleDefault.Foreground(tcell.GetColor("yellow")).Background(tcell.ColorReset)
		retryStyle       = tcell.StyleDefault.Foreground(tcell.GetColor("pink")).Background(tcell.ColorReset)
		archivedStyle    = tcell.StyleDefault.Foreground(tcell.GetColor("purple")).Background(tcell.ColorReset)
		completedStyle   = tcell.StyleDefault.Foreground(tcell.GetColor("darkgreen")).Background(tcell.ColorReset)
	)

	var qnames []string
	var qsizes []string // queue size in strings
	maxSize := 1        // not zero to avoid division by zero
	for _, q := range state.queues {
		qnames = append(qnames, q.Queue)
		qsizes = append(qsizes, strconv.Itoa(q.Size))
		if q.Size > maxSize {
			maxSize = q.Size
		}
	}
	qnameWidth := maxwidth(qnames)
	qsizeWidth := maxwidth(qsizes)

	// Calculate the multipler to scale the graph
	screenWidth, _ := d.Screen().Size()
	graphMaxWidth := screenWidth - (qnameWidth + qsizeWidth + 3) // <qname> |<graph> <size>
	multipiler := 1.0
	if graphMaxWidth < maxSize {
		multipiler = float64(graphMaxWidth) / float64(maxSize)
	}

	const tick = '▇'
	for _, q := range state.queues {
		d.Print(q.Queue, style)
		d.Print(strings.Repeat(" ", qnameWidth-runewidth.StringWidth(q.Queue)+1), style) // padding between qname and graph
		d.Print("|", style)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Completed)*multipiler))), completedStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Archived)*multipiler))), archivedStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Retry)*multipiler))), retryStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Scheduled)*multipiler))), scheduledStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Aggregating)*multipiler))), aggregatingStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Pending)*multipiler))), pendingStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Active)*multipiler))), activeStyle)
		d.Print(fmt.Sprintf(" %d", q.Size), style)
		d.NL()
	}
	d.NL()
	d.Print("completed=", style)
	d.Print(string(tick), completedStyle)
	d.Print(" archived=", style)
	d.Print(string(tick), archivedStyle)
	d.Print(" retry=", style)
	d.Print(string(tick), retryStyle)
	d.Print(" scheduled=", style)
	d.Print(string(tick), scheduledStyle)
	d.Print(" aggregating=", style)
	d.Print(string(tick), aggregatingStyle)
	d.Print(" pending=", style)
	d.Print(string(tick), pendingStyle)
	d.Print(" active=", style)
	d.Print(string(tick), activeStyle)
	d.NL()
}

func drawFooter(d *ScreenDrawer, baseStyle tcell.Style, state *State) {
	if state.err != nil {
		style := baseStyle.Background(tcell.ColorDarkRed)
		d.Print(state.err.Error(), style)
		d.FillLine(' ', style)
		return
	}
	style := baseStyle.Background(tcell.ColorDarkSlateGray)
	switch state.view {
	case viewTypeHelp:
		d.Print("Esc=GoBack", style)
	default:
		type menu struct {
			label string
			view  viewType
		}
		menus := []*menu{
			{"F1=Queues", viewTypeQueues},
			{"F2=Servers", viewTypeServers},
			{"F3=Schedulers", viewTypeSchedulers},
			{"F4=Redis", viewTypeRedis},
			{"?=Help", viewTypeHelp},
		}
		var b strings.Builder
		for _, m := range menus {
			b.WriteString(m.label)
			// Add * for the current view
			if m.view == state.view {
				b.WriteString("* ")
			} else {
				b.WriteString("  ")
			}
		}
		d.Print(b.String(), style)
	}
	d.FillLine(' ', style)
}

// returns the maximum width from the given list of names
func maxwidth(names []string) int {
	max := 0
	for _, s := range names {
		if w := runewidth.StringWidth(s); w > max {
			max = w
		}
	}
	return max
}

// rpad adds padding to the right of a string.
func rpad(s string, padding int) string {
	tmpl := fmt.Sprintf("%%-%ds ", padding)
	return fmt.Sprintf(tmpl, s)

}

// lpad adds padding to the left of a string.
func lpad(s string, padding int) string {
	tmpl := fmt.Sprintf("%%%ds ", padding)
	return fmt.Sprintf(tmpl, s)
}

// ByteCount converts the given bytes into human readable string
func ByteCount(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)

	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++

	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

var queueColumnConfigs = []*columnConfig[*asynq.QueueInfo]{
	{"Queue", alignLeft, func(q *asynq.QueueInfo) string { return q.Queue }},
	{"State", alignLeft, func(q *asynq.QueueInfo) string {
		if q.Paused {
			return "PAUSED"
		} else {
			return "RUN"
		}
	}},
	{"Size", alignRight, func(q *asynq.QueueInfo) string { return strconv.Itoa(q.Size) }},
	{"Latency", alignRight, func(q *asynq.QueueInfo) string { return q.Latency.Round(time.Second).String() }},
	{"MemoryUsage", alignRight, func(q *asynq.QueueInfo) string { return ByteCount(q.MemoryUsage) }},
	{"Processed", alignRight, func(q *asynq.QueueInfo) string { return strconv.Itoa(q.Processed) }},
	{"Failed", alignRight, func(q *asynq.QueueInfo) string { return strconv.Itoa(q.Failed) }},
	{"ErrorRate", alignRight, func(q *asynq.QueueInfo) string { return formatErrorRate(q.Processed, q.Failed) }},
}

func formatErrorRate(processed, failed int) string {
	if processed == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", float64(failed)/float64(processed))
}

func drawQueueTable(d *ScreenDrawer, style tcell.Style, state *State) {
	drawTable(d, style, queueColumnConfigs, state.queues, state.queueTableRowIdx-1)
}

func drawQueueSummary(d *ScreenDrawer, style tcell.Style, state *State) {
	q := state.selectedQueue
	labelStyle := style.Foreground(tcell.ColorLightGray)
	d.Print("Name:     ", labelStyle)
	d.Println(q.Queue, style)
	d.Print("Size:     ", labelStyle)
	d.Println(strconv.Itoa(q.Size), style)
	d.Print("Latency   ", labelStyle)
	d.Println(q.Latency.Round(time.Second).String(), style)
	d.Print("MemUsage  ", labelStyle)
	d.Println(ByteCount(q.MemoryUsage), style)
}

func drawTaskTable(d *ScreenDrawer, style tcell.Style, state *State) {
	if state.taskState == asynq.TaskStateAggregating {
		d.Println("TODO: aggregating tasks need group name", style)
		return
	}
	if len(state.tasks) == 0 {
		return // print nothing
	}
	colConfigs := []*columnConfig[*asynq.TaskInfo]{
		{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
		{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
		{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return string(t.Payload) }},
		{"MaxRetry", alignRight, func(t *asynq.TaskInfo) string { return strconv.Itoa(t.MaxRetry) }},
		{"LastError", alignLeft, func(t *asynq.TaskInfo) string { return t.LastErr }},
	}
	drawTable(d, style, colConfigs, state.tasks, state.taskTableRowIdx-1)
}

// Define the order of states to show
var taskStates = []asynq.TaskState{
	asynq.TaskStateActive,
	asynq.TaskStatePending,
	asynq.TaskStateAggregating,
	asynq.TaskStateScheduled,
	asynq.TaskStateRetry,
	asynq.TaskStateArchived,
	asynq.TaskStateCompleted,
}

func nextTaskState(current asynq.TaskState) asynq.TaskState {
	for i, ts := range taskStates {
		if current == ts {
			if i == len(taskStates)-1 {
				return taskStates[0]
			} else {
				return taskStates[i+1]
			}
		}
	}
	panic("unkown task state")
}

func prevTaskState(current asynq.TaskState) asynq.TaskState {
	for i, ts := range taskStates {
		if current == ts {
			if i == 0 {
				return taskStates[len(taskStates)-1]
			} else {
				return taskStates[i-1]
			}
		}
	}
	panic("unkown task state")
}

func getTaskCount(queue *asynq.QueueInfo, taskState asynq.TaskState) int {
	switch taskState {
	case asynq.TaskStateActive:
		return queue.Active
	case asynq.TaskStatePending:
		return queue.Pending
	case asynq.TaskStateAggregating:
		return queue.Aggregating
	case asynq.TaskStateScheduled:
		return queue.Scheduled
	case asynq.TaskStateRetry:
		return queue.Retry
	case asynq.TaskStateArchived:
		return queue.Archived
	case asynq.TaskStateCompleted:
		return queue.Completed
	}
	panic("unkonwn task state")
}

func drawTaskStateBreakdown(d *ScreenDrawer, style tcell.Style, state *State) {
	const pad = "    " // padding between states
	for _, ts := range taskStates {
		s := style
		if state.taskState == ts {
			s = s.Bold(true).Underline(true)
		}
		d.Print(fmt.Sprintf("%s:%d", strings.Title(ts.String()), getTaskCount(state.selectedQueue, ts)), s)
		d.Print(pad, style)
	}
	d.NL()
}
