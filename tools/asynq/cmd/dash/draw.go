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

// drawer draws UI with the given state.
type drawer interface {
	draw(state *State)
}

type dashDrawer struct {
	s    tcell.Screen
	opts Options
}

func (dd *dashDrawer) draw(state *State) {
	s, opts := dd.s, dd.opts
	s.Clear()
	// Simulate data update on every render
	d := NewScreenDrawer(s)
	switch state.view {
	case viewTypeQueues:
		d.Println("=== Queues ===", baseStyle.Bold(true))
		d.NL()
		drawQueueSizeGraphs(d, state)
		d.NL()
		drawQueueTable(d, baseStyle, state)
	case viewTypeQueueDetails:
		d.Println("=== Queue Summary ===", baseStyle.Bold(true))
		d.NL()
		drawQueueSummary(d, state)
		d.NL()
		d.NL()
		d.Println("=== Tasks ===", baseStyle.Bold(true))
		d.NL()
		drawTaskStateBreakdown(d, baseStyle, state)
		d.NL()
		drawTaskTable(d, state)
	case viewTypeServers:
		d.Println("=== Servers ===", baseStyle.Bold(true))
		d.NL()
		// TODO: Draw body
	case viewTypeSchedulers:
		d.Println("=== Schedulers === ", baseStyle.Bold(true))
		d.NL()
		// TODO: Draw body
	case viewTypeRedis:
		d.Println("=== Redis Info === ", baseStyle.Bold(true))
		d.NL()
		d.Println(fmt.Sprintf("Version: %s", state.redisInfo.version), baseStyle)
		d.Println(fmt.Sprintf("Uptime: %s", state.redisInfo.uptime), baseStyle)
		d.Println(fmt.Sprintf("Memory Usage: %s", byteCount(int64(state.redisInfo.memoryUsage))), baseStyle)
		d.Println(fmt.Sprintf("Peak Memory Usage: %s", byteCount(int64(state.redisInfo.peakMemoryUsage))), baseStyle)
	case viewTypeHelp:
		d.Println("=== HELP ===", baseStyle.Bold(true))
		d.NL()
		// TODO: Draw HELP body
	}
	if opts.DebugMode {
		d.Println(fmt.Sprintf("DEBUG: rowIdx = %d", state.queueTableRowIdx), baseStyle)
		d.Println(fmt.Sprintf("DEBUG: selectedQueue = %s", state.selectedQueue.Queue), baseStyle)
		d.Println(fmt.Sprintf("DEBUG: view = %v", state.view), baseStyle)
	}
	d.GoToBottom()
	drawFooter(d, state)
}

func drawQueueSizeGraphs(d *ScreenDrawer, state *State) {
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
		d.Print(q.Queue, baseStyle)
		d.Print(strings.Repeat(" ", qnameWidth-runewidth.StringWidth(q.Queue)+1), baseStyle) // padding between qname and graph
		d.Print("|", baseStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Completed)*multipiler))), completedStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Archived)*multipiler))), archivedStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Retry)*multipiler))), retryStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Scheduled)*multipiler))), scheduledStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Aggregating)*multipiler))), aggregatingStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Pending)*multipiler))), pendingStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Active)*multipiler))), activeStyle)
		d.Print(fmt.Sprintf(" %d", q.Size), baseStyle)
		d.NL()
	}
	d.NL()
	d.Print("completed=", baseStyle)
	d.Print(string(tick), completedStyle)
	d.Print(" archived=", baseStyle)
	d.Print(string(tick), archivedStyle)
	d.Print(" retry=", baseStyle)
	d.Print(string(tick), retryStyle)
	d.Print(" scheduled=", baseStyle)
	d.Print(string(tick), scheduledStyle)
	d.Print(" aggregating=", baseStyle)
	d.Print(string(tick), aggregatingStyle)
	d.Print(" pending=", baseStyle)
	d.Print(string(tick), pendingStyle)
	d.Print(" active=", baseStyle)
	d.Print(string(tick), activeStyle)
	d.NL()
}

func drawFooter(d *ScreenDrawer, state *State) {
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

// byteCount converts the given bytes into human readable string
func byteCount(b int64) string {
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
	{"State", alignLeft, func(q *asynq.QueueInfo) string { return formatQueueState(q) }},
	{"Size", alignRight, func(q *asynq.QueueInfo) string { return strconv.Itoa(q.Size) }},
	{"Latency", alignRight, func(q *asynq.QueueInfo) string { return q.Latency.Round(time.Second).String() }},
	{"MemoryUsage", alignRight, func(q *asynq.QueueInfo) string { return byteCount(q.MemoryUsage) }},
	{"Processed", alignRight, func(q *asynq.QueueInfo) string { return strconv.Itoa(q.Processed) }},
	{"Failed", alignRight, func(q *asynq.QueueInfo) string { return strconv.Itoa(q.Failed) }},
	{"ErrorRate", alignRight, func(q *asynq.QueueInfo) string { return formatErrorRate(q.Processed, q.Failed) }},
}

func formatQueueState(q *asynq.QueueInfo) string {
	if q.Paused {
		return "PAUSED"
	}
	return "RUN"
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

func drawQueueSummary(d *ScreenDrawer, state *State) {
	q := state.selectedQueue
	if q == nil {
		d.Println("ERROR: Press q to go back", baseStyle)
		return
	}
	labelStyle := baseStyle.Foreground(tcell.ColorLightGray)
	d.Print("Name:     ", labelStyle)
	d.Println(q.Queue, baseStyle)
	d.Print("Size:     ", labelStyle)
	d.Println(strconv.Itoa(q.Size), baseStyle)
	d.Print("Latency   ", labelStyle)
	d.Println(q.Latency.Round(time.Second).String(), baseStyle)
	d.Print("MemUsage  ", labelStyle)
	d.Println(byteCount(q.MemoryUsage), baseStyle)
}

// Returns the max number of groups that can be displayed.
func groupPageSize(s tcell.Screen) int {
	_, h := s.Size()
	return h - 16 // height - (# of rows used)
}

// Returns the number of tasks to fetch.
func taskPageSize(s tcell.Screen) int {
	_, h := s.Size()
	return h - 15 // height - (# of rows used)
}

func shouldShowGroupTable(state *State) bool {
	return state.taskState == asynq.TaskStateAggregating && state.selectedGroup == nil
}

func drawTaskTable(d *ScreenDrawer, state *State) {
	if shouldShowGroupTable(state) {
		drawGroupTable(d, state)
		return
	}
	if len(state.tasks) == 0 {
		return // print nothing
	}
	// TODO: colConfigs should be different for each state
	colConfigs := []*columnConfig[*asynq.TaskInfo]{
		{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
		{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
		{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return string(t.Payload) }},
		{"MaxRetry", alignRight, func(t *asynq.TaskInfo) string { return strconv.Itoa(t.MaxRetry) }},
		{"LastError", alignLeft, func(t *asynq.TaskInfo) string { return t.LastErr }},
	}
	drawTable(d, baseStyle, colConfigs, state.tasks, state.taskTableRowIdx-1)

	// Pagination
	pageSize := taskPageSize(d.Screen())
	totalCount := getTaskCount(state.selectedQueue, state.taskState)
	if state.taskState == asynq.TaskStateAggregating {
		// aggregating tasks are scoped to each group when shown in the table.
		totalCount = state.selectedGroup.Size
	}
	if pageSize < totalCount {
		start := (state.pageNum-1)*pageSize + 1
		end := start + len(state.tasks) - 1
		paginationStyle := baseStyle.Foreground(tcell.ColorLightGray)
		d.Print(fmt.Sprintf("Showing %d-%d out of %d", start, end, totalCount), paginationStyle)
		if isNextTaskPageAvailable(d.Screen(), state) {
			d.Print("  n=NextPage", paginationStyle)
		}
		if state.pageNum > 1 {
			d.Print("  p=PrevPage", paginationStyle)
		}
		d.FillLine(' ', paginationStyle)
	}
}

func isNextTaskPageAvailable(s tcell.Screen, state *State) bool {
	totalCount := getTaskCount(state.selectedQueue, state.taskState)
	end := (state.pageNum-1)*taskPageSize(s) + len(state.tasks)
	return end < totalCount
}

func drawGroupTable(d *ScreenDrawer, state *State) {
	if len(state.groups) == 0 {
		return // print nothing
	}
	d.Println("<<< Select group >>>", baseStyle)
	colConfigs := []*columnConfig[*asynq.GroupInfo]{
		{"Name", alignLeft, func(g *asynq.GroupInfo) string { return g.Group }},
		{"Size", alignRight, func(g *asynq.GroupInfo) string { return strconv.Itoa(g.Size) }},
	}
	// pagination
	pageSize := groupPageSize(d.Screen())
	total := len(state.groups)
	start := (state.pageNum - 1) * pageSize
	end := min(start+pageSize, total)
	drawTable(d, baseStyle, colConfigs, state.groups[start:end], state.groupTableRowIdx-1)

	footerStyle := baseStyle.Foreground(tcell.ColorLightGray)
	if pageSize < total {
		d.Print(fmt.Sprintf("Showing %d-%d out of %d", start+1, end, total), footerStyle)
		if end < total {
			d.Print("  n=NextPage", footerStyle)
		}
		if start > 0 {
			d.Print("  p=PrevPage", footerStyle)
		}
	}
	d.FillLine(' ', footerStyle)
}

type number interface {
	int | int64 | float64
}

// min returns the smaller of x and y. if x==y, returns x
func min[V number](x, y V) V {
	if x > y {
		return y
	}
	return x
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
