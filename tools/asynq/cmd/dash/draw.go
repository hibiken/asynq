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
	"github.com/hibiken/asynq/tools/asynq/payload"
	"github.com/mattn/go-runewidth"
)

var (
	baseStyle  = tcell.StyleDefault.Background(tcell.ColorReset).Foreground(tcell.ColorReset)
	labelStyle = baseStyle.Foreground(tcell.ColorLightGray)

	// styles for bar graph
	activeStyle      = baseStyle.Foreground(tcell.ColorBlue)
	pendingStyle     = baseStyle.Foreground(tcell.ColorGreen)
	aggregatingStyle = baseStyle.Foreground(tcell.ColorLightGreen)
	scheduledStyle   = baseStyle.Foreground(tcell.ColorYellow)
	retryStyle       = baseStyle.Foreground(tcell.ColorPink)
	archivedStyle    = baseStyle.Foreground(tcell.ColorPurple)
	completedStyle   = baseStyle.Foreground(tcell.ColorDarkGreen)
)

// drawer draws UI with the given state.
type drawer interface {
	Draw(state *State)
}

type dashDrawer struct {
	s    tcell.Screen
	opts Options
}

func (dd *dashDrawer) Draw(state *State) {
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
		drawTaskModal(d, state)
	case viewTypeHelp:
		drawHelp(d)
	}
	d.GoToBottom()
	if opts.DebugMode {
		drawDebugInfo(d, state)
	} else {
		drawFooter(d, state)
	}
}

func drawQueueSizeGraphs(d *ScreenDrawer, state *State) {
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
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Active)*multipiler))), activeStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Pending)*multipiler))), pendingStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Aggregating)*multipiler))), aggregatingStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Scheduled)*multipiler))), scheduledStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Retry)*multipiler))), retryStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Archived)*multipiler))), archivedStyle)
		d.Print(strings.Repeat(string(tick), int(math.Floor(float64(q.Completed)*multipiler))), completedStyle)
		d.Print(fmt.Sprintf(" %d", q.Size), baseStyle)
		d.NL()
	}
	d.NL()
	d.Print("active=", baseStyle)
	d.Print(string(tick), activeStyle)
	d.Print(" pending=", baseStyle)
	d.Print(string(tick), pendingStyle)
	d.Print(" aggregating=", baseStyle)
	d.Print(string(tick), aggregatingStyle)
	d.Print(" scheduled=", baseStyle)
	d.Print(string(tick), scheduledStyle)
	d.Print(" retry=", baseStyle)
	d.Print(string(tick), retryStyle)
	d.Print(" archived=", baseStyle)
	d.Print(string(tick), archivedStyle)
	d.Print(" completed=", baseStyle)
	d.Print(string(tick), completedStyle)
	d.NL()
}

func drawFooter(d *ScreenDrawer, state *State) {
	if state.err != nil {
		style := baseStyle.Background(tcell.ColorDarkRed)
		d.Print(state.err.Error(), style)
		d.FillLine(' ', style)
		return
	}
	style := baseStyle.Background(tcell.ColorDarkSlateGray).Foreground(tcell.ColorWhite)
	switch state.view {
	case viewTypeHelp:
		d.Print("<Esc>: GoBack", style)
	case viewTypeQueueDetails:
		if state.taskID != "" {
			d.Print("<Up/Down> or <j/k>: Scroll    <Esc>/<q>: Back    <?>: Help ", style)
		} else {
			d.Print("<?>: Help    <Ctrl+C>: Exit ", style)
		}
	default:
		d.Print("<?>: Help    <Ctrl+C>: Exit ", style)
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

func formatNextProcessTime(t time.Time) string {
	now := time.Now()
	if t.Before(now) {
		return "now"
	}
	return fmt.Sprintf("in %v", (t.Sub(now).Round(time.Second)))
}

func formatPastTime(t time.Time) string {
	now := time.Now()
	if t.After(now) || t.Equal(now) {
		return "just now"
	}
	return fmt.Sprintf("%v ago", time.Since(t).Round(time.Second))
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
	d.Print("Name      ", labelStyle)
	d.Println(q.Queue, baseStyle)
	d.Print("Size      ", labelStyle)
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

func getTaskTableColumnConfig(taskState asynq.TaskState) []*columnConfig[*asynq.TaskInfo] {
	switch taskState {
	case asynq.TaskStateActive:
		return activeTaskTableColumns
	case asynq.TaskStatePending:
		return pendingTaskTableColumns
	case asynq.TaskStateAggregating:
		return aggregatingTaskTableColumns
	case asynq.TaskStateScheduled:
		return scheduledTaskTableColumns
	case asynq.TaskStateRetry:
		return retryTaskTableColumns
	case asynq.TaskStateArchived:
		return archivedTaskTableColumns
	case asynq.TaskStateCompleted:
		return completedTaskTableColumns
	}
	panic("unknown task state")
}

var activeTaskTableColumns = []*columnConfig[*asynq.TaskInfo]{
	{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
	{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
	{"Retried", alignRight, func(t *asynq.TaskInfo) string { return strconv.Itoa(t.Retried) }},
	{"Max Retry", alignRight, func(t *asynq.TaskInfo) string { return strconv.Itoa(t.MaxRetry) }},
	{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Payload, false) }},
}

var pendingTaskTableColumns = []*columnConfig[*asynq.TaskInfo]{
	{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
	{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
	{"Retried", alignRight, func(t *asynq.TaskInfo) string { return strconv.Itoa(t.Retried) }},
	{"Max Retry", alignRight, func(t *asynq.TaskInfo) string { return strconv.Itoa(t.MaxRetry) }},
	{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Payload, false) }},
}

var aggregatingTaskTableColumns = []*columnConfig[*asynq.TaskInfo]{
	{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
	{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
	{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Payload, false) }},
	{"Group", alignLeft, func(t *asynq.TaskInfo) string { return t.Group }},
}

var scheduledTaskTableColumns = []*columnConfig[*asynq.TaskInfo]{
	{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
	{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
	{"Next Process Time", alignLeft, func(t *asynq.TaskInfo) string {
		return formatNextProcessTime(t.NextProcessAt)
	}},
	{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Payload, false) }},
}

var retryTaskTableColumns = []*columnConfig[*asynq.TaskInfo]{
	{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
	{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
	{"Retry", alignRight, func(t *asynq.TaskInfo) string { return fmt.Sprintf("%d/%d", t.Retried, t.MaxRetry) }},
	{"Last Failure", alignLeft, func(t *asynq.TaskInfo) string { return t.LastErr }},
	{"Last Failure Time", alignLeft, func(t *asynq.TaskInfo) string { return formatPastTime(t.LastFailedAt) }},
	{"Next Process Time", alignLeft, func(t *asynq.TaskInfo) string {
		return formatNextProcessTime(t.NextProcessAt)
	}},
	{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Payload, false) }},
}

var archivedTaskTableColumns = []*columnConfig[*asynq.TaskInfo]{
	{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
	{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
	{"Retry", alignRight, func(t *asynq.TaskInfo) string { return fmt.Sprintf("%d/%d", t.Retried, t.MaxRetry) }},
	{"Last Failure", alignLeft, func(t *asynq.TaskInfo) string { return t.LastErr }},
	{"Last Failure Time", alignLeft, func(t *asynq.TaskInfo) string { return formatPastTime(t.LastFailedAt) }},
	{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Payload, false) }},
}

var completedTaskTableColumns = []*columnConfig[*asynq.TaskInfo]{
	{"ID", alignLeft, func(t *asynq.TaskInfo) string { return t.ID }},
	{"Type", alignLeft, func(t *asynq.TaskInfo) string { return t.Type }},
	{"Completion Time", alignLeft, func(t *asynq.TaskInfo) string { return formatPastTime(t.CompletedAt) }},
	{"Payload", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Payload, false) }},
	{"Result", alignLeft, func(t *asynq.TaskInfo) string { return payload.Format(t.Result, false) }},
}

func drawTaskTable(d *ScreenDrawer, state *State) {
	if shouldShowGroupTable(state) {
		drawGroupTable(d, state)
		return
	}
	if len(state.tasks) == 0 {
		return // print nothing
	}
	drawTable(d, baseStyle, getTaskTableColumnConfig(state.taskState), state.tasks, state.taskTableRowIdx-1)

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

	if pageSize < total {
		d.Print(fmt.Sprintf("Showing %d-%d out of %d", start+1, end, total), labelStyle)
		if end < total {
			d.Print("  n=NextPage", labelStyle)
		}
		if start > 0 {
			d.Print("  p=PrevPage", labelStyle)
		}
	}
	d.FillLine(' ', labelStyle)
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
	panic("unknown task state")
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
	panic("unknown task state")
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

func drawTaskModal(d *ScreenDrawer, state *State) {
	if state.taskID == "" {
		return
	}
	task := state.selectedTask
	if task == nil {
		// task no longer found
		withModal(d, []func(d *modalRowDrawer){
			func(d *modalRowDrawer) { d.Print("=== Task Info ===", baseStyle.Bold(true)) },
			func(d *modalRowDrawer) { d.Print("", baseStyle) },
			func(d *modalRowDrawer) {
				d.Print(fmt.Sprintf("Task %q no longer exists", state.taskID), baseStyle)
			},
		})
		return
	}

	contentWidth, bodyHeight := modalDims(d.Screen())
	rows := buildModalRows(task, contentWidth)

	offset := state.payloadScrollOffset
	if max := clampMaxOffset(len(rows), bodyHeight); offset > max {
		offset = max
	}
	if offset < 0 {
		offset = 0
	}
	end := offset + bodyHeight
	if end > len(rows) {
		end = len(rows)
	}
	withModal(d, rows[offset:end])
}

// buildModalRows builds the ordered body rows for the task modal (title, fields,
// decoded payload lines, envelope metadata, result). Width is used to wrap the
// payload. The caller windows the result by scroll offset.
func buildModalRows(task *asynq.TaskInfo, contentWidth int) []func(*modalRowDrawer) {
	res := payload.Decode(task.Payload, true)

	var rows []func(*modalRowDrawer)
	add := func(fn func(*modalRowDrawer)) { rows = append(rows, fn) }

	add(func(d *modalRowDrawer) { d.Print("=== Task Info ===", baseStyle.Bold(true)) })
	add(func(d *modalRowDrawer) { d.Print("", baseStyle) })
	add(func(d *modalRowDrawer) {
		d.Print("ID: ", labelStyle)
		d.Print(task.ID, baseStyle)
	})
	add(func(d *modalRowDrawer) {
		d.Print("Type: ", labelStyle)
		d.Print(task.Type, baseStyle)
	})
	add(func(d *modalRowDrawer) {
		d.Print("State: ", labelStyle)
		d.Print(task.State.String(), baseStyle)
	})
	add(func(d *modalRowDrawer) {
		d.Print("Queue: ", labelStyle)
		d.Print(task.Queue, baseStyle)
	})
	add(func(d *modalRowDrawer) {
		d.Print("Retry: ", labelStyle)
		d.Print(fmt.Sprintf("%d/%d", task.Retried, task.MaxRetry), baseStyle)
	})
	if task.LastErr != "" {
		add(func(d *modalRowDrawer) {
			d.Print("Last Failure: ", labelStyle)
			d.Print(task.LastErr, baseStyle)
		})
		add(func(d *modalRowDrawer) {
			d.Print("Last Failure Time: ", labelStyle)
			d.Print(fmt.Sprintf("%v (%s)", task.LastFailedAt, formatPastTime(task.LastFailedAt)), baseStyle)
		})
	}
	if !task.NextProcessAt.IsZero() {
		add(func(d *modalRowDrawer) {
			d.Print("Next Process Time: ", labelStyle)
			d.Print(fmt.Sprintf("%v (%s)", task.NextProcessAt, formatNextProcessTime(task.NextProcessAt)), baseStyle)
		})
	}
	if !task.CompletedAt.IsZero() {
		add(func(d *modalRowDrawer) {
			d.Print("Completion Time: ", labelStyle)
			d.Print(fmt.Sprintf("%v (%s)", task.CompletedAt, formatPastTime(task.CompletedAt)), baseStyle)
		})
	}

	// Payload (decoded, multi-line, wrapped to content width)
	add(func(d *modalRowDrawer) { d.Print("Payload:", labelStyle) })
	for _, line := range wrapLines(res.Payload, contentWidth-2) {
		add(func(d *modalRowDrawer) { d.Print(line, baseStyle) })
	}

	// Envelope metadata
	if res.Envelope != nil {
		appendEnvelopeRows(add, res.Envelope)
	}

	// Result (decoded, multi-line, wrapped)
	if task.Result != nil {
		add(func(d *modalRowDrawer) { d.Print("Result:", labelStyle) })
		for _, line := range wrapLines(payload.Format(task.Result, true), contentWidth-2) {
			add(func(d *modalRowDrawer) { d.Print(line, baseStyle) })
		}
	}

	return rows
}

// modalMaxOffset returns the maximum scroll offset for the task modal, given the
// current screen size and the selected task's content. Used by key handling.
func modalMaxOffset(s tcell.Screen, task *asynq.TaskInfo) int {
	contentWidth, bodyHeight := modalDims(s)
	rows := buildModalRows(task, contentWidth)
	return clampMaxOffset(len(rows), bodyHeight)
}

// appendEnvelopeRows adds envelope metadata rows to the modal body.
func appendEnvelopeRows(add func(func(*modalRowDrawer)), e *payload.Envelope) {
	add(func(d *modalRowDrawer) { d.Print("Envelope:", labelStyle) })
	add(func(d *modalRowDrawer) {
		d.Print(fmt.Sprintf("  version: %d", e.Version), baseStyle)
	})
	if e.HasSampled {
		add(func(d *modalRowDrawer) {
			d.Print(fmt.Sprintf("  sampled: %v", e.Sampled), baseStyle)
		})
	}
	if e.TraceID != "" {
		add(func(d *modalRowDrawer) { d.Print(fmt.Sprintf("  trace_id: %s", e.TraceID), baseStyle) })
	}
	if e.SpanID != "" {
		add(func(d *modalRowDrawer) { d.Print(fmt.Sprintf("  span_id: %s", e.SpanID), baseStyle) })
	}
	if e.ReplyTo != "" {
		add(func(d *modalRowDrawer) { d.Print(fmt.Sprintf("  reply_to: %s", e.ReplyTo), baseStyle) })
	}
	for k, v := range e.Meta {
		add(func(d *modalRowDrawer) { d.Print(fmt.Sprintf("  meta.%s: %s", k, v), baseStyle) })
	}
}

type modalRowDrawer struct {
	d        *ScreenDrawer
	width    int // current width occupied by content
	maxWidth int
}

// Note: s should not include newline
func (d *modalRowDrawer) Print(s string, style tcell.Style) {
	if d.width >= d.maxWidth {
		return // no longer write to this row
	}
	if d.width+runewidth.StringWidth(s) > d.maxWidth {
		s = truncate(s, d.maxWidth-d.width)
	}
	d.d.Print(s, style)
}

// wrapLines splits s into visual lines no wider than max (measured by rune width).
// Existing newlines are honored; over-long logical lines are wrapped.
func wrapLines(s string, max int) []string {
	if max <= 0 {
		return []string{s}
	}
	var out []string
	for _, line := range strings.Split(s, "\n") {
		var cur strings.Builder
		curW := 0
		for _, r := range line {
			rw := runewidth.RuneWidth(r)
			if curW+rw > max && cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
				curW = 0
			}
			cur.WriteRune(r)
			curW += rw
		}
		out = append(out, cur.String())
	}
	return out
}

// clampMaxOffset returns the largest scroll offset that still shows content,
// i.e. max(0, total-height). height<1 is treated as 1.
func clampMaxOffset(total, height int) int {
	if height < 1 {
		height = 1
	}
	if total <= height {
		return 0
	}
	return total - height
}

// modalDimsFromSize computes (contentWidth, bodyHeight) from screen size using
// the same proportions as withModal (60% width/height, 4-char horizontal padding,
// 2-char vertical border).
func modalDimsFromSize(w, h int) (contentWidth, bodyHeight int) {
	modalWidth := int(math.Floor(float64(w) * 0.6))
	modalHeight := int(math.Floor(float64(h) * 0.6))
	return modalWidth - 4, modalHeight - 2
}

func modalDims(s tcell.Screen) (contentWidth, bodyHeight int) {
	w, h := s.Size()
	return modalDimsFromSize(w, h)
}

// withModal draws a modal with the given functions row by row.
func withModal(d *ScreenDrawer, rowPrintFns []func(d *modalRowDrawer)) {
	w, h := d.Screen().Size()
	modalWidth, modalHeight := modalDimsFromSize(w, h)
	modalWidth += 4  // restore full modal width (content + horizontal padding)
	modalHeight += 2 // restore full modal height (body + vertical borders)
	rowOffset := int(math.Floor(float64(h) * 0.2)) // 20% from the top
	colOffset := int(math.Floor(float64(w) * 0.2)) // 20% from the left
	if modalHeight < 3 {
		return // no content can be shown
	}
	d.Goto(colOffset, rowOffset)
	d.Print(string(tcell.RuneULCorner), baseStyle)
	d.Print(strings.Repeat(string(tcell.RuneHLine), modalWidth-2), baseStyle)
	d.Print(string(tcell.RuneURCorner), baseStyle)
	d.NL()
	rowDrawer := modalRowDrawer{
		d:        d,
		width:    0,
		maxWidth: modalWidth - 4, /* borders + paddings */
	}
	for i := 1; i < modalHeight-1; i++ {
		d.Goto(colOffset, rowOffset+i)
		d.Print(fmt.Sprintf("%c ", tcell.RuneVLine), baseStyle)
		if i <= len(rowPrintFns) {
			rowPrintFns[i-1](&rowDrawer)
		}
		d.FillUntil(' ', baseStyle, colOffset+modalWidth-2)
		d.Print(fmt.Sprintf(" %c", tcell.RuneVLine), baseStyle)
		d.NL()
	}
	d.Goto(colOffset, rowOffset+modalHeight-1)
	d.Print(string(tcell.RuneLLCorner), baseStyle)
	d.Print(strings.Repeat(string(tcell.RuneHLine), modalWidth-2), baseStyle)
	d.Print(string(tcell.RuneLRCorner), baseStyle)
	d.NL()
}

func adjustWidth(s string, width int) string {
	sw := runewidth.StringWidth(s)
	if sw > width {
		return truncate(s, width)
	}
	var b strings.Builder
	b.WriteString(s)
	b.WriteString(strings.Repeat(" ", width-sw))
	return b.String()
}

// truncates s if s exceeds max length.
func truncate(s string, max int) string {
	if runewidth.StringWidth(s) <= max {
		return s
	}
	return string([]rune(s)[:max-1]) + "…"
}

func drawDebugInfo(d *ScreenDrawer, state *State) {
	d.Println(state.DebugString(), baseStyle)
}

func drawHelp(d *ScreenDrawer) {
	keyStyle := labelStyle.Bold(true)
	withModal(d, []func(*modalRowDrawer){
		func(d *modalRowDrawer) { d.Print("=== Help ===", baseStyle.Bold(true)) },
		func(d *modalRowDrawer) { d.Print("", baseStyle) },
		func(d *modalRowDrawer) {
			d.Print("<Enter>", keyStyle)
			d.Print("              to select", baseStyle)
		},
		func(d *modalRowDrawer) {
			d.Print("<Esc>", keyStyle)
			d.Print(" or ", baseStyle)
			d.Print("<q>", keyStyle)
			d.Print("         to go back", baseStyle)
		},
		func(d *modalRowDrawer) {
			d.Print("<UpArrow>", keyStyle)
			d.Print(" or ", baseStyle)
			d.Print("<k>", keyStyle)
			d.Print("     to move up", baseStyle)
		},
		func(d *modalRowDrawer) {
			d.Print("<DownArrow>", keyStyle)
			d.Print(" or ", baseStyle)
			d.Print("<j>", keyStyle)
			d.Print("   to move down", baseStyle)
		},
		func(d *modalRowDrawer) {
			d.Print("<LeftArrow>", keyStyle)
			d.Print(" or ", baseStyle)
			d.Print("<h>", keyStyle)
			d.Print("   to move left", baseStyle)
		},
		func(d *modalRowDrawer) {
			d.Print("<RightArrow>", keyStyle)
			d.Print(" or ", baseStyle)
			d.Print("<l>", keyStyle)
			d.Print("  to move right", baseStyle)
		},
		func(d *modalRowDrawer) {
			d.Print("<UpArrow>", keyStyle)
			d.Print("/", baseStyle)
			d.Print("<DownArrow>", keyStyle)
			d.Print(" scroll payload in task modal", baseStyle)
		},
		func(d *modalRowDrawer) {
			d.Print("<Ctrl+C>", keyStyle)
			d.Print("             to quit", baseStyle)
		},
	})
}
