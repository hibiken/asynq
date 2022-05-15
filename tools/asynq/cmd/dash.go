// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/MakeNowJust/heredoc/v2"
	"github.com/gdamore/tcell/v2"
	"github.com/hibiken/asynq"
	"github.com/mattn/go-runewidth"
	"github.com/spf13/cobra"
)

var dashCmd = &cobra.Command{
	Use:   "dash",
	Short: "View dashboard",
	Long: heredoc.Doc(`
		Displays dashboard.`),
	Args: cobra.NoArgs,
	Run:  dash,
}

var (
	flagDebug       = false
	flagUseRealData = false
)

func init() {
	rootCmd.AddCommand(dashCmd)
	// TODO: Remove this debug once we're done
	dashCmd.Flags().BoolVar(&flagDebug, "debug", false, "Print debug info")
	dashCmd.Flags().BoolVar(&flagUseRealData, "realdata", false, "Use real data in redis")
}

// viewType is an enum for dashboard views.
type viewType int

const (
	viewTypeQueues viewType = iota
	viewTypeServers
	viewTypeSchedulers
	viewTypeRedis
	viewTypeHelp
)

type dashState struct {
	queues   []*asynq.QueueInfo
	err      error
	rowIdx   int      // highlighted row
	view     viewType // current view type
	prevView viewType // to support "go back"
}

func dash(cmd *cobra.Command, args []string) {
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
		queuesCh = make(chan []*asynq.QueueInfo)
		errorCh  = make(chan error)
	)

	go getQueueInfo(inspector, queuesCh, errorCh)

	var state dashState // contained in this goroutine only; do not share

	// draw initial screen
	drawDash(s, baseStyle, &state)

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
				if ev.Key() == tcell.KeyEscape {
					if state.view == viewTypeHelp {
						state.view = state.prevView // exit help
						drawDash(s, baseStyle, &state)
					} else {
						quit()
					}
				} else if ev.Key() == tcell.KeyCtrlC || ev.Rune() == 'q' {
					quit()
				} else if ev.Key() == tcell.KeyCtrlL {
					s.Sync()
				} else if ev.Key() == tcell.KeyDown || ev.Rune() == 'j' {
					if state.rowIdx < len(state.queues) {
						state.rowIdx++
					} else {
						state.rowIdx = 0 // loop back
					}
					drawDash(s, baseStyle, &state)
				} else if ev.Key() == tcell.KeyUp || ev.Rune() == 'k' {
					if state.rowIdx == 0 {
						state.rowIdx = len(state.queues)
					} else {
						state.rowIdx--
					}
					drawDash(s, baseStyle, &state)
				} else if ev.Rune() == '?' {
					state.prevView = state.view
					state.view = viewTypeHelp
					drawDash(s, baseStyle, &state)
				} else if ev.Key() == tcell.KeyF1 && state.view != viewTypeQueues {
					go getQueueInfo(inspector, queuesCh, errorCh)
					ticker.Reset(interval)
					state.view = viewTypeQueues
					drawDash(s, baseStyle, &state)
				} else if ev.Key() == tcell.KeyF2 && state.view != viewTypeServers {
					state.view = viewTypeServers
					drawDash(s, baseStyle, &state)
				} else if ev.Key() == tcell.KeyF3 && state.view != viewTypeSchedulers {
					state.view = viewTypeSchedulers
					drawDash(s, baseStyle, &state)
				} else if ev.Key() == tcell.KeyF4 && state.view != viewTypeRedis {
					state.view = viewTypeRedis
					drawDash(s, baseStyle, &state)
				}
			}

		case <-ticker.C:
			switch state.view {
			case viewTypeQueues:
				go getQueueInfo(inspector, queuesCh, errorCh)

				// TODO: Add more cases for other type of data
			}

		case queues := <-queuesCh:
			state.queues = queues
			state.err = nil
			drawDash(s, baseStyle, &state)

		case err := <-errorCh:
			state.err = err
			drawDash(s, baseStyle, &state)
		}
	}

}

func getQueueInfo(i *asynq.Inspector, queuesCh chan<- []*asynq.QueueInfo, errorCh chan<- error) {
	if !flagUseRealData {
		n := rand.Intn(100)
		queuesCh <- []*asynq.QueueInfo{
			{Queue: "default", Size: 1800 + n, Pending: 700 + n, Active: 300, Aggregating: 300, Scheduled: 200, Retry: 100, Archived: 200},
			{Queue: "critical", Size: 2300 + n, Pending: 1000 + n, Active: 500, Retry: 400, Completed: 400},
			{Queue: "low", Size: 900 + n, Pending: n, Active: 300, Scheduled: 400, Completed: 200},
		}
		return
	}
	queues, err := i.Queues()
	if err != nil {
		errorCh <- err
		return
	}
	var res []*asynq.QueueInfo
	for _, q := range queues {
		info, err := i.GetQueueInfo(q)
		if err != nil {
			errorCh <- err
			return
		}
		res = append(res, info)
	}
	queuesCh <- res

}

func drawDash(s tcell.Screen, style tcell.Style, state *dashState) {
	s.Clear()
	// Simulate data update on every render
	d := NewScreenDrawer(s)
	switch state.view {
	case viewTypeQueues:
		d.Println("=== Queues ===", style.Bold(true))
		d.NL() // empty line
		drawQueueSizeGraphs(d, style, state)
		d.NL() // empty line
		drawQueueTable(d, style, state)
	case viewTypeServers:
		d.Println("=== Servers ===", style.Bold(true))
		d.NL() // empty line
		// TODO: Draw body
	case viewTypeSchedulers:
		d.Println("=== Schedulers === ", style.Bold(true))
		d.NL() // empty line
		// TODO: Draw body
	case viewTypeRedis:
		d.Println("=== Redis Info === ", style.Bold(true))
		d.NL() // empty line
		// TODO: Draw body
	case viewTypeHelp:
		d.Println("=== HELP ===", style.Bold(true))
		d.NL() // empty line
		// TODO: Draw HELP body
	}
	d.GoToBottom()
	drawFooter(d, style, state)
}

func drawQueueSizeGraphs(d *ScreenDrawer, style tcell.Style, state *dashState) {
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

func drawFooter(d *ScreenDrawer, baseStyle tcell.Style, state *dashState) {
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

const colBuffer = 4 // extra buffer between columns

type columnAlignment int

const (
	alignRight columnAlignment = iota
	alignLeft
)

type column struct {
	name          string
	width         int
	align         columnAlignment
	displayValues []string // TODO: Can we use these displayValues to display stuff?
}

func newColumn(name string, align columnAlignment) *column {
	return &column{
		name:  name,
		width: runewidth.StringWidth(name),
		align: align,
	}
}

func (c *column) accommodate(v string) {
	c.displayValues = append(c.displayValues, v)
	if w := runewidth.StringWidth(v); w > c.width {
		c.width = w
	}
}

type table struct {
	cols []*column
}

// QueueInfoFormatter exposes API to return display values for QueueInfo properties.
type QueueInfoFormatter struct {
	q *asynq.QueueInfo
}

func (f *QueueInfoFormatter) Queue() string     { return f.q.Queue }
func (f *QueueInfoFormatter) Size() string      { return strconv.Itoa(f.q.Size) }
func (f *QueueInfoFormatter) Processed() string { return strconv.Itoa(f.q.Processed) }
func (f *QueueInfoFormatter) Failed() string    { return strconv.Itoa(f.q.Failed) }

func (f *QueueInfoFormatter) State() string {
	if f.q.Paused {
		return "PAUSED"
	}
	return "RUN"
}

func (f *QueueInfoFormatter) Latency() string {
	return f.q.Latency.String()
}

func (f *QueueInfoFormatter) ErrorRate() string {
	return "0.23%" // TODO: Implement this
}

func (f *QueueInfoFormatter) MemoryUsage() string {
	return ByteCount(f.q.MemoryUsage)
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

func drawQueueTable(d *ScreenDrawer, style tcell.Style, state *dashState) {
	columns := []*column{
		newColumn("Queue", alignLeft),
		newColumn("State", alignLeft),
		newColumn("Size", alignRight),
		newColumn("Latency", alignRight),
		newColumn("MemoryUsage", alignRight),
		newColumn("Processed", alignRight),
		newColumn("Failed", alignRight),
		newColumn("ErrorRate", alignRight),
	}

	// Adjust the column widths to accomodate the values
	for _, q := range state.queues {
		f := QueueInfoFormatter{q}
		for _, col := range columns {
			switch col.name {
			case "Queue":
				col.accommodate(f.Queue())
			case "State":
				col.accommodate(f.State())
			case "Size":
				col.accommodate(f.Size())
			case "MemoryUsage":
				col.accommodate(f.MemoryUsage())
			case "Latency":
				col.accommodate(f.Latency())
			case "Processed":
				col.accommodate(f.Processed())
			case "Failed":
				col.accommodate(f.Failed())
			case "ErrorRate":
				col.accommodate(f.ErrorRate())
			}
		}
	}

	// Header
	headerStyle := style.Background(tcell.ColorDimGray).Foreground(tcell.ColorWhite)
	width, _ := d.Screen().Size()
	var b strings.Builder
	for _, col := range columns {
		if col.align == alignRight {
			b.WriteString(lpad(col.name, col.width+colBuffer))
		} else {
			b.WriteString(rpad(col.name, col.width+colBuffer))
		}
	}
	b.WriteString(strings.Repeat(" ", width-b.Len())) // span the full width
	d.Println(b.String(), headerStyle)

	// Body
	for i, q := range state.queues {
		rowStyle := style
		if state.rowIdx == i+1 {
			rowStyle = style.Background(tcell.ColorDarkOliveGreen)
		}
		f := QueueInfoFormatter{q}
		for _, col := range columns {
			switch col.name {
			case "Queue":
				d.Print(rpad(f.Queue(), col.width+colBuffer), rowStyle)
			case "State":
				d.Print(rpad(f.State(), col.width+colBuffer), rowStyle)
			case "Size":
				d.Print(lpad(f.Size(), col.width+colBuffer), rowStyle)
			case "MemoryUsage":
				d.Print(lpad(f.MemoryUsage(), col.width+colBuffer), rowStyle)
			case "Latency":
				d.Print(lpad(f.Latency(), col.width+colBuffer), rowStyle)
			case "Processed":
				d.Print(lpad(f.Processed(), col.width+colBuffer), rowStyle)
			case "Failed":
				d.Print(lpad(f.Failed(), col.width+colBuffer), rowStyle)
			case "ErrorRate":
				d.Print(lpad(f.ErrorRate(), col.width+colBuffer), rowStyle)
			}
		}
		d.FillLine(' ', rowStyle)
	}

	if flagDebug {
		d.Println(fmt.Sprintf("DEBUG: rowIdx = %d", state.rowIdx), style)
		d.Println(fmt.Sprintf("DEBUG: view = %v", state.view), style)
	}
}

/*** Screen Drawer ***/

// ScreenDrawer is used to draw contents on screen.
//
// Usage example:
//    d := NewScreenDrawer(s)
//    d.Println("Hello world", mystyle)
//    d.NL() // adds newline
//    d.Print("foo", mystyle.Bold(true))
//    d.Print("bar", mystyle.Italic(true))
type ScreenDrawer struct {
	l *LineDrawer
}

func NewScreenDrawer(s tcell.Screen) *ScreenDrawer {
	return &ScreenDrawer{l: NewLineDrawer(0, s)}
}

func (d *ScreenDrawer) Print(s string, style tcell.Style) {
	d.l.Draw(s, style)
}

func (d *ScreenDrawer) Println(s string, style tcell.Style) {
	d.Print(s, style)
	d.NL()
}

// FillLine prints the given run until the end of the current line
// and adds a newline.
func (d *ScreenDrawer) FillLine(r rune, style tcell.Style) {
	w, _ := d.Screen().Size()
	s := strings.Repeat(string(r), w-d.l.col)
	d.Print(s, style)
	d.NL()
}

// NL adds a newline (i.e., moves to the next line).
func (d *ScreenDrawer) NL() {
	d.l.row++
	d.l.col = 0
}

func (d *ScreenDrawer) Screen() tcell.Screen {
	return d.l.s
}

// Go to the bottom of the screen.
func (d *ScreenDrawer) GoToBottom() {
	_, h := d.Screen().Size()
	d.l.row = h - 1
	d.l.col = 0
}

type LineDrawer struct {
	s   tcell.Screen
	row int
	col int
}

func NewLineDrawer(row int, s tcell.Screen) *LineDrawer {
	return &LineDrawer{row: row, col: 0, s: s}
}

func (d *LineDrawer) Draw(s string, style tcell.Style) {
	for _, r := range s {
		d.s.SetContent(d.col, d.row, r, nil, style)
		d.col += runewidth.RuneWidth(r)
	}
}
