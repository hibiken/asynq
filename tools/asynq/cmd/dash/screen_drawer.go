// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/mattn/go-runewidth"
)

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

// FillLine prints the given rune until the end of the current line
// and adds a newline.
func (d *ScreenDrawer) FillLine(r rune, style tcell.Style) {
	w, _ := d.Screen().Size()
	if w-d.l.col < 0 {
		d.NL()
		return
	}
	s := strings.Repeat(string(r), w-d.l.col)
	d.Print(s, style)
	d.NL()
}

func (d *ScreenDrawer) FillUntil(r rune, style tcell.Style, limit int) {
	if d.l.col > limit {
		return // already passed the limit
	}
	s := strings.Repeat(string(r), limit-d.l.col)
	d.Print(s, style)
}

// NL adds a newline (i.e., moves to the next line).
func (d *ScreenDrawer) NL() {
	d.l.row++
	d.l.col = 0
}

func (d *ScreenDrawer) Screen() tcell.Screen {
	return d.l.s
}

// Goto moves the screendrawer to the specified cell.
func (d *ScreenDrawer) Goto(x, y int) {
	d.l.row = y
	d.l.col = x
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
