// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import (
	"github.com/gdamore/tcell/v2"
	"github.com/mattn/go-runewidth"
)

type columnAlignment int

const (
	alignRight columnAlignment = iota
	alignLeft
)

type columnConfig[V any] struct {
	name      string
	alignment columnAlignment
	displayFn func(v V) string
}

type column[V any] struct {
	*columnConfig[V]
	width int
}

// Helper to draw a table.
func drawTable[V any](d *ScreenDrawer, style tcell.Style, configs []*columnConfig[V], data []V, highlightRowIdx int) {
	const colBuffer = "    " // extra buffer between columns
	cols := make([]*column[V], len(configs))
	for i, cfg := range configs {
		cols[i] = &column[V]{cfg, runewidth.StringWidth(cfg.name)}
	}
	// adjust the column width to accommodate the widest value.
	for _, v := range data {
		for _, col := range cols {
			if w := runewidth.StringWidth(col.displayFn(v)); col.width < w {
				col.width = w
			}
		}
	}
	// print header
	headerStyle := style.Background(tcell.ColorDimGray).Foreground(tcell.ColorWhite)
	for _, col := range cols {
		if col.alignment == alignLeft {
			d.Print(rpad(col.name, col.width)+colBuffer, headerStyle)
		} else {
			d.Print(lpad(col.name, col.width)+colBuffer, headerStyle)
		}
	}
	d.FillLine(' ', headerStyle)
	// print body
	for i, v := range data {
		rowStyle := style
		if highlightRowIdx == i {
			rowStyle = style.Background(tcell.ColorDarkOliveGreen)
		}
		for _, col := range cols {
			if col.alignment == alignLeft {
				d.Print(rpad(col.displayFn(v), col.width)+colBuffer, rowStyle)
			} else {
				d.Print(lpad(col.displayFn(v), col.width)+colBuffer, rowStyle)
			}
		}
		d.FillLine(' ', rowStyle)
	}
}
