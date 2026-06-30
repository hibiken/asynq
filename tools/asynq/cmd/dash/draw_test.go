// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package dash

import "testing"

func TestTruncate(t *testing.T) {
	tests := []struct {
		s    string
		max  int
		want string
	}{
		{
			s:    "hello world!",
			max:  15,
			want: "hello world!",
		},
		{
			s:    "hello world!",
			max:  6,
			want: "hello…",
		},
	}

	for _, tc := range tests {
		got := truncate(tc.s, tc.max)
		if tc.want != got {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.s, tc.max, got, tc.want)
		}
	}
}

func TestWrapLines(t *testing.T) {
	tests := []struct {
		s    string
		max  int
		want []string
	}{
		{"", 5, []string{""}},
		{"abc", 10, []string{"abc"}},
		{"abcdefghij", 4, []string{"abcd", "efgh", "ij"}},
		{"a\nb\nc", 10, []string{"a", "b", "c"}},
		{"line1\nline2", 4, []string{"line", "1", "line", "2"}},
	}
	for _, tc := range tests {
		got := wrapLines(tc.s, tc.max)
		if len(got) != len(tc.want) {
			t.Fatalf("wrapLines(%q,%d) = %v, want %v", tc.s, tc.max, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("wrapLines(%q,%d)[%d] = %q, want %q", tc.s, tc.max, i, got[i], tc.want[i])
			}
		}
	}
}

func TestClampMaxOffset(t *testing.T) {
	tests := []struct {
		total, height, want int
	}{
		{0, 10, 0},
		{5, 10, 0},  // fewer rows than height -> no scroll
		{10, 10, 0}, // exactly fits
		{12, 10, 2},
		{20, 0, 19}, // height<1 treated as 1
	}
	for _, tc := range tests {
		got := clampMaxOffset(tc.total, tc.height)
		if got != tc.want {
			t.Errorf("clampMaxOffset(%d,%d) = %d, want %d", tc.total, tc.height, got, tc.want)
		}
	}
}

func TestModalDimsFromSize(t *testing.T) {
	cw, bh := modalDimsFromSize(100, 50)
	// modalWidth=60 -> content=56; modalHeight=30 -> body=28
	if cw != 56 || bh != 28 {
		t.Errorf("modalDimsFromSize(100,50) = (%d,%d), want (56,28)", cw, bh)
	}
}
