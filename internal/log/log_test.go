// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package log

import (
	"bytes"
	"fmt"
	"regexp"
	"testing"
)

// regexp for timestamps
const (
	rgxdate         = `[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]`
	rgxtime         = `[0-9][0-9]:[0-9][0-9]:[0-9][0-9]`
	rgxmicroseconds = `\.[0-9][0-9][0-9][0-9][0-9][0-9]`
)

type tester struct {
	desc        string
	message     string
	wantPattern string // regexp that log output must match
}

func TestLoggerDebug(t *testing.T) {
	tests := []tester{
		{
			desc:        "without trailing newline, logger adds newline",
			message:     "hello, world!",
			wantPattern: fmt.Sprintf("^%s %s%s DEBUG: hello, world!\n$", rgxdate, rgxtime, rgxmicroseconds),
		},
		{
			desc:        "with trailing newline, logger preserves newline",
			message:     "hello, world!\n",
			wantPattern: fmt.Sprintf("^%s %s%s DEBUG: hello, world!\n$", rgxdate, rgxtime, rgxmicroseconds),
		},
	}

	for _, tc := range tests {
		var buf bytes.Buffer
		logger := NewLogger(&buf)

		logger.Debug(tc.message)

		got := buf.String()
		matched, err := regexp.MatchString(tc.wantPattern, got)
		if err != nil {
			t.Fatal("pattern did not compile:", err)
		}
		if !matched {
			t.Errorf("logger.info(%q) outputted %q, should match pattern %q",
				tc.message, got, tc.wantPattern)
		}
	}
}

func TestLoggerInfo(t *testing.T) {
	tests := []tester{
		{
			desc:        "without trailing newline, logger adds newline",
			message:     "hello, world!",
			wantPattern: fmt.Sprintf("^%s %s%s INFO: hello, world!\n$", rgxdate, rgxtime, rgxmicroseconds),
		},
		{
			desc:        "with trailing newline, logger preserves newline",
			message:     "hello, world!\n",
			wantPattern: fmt.Sprintf("^%s %s%s INFO: hello, world!\n$", rgxdate, rgxtime, rgxmicroseconds),
		},
	}

	for _, tc := range tests {
		var buf bytes.Buffer
		logger := NewLogger(&buf)

		logger.Info(tc.message)

		got := buf.String()
		matched, err := regexp.MatchString(tc.wantPattern, got)
		if err != nil {
			t.Fatal("pattern did not compile:", err)
		}
		if !matched {
			t.Errorf("logger.info(%q) outputted %q, should match pattern %q",
				tc.message, got, tc.wantPattern)
		}
	}
}

func TestLoggerWarn(t *testing.T) {
	tests := []tester{
		{
			desc:        "without trailing newline, logger adds newline",
			message:     "hello, world!",
			wantPattern: fmt.Sprintf("^%s %s%s WARN: hello, world!\n$", rgxdate, rgxtime, rgxmicroseconds),
		},
		{
			desc:        "with trailing newline, logger preserves newline",
			message:     "hello, world!\n",
			wantPattern: fmt.Sprintf("^%s %s%s WARN: hello, world!\n$", rgxdate, rgxtime, rgxmicroseconds),
		},
	}

	for _, tc := range tests {
		var buf bytes.Buffer
		logger := NewLogger(&buf)

		logger.Warn(tc.message)

		got := buf.String()
		matched, err := regexp.MatchString(tc.wantPattern, got)
		if err != nil {
			t.Fatal("pattern did not compile:", err)
		}
		if !matched {
			t.Errorf("logger.info(%q) outputted %q, should match pattern %q",
				tc.message, got, tc.wantPattern)
		}
	}
}

func TestLoggerError(t *testing.T) {
	tests := []tester{
		{
			desc:        "without trailing newline, logger adds newline",
			message:     "hello, world!",
			wantPattern: fmt.Sprintf("^%s %s%s ERROR: hello, world!\n$", rgxdate, rgxtime, rgxmicroseconds),
		},
		{
			desc:        "with trailing newline, logger preserves newline",
			message:     "hello, world!\n",
			wantPattern: fmt.Sprintf("^%s %s%s ERROR: hello, world!\n$", rgxdate, rgxtime, rgxmicroseconds),
		},
	}

	for _, tc := range tests {
		var buf bytes.Buffer
		logger := NewLogger(&buf)

		logger.Error(tc.message)

		got := buf.String()
		matched, err := regexp.MatchString(tc.wantPattern, got)
		if err != nil {
			t.Fatal("pattern did not compile:", err)
		}
		if !matched {
			t.Errorf("logger.info(%q) outputted %q, should match pattern %q",
				tc.message, got, tc.wantPattern)
		}
	}
}
