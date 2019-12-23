package base

import (
	"testing"
	"time"
)

func TestProcessedKey(t *testing.T) {
	tests := []struct {
		input time.Time
		want  string
	}{
		{time.Date(2019, 11, 14, 10, 30, 1, 1, time.UTC), "asynq:processed:2019-11-14"},
		{time.Date(2020, 12, 1, 1, 0, 1, 1, time.UTC), "asynq:processed:2020-12-01"},
		{time.Date(2020, 1, 6, 15, 02, 1, 1, time.UTC), "asynq:processed:2020-01-06"},
	}

	for _, tc := range tests {
		got := ProcessedKey(tc.input)
		if got != tc.want {
			t.Errorf("ProcessedKey(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestFailureKey(t *testing.T) {
	tests := []struct {
		input time.Time
		want  string
	}{
		{time.Date(2019, 11, 14, 10, 30, 1, 1, time.UTC), "asynq:failure:2019-11-14"},
		{time.Date(2020, 12, 1, 1, 0, 1, 1, time.UTC), "asynq:failure:2020-12-01"},
		{time.Date(2020, 1, 6, 15, 02, 1, 1, time.UTC), "asynq:failure:2020-01-06"},
	}

	for _, tc := range tests {
		got := FailureKey(tc.input)
		if got != tc.want {
			t.Errorf("FailureKey(%v) = %q, want %q", tc.input, got, tc.want)
		}
	}
}
