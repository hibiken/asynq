// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/goleak"
)

func TestBackground(t *testing.T) {
	// https://github.com/go-redis/redis/issues/1029
	ignoreOpt := goleak.IgnoreTopFunction("github.com/go-redis/redis/v7/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNoLeaks(t, ignoreOpt)

	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	client := NewClient(r)
	bg := NewBackground(r, &Config{
		Concurrency: 10,
	})

	// no-op handler
	h := func(task *Task) error {
		return nil
	}

	bg.start(HandlerFunc(h))

	client.Schedule(NewTask("send_email", map[string]interface{}{"recipient_id": 123}), time.Now())

	client.Schedule(NewTask("send_email", map[string]interface{}{"recipient_id": 456}), time.Now().Add(time.Hour))

	bg.stop()
}

func TestGCD(t *testing.T) {
	tests := []struct {
		input []uint
		want  uint
	}{
		{[]uint{6, 2, 12}, 2},
		{[]uint{3, 3, 3}, 3},
		{[]uint{6, 3, 1}, 1},
		{[]uint{1}, 1},
		{[]uint{1, 0, 2}, 1},
		{[]uint{8, 0, 4}, 4},
		{[]uint{9, 12, 18, 30}, 3},
	}

	for _, tc := range tests {
		got := gcd(tc.input...)
		if got != tc.want {
			t.Errorf("gcd(%v) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestNormalizeQueueCfg(t *testing.T) {
	tests := []struct {
		input map[string]uint
		want  map[string]uint
	}{
		{
			input: map[string]uint{
				"high":    100,
				"default": 20,
				"low":     5,
			},
			want: map[string]uint{
				"high":    20,
				"default": 4,
				"low":     1,
			},
		},
		{
			input: map[string]uint{
				"default": 10,
			},
			want: map[string]uint{
				"default": 1,
			},
		},
		{
			input: map[string]uint{
				"critical": 5,
				"default":  1,
			},
			want: map[string]uint{
				"critical": 5,
				"default":  1,
			},
		},
		{
			input: map[string]uint{
				"critical": 6,
				"default":  3,
				"low":      0,
			},
			want: map[string]uint{
				"critical": 2,
				"default":  1,
				"low":      0,
			},
		},
	}

	for _, tc := range tests {
		got := normalizeQueueCfg(tc.input)
		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("normalizeQueueCfg(%v) = %v, want %v; (-want, +got):\n%s",
				tc.input, got, tc.want, diff)
		}
	}
}
