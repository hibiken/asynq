// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"os"
	"sort"
	"testing"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	h "github.com/hibiken/asynq/internal/asynqtest"
	"github.com/hibiken/asynq/internal/log"
)

// This file defines test helper functions used by
// other test files.

// redis used for package testing.
const (
	redisAddr = "localhost:6379"
	redisDB   = 14
)

var testLogger = log.NewLogger(os.Stderr)

func setup(tb testing.TB) *redis.Client {
	tb.Helper()
	r := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   redisDB,
	})
	// Start each test with a clean slate.
	h.FlushDB(tb, r)
	return r
}

var sortTaskOpt = cmp.Transformer("SortMsg", func(in []*Task) []*Task {
	out := append([]*Task(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Type < out[j].Type
	})
	return out
})
