// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
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

var testLogger = log.NewLogger(nil)

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

func TestParseRedisURI(t *testing.T) {
	tests := []struct {
		uri  string
		want RedisConnOpt
	}{
		{
			"redis://localhost:6379",
			RedisClientOpt{Addr: "localhost:6379"},
		},
		{
			"redis://localhost:6379/3",
			RedisClientOpt{Addr: "localhost:6379", DB: 3},
		},
		{
			"redis://:mypassword@localhost:6379",
			RedisClientOpt{Addr: "localhost:6379", Password: "mypassword"},
		},
		{
			"redis://:mypassword@127.0.0.1:6379/11",
			RedisClientOpt{Addr: "127.0.0.1:6379", Password: "mypassword", DB: 11},
		},
		{
			"redis-socket:///var/run/redis/redis.sock",
			RedisClientOpt{Network: "unix", Addr: "/var/run/redis/redis.sock"},
		},
		{
			"redis-socket://:mypassword@/var/run/redis/redis.sock",
			RedisClientOpt{Network: "unix", Addr: "/var/run/redis/redis.sock", Password: "mypassword"},
		},
		{
			"redis-socket:///var/run/redis/redis.sock?db=7",
			RedisClientOpt{Network: "unix", Addr: "/var/run/redis/redis.sock", DB: 7},
		},
		{
			"redis-socket://:mypassword@/var/run/redis/redis.sock?db=12",
			RedisClientOpt{Network: "unix", Addr: "/var/run/redis/redis.sock", Password: "mypassword", DB: 12},
		},
		{
			"redis-sentinel://localhost:5000,localhost:5001,localhost:5002?master=mymaster",
			RedisFailoverClientOpt{
				MasterName:    "mymaster",
				SentinelAddrs: []string{"localhost:5000", "localhost:5001", "localhost:5002"},
			},
		},
		{
			"redis-sentinel://:mypassword@localhost:5000,localhost:5001,localhost:5002?master=mymaster",
			RedisFailoverClientOpt{
				MasterName:    "mymaster",
				SentinelAddrs: []string{"localhost:5000", "localhost:5001", "localhost:5002"},
				Password:      "mypassword",
			},
		},
	}

	for _, tc := range tests {
		got, err := ParseRedisURI(tc.uri)
		if err != nil {
			t.Errorf("ParseRedisURI(%q) returned an error: %v", tc.uri, err)
			continue
		}

		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("ParseRedisURI(%q) = %+v, want %+v\n(-want,+got)\n%s", tc.uri, got, tc.want, diff)
		}
	}
}

func TestParseRedisURIErrors(t *testing.T) {
	tests := []struct {
		desc string
		uri  string
	}{
		{
			"unsupported scheme",
			"rdb://localhost:6379",
		},
		{
			"missing scheme",
			"localhost:6379",
		},
		{
			"multiple db numbers",
			"redis://localhost:6379/1,2,3",
		},
		{
			"missing path for socket connection",
			"redis-socket://?db=one",
		},
		{
			"non integer for db numbers for socket",
			"redis-socket:///some/path/to/redis?db=one",
		},
	}

	for _, tc := range tests {
		_, err := ParseRedisURI(tc.uri)
		if err == nil {
			t.Errorf("%s: ParseRedisURI(%q) succeeded for malformed input, want error",
				tc.desc, tc.uri)
		}
	}
}
