package rdb

import (
	"encoding/json"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/rs/xid"
)

// This file defines test helpers for the rdb package testing.

func init() {
	rand.Seed(time.Now().UnixNano())
}

// TODO(hibiken): Get Redis address and db number from ENV variables.
func setup(t *testing.T) *RDB {
	t.Helper()
	r := NewRDB(redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   13,
	}))
	// Start each test with a clean slate.
	flushDB(t, r)
	return r
}

func flushDB(t *testing.T, r *RDB) {
	t.Helper()
	if err := r.client.FlushDB().Err(); err != nil {
		t.Fatal(err)
	}
}

var sortMsgOpt = cmp.Transformer("SortMsg", func(in []*TaskMessage) []*TaskMessage {
	out := append([]*TaskMessage(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID.String() < out[j].ID.String()
	})
	return out
})

var sortZSetEntryOpt = cmp.Transformer("SortZSetEntry", func(in []sortedSetEntry) []sortedSetEntry {
	out := append([]sortedSetEntry(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].msg.ID.String() < out[j].msg.ID.String()
	})
	return out
})

func newTaskMessage(taskType string, payload map[string]interface{}) *TaskMessage {
	return &TaskMessage{
		ID:      xid.New(),
		Type:    taskType,
		Queue:   "default",
		Retry:   25,
		Payload: payload,
	}
}

func mustMarshal(t *testing.T, msg *TaskMessage) string {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func mustUnmarshal(t *testing.T, data string) *TaskMessage {
	t.Helper()
	var msg TaskMessage
	err := json.Unmarshal([]byte(data), &msg)
	if err != nil {
		t.Fatal(err)
	}
	return &msg
}

func mustMarshalSlice(t *testing.T, msgs []*TaskMessage) []string {
	t.Helper()
	var data []string
	for _, m := range msgs {
		data = append(data, mustMarshal(t, m))
	}
	return data
}

func mustUnmarshalSlice(t *testing.T, data []string) []*TaskMessage {
	t.Helper()
	var msgs []*TaskMessage
	for _, s := range data {
		msgs = append(msgs, mustUnmarshal(t, s))
	}
	return msgs
}

func seedRedisList(t *testing.T, c *redis.Client, key string, msgs []*TaskMessage) {
	data := mustMarshalSlice(t, msgs)
	for _, s := range data {
		if err := c.LPush(key, s).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func seedRedisZSet(t *testing.T, c *redis.Client, key string, items []sortedSetEntry) {
	for _, item := range items {
		z := &redis.Z{Member: mustMarshal(t, item.msg), Score: float64(item.score)}
		if err := c.ZAdd(key, z).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

// scheduledEntry represents an item in redis sorted set (aka ZSET).
type sortedSetEntry struct {
	msg   *TaskMessage
	score int64
}

// seedDefaultQueue initializes the default queue with the given messages.
func seedDefaultQueue(t *testing.T, r *RDB, msgs []*TaskMessage) {
	t.Helper()
	seedRedisList(t, r.client, defaultQ, msgs)
}

// seedInProgressQueue initializes the in-progress queue with the given messages.
func seedInProgressQueue(t *testing.T, r *RDB, msgs []*TaskMessage) {
	t.Helper()
	seedRedisList(t, r.client, inProgressQ, msgs)
}

// seedScheduledQueue initializes the scheduled queue with the given messages.
func seedScheduledQueue(t *testing.T, r *RDB, entries []sortedSetEntry) {
	t.Helper()
	seedRedisZSet(t, r.client, scheduledQ, entries)
}

// seedRetryQueue initializes the retry queue with the given messages.
func seedRetryQueue(t *testing.T, r *RDB, entries []sortedSetEntry) {
	t.Helper()
	seedRedisZSet(t, r.client, retryQ, entries)
}

// seedDeadQueue initializes the dead queue with the given messages.
func seedDeadQueue(t *testing.T, r *RDB, entries []sortedSetEntry) {
	t.Helper()
	seedRedisZSet(t, r.client, deadQ, entries)
}
