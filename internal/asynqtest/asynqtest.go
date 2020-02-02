// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package asynqtest defines test helpers for asynq and its internal packages.
package asynqtest

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hibiken/asynq/internal/base"
	"github.com/rs/xid"
)

// ZSetEntry is an entry in redis sorted set.
type ZSetEntry struct {
	Msg   *base.TaskMessage
	Score float64
}

// SortMsgOpt is a cmp.Option to sort base.TaskMessage for comparing slice of task messages.
var SortMsgOpt = cmp.Transformer("SortTaskMessages", func(in []*base.TaskMessage) []*base.TaskMessage {
	out := append([]*base.TaskMessage(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID.String() < out[j].ID.String()
	})
	return out
})

// SortZSetEntryOpt is an cmp.Option to sort ZSetEntry for comparing slice of zset entries.
var SortZSetEntryOpt = cmp.Transformer("SortZSetEntries", func(in []ZSetEntry) []ZSetEntry {
	out := append([]ZSetEntry(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Msg.ID.String() < out[j].Msg.ID.String()
	})
	return out
})

// SortProcessInfoOpt is a cmp.Option to sort base.ProcessInfo for comparing slice of process info.
var SortProcessInfoOpt = cmp.Transformer("SortProcessInfo", func(in []*base.ProcessInfo) []*base.ProcessInfo {
	out := append([]*base.ProcessInfo(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].PID < out[j].PID
	})
	return out
})

// IgnoreIDOpt is an cmp.Option to ignore ID field in task messages when comparing.
var IgnoreIDOpt = cmpopts.IgnoreFields(base.TaskMessage{}, "ID")

// NewTaskMessage returns a new instance of TaskMessage given a task type and payload.
func NewTaskMessage(taskType string, payload map[string]interface{}) *base.TaskMessage {
	return &base.TaskMessage{
		ID:      xid.New(),
		Type:    taskType,
		Queue:   base.DefaultQueueName,
		Retry:   25,
		Payload: payload,
	}
}

// NewTaskMessageWithQueue returns a new instance of TaskMessage given a
// task type, payload and queue name.
func NewTaskMessageWithQueue(taskType string, payload map[string]interface{}, qname string) *base.TaskMessage {
	return &base.TaskMessage{
		ID:      xid.New(),
		Type:    taskType,
		Queue:   qname,
		Retry:   25,
		Payload: payload,
	}
}

// MustMarshal marshals given task message and returns a json string.
// Calling test will fail if marshaling errors out.
func MustMarshal(tb testing.TB, msg *base.TaskMessage) string {
	tb.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		tb.Fatal(err)
	}
	return string(data)
}

// MustUnmarshal unmarshals given string into task message struct.
// Calling test will fail if unmarshaling errors out.
func MustUnmarshal(tb testing.TB, data string) *base.TaskMessage {
	tb.Helper()
	var msg base.TaskMessage
	err := json.Unmarshal([]byte(data), &msg)
	if err != nil {
		tb.Fatal(err)
	}
	return &msg
}

// MustMarshalSlice marshals a slice of task messages and return a slice of
// json strings. Calling test will fail if marshaling errors out.
func MustMarshalSlice(tb testing.TB, msgs []*base.TaskMessage) []string {
	tb.Helper()
	var data []string
	for _, m := range msgs {
		data = append(data, MustMarshal(tb, m))
	}
	return data
}

// MustUnmarshalSlice unmarshals a slice of strings into a slice of task message structs.
// Calling test will fail if marshaling errors out.
func MustUnmarshalSlice(tb testing.TB, data []string) []*base.TaskMessage {
	tb.Helper()
	var msgs []*base.TaskMessage
	for _, s := range data {
		msgs = append(msgs, MustUnmarshal(tb, s))
	}
	return msgs
}

// FlushDB deletes all the keys of the currently selected DB.
func FlushDB(tb testing.TB, r *redis.Client) {
	tb.Helper()
	if err := r.FlushDB().Err(); err != nil {
		tb.Fatal(err)
	}
}

// SeedEnqueuedQueue initializes the specified queue with the given messages.
//
// If queue name option is not passed, it defaults to the default queue.
func SeedEnqueuedQueue(tb testing.TB, r *redis.Client, msgs []*base.TaskMessage, queueOpt ...string) {
	tb.Helper()
	queue := base.DefaultQueue
	if len(queueOpt) > 0 {
		queue = base.QueueKey(queueOpt[0])
	}
	r.SAdd(base.AllQueues, queue)
	seedRedisList(tb, r, queue, msgs)
}

// SeedInProgressQueue initializes the in-progress queue with the given messages.
func SeedInProgressQueue(tb testing.TB, r *redis.Client, msgs []*base.TaskMessage) {
	tb.Helper()
	seedRedisList(tb, r, base.InProgressQueue, msgs)
}

// SeedScheduledQueue initializes the scheduled queue with the given messages.
func SeedScheduledQueue(tb testing.TB, r *redis.Client, entries []ZSetEntry) {
	tb.Helper()
	seedRedisZSet(tb, r, base.ScheduledQueue, entries)
}

// SeedRetryQueue initializes the retry queue with the given messages.
func SeedRetryQueue(tb testing.TB, r *redis.Client, entries []ZSetEntry) {
	tb.Helper()
	seedRedisZSet(tb, r, base.RetryQueue, entries)
}

// SeedDeadQueue initializes the dead queue with the given messages.
func SeedDeadQueue(tb testing.TB, r *redis.Client, entries []ZSetEntry) {
	tb.Helper()
	seedRedisZSet(tb, r, base.DeadQueue, entries)
}

func seedRedisList(tb testing.TB, c *redis.Client, key string, msgs []*base.TaskMessage) {
	data := MustMarshalSlice(tb, msgs)
	for _, s := range data {
		if err := c.LPush(key, s).Err(); err != nil {
			tb.Fatal(err)
		}
	}
}

func seedRedisZSet(tb testing.TB, c *redis.Client, key string, items []ZSetEntry) {
	for _, item := range items {
		z := &redis.Z{Member: MustMarshal(tb, item.Msg), Score: float64(item.Score)}
		if err := c.ZAdd(key, z).Err(); err != nil {
			tb.Fatal(err)
		}
	}
}

// GetEnqueuedMessages returns all task messages in the specified queue.
//
// If queue name option is not passed, it defaults to the default queue.
func GetEnqueuedMessages(tb testing.TB, r *redis.Client, queueOpt ...string) []*base.TaskMessage {
	tb.Helper()
	queue := base.DefaultQueue
	if len(queueOpt) > 0 {
		queue = base.QueueKey(queueOpt[0])
	}
	return getListMessages(tb, r, queue)
}

// GetInProgressMessages returns all task messages in the in-progress queue.
func GetInProgressMessages(tb testing.TB, r *redis.Client) []*base.TaskMessage {
	tb.Helper()
	return getListMessages(tb, r, base.InProgressQueue)
}

// GetScheduledMessages returns all task messages in the scheduled queue.
func GetScheduledMessages(tb testing.TB, r *redis.Client) []*base.TaskMessage {
	tb.Helper()
	return getZSetMessages(tb, r, base.ScheduledQueue)
}

// GetRetryMessages returns all task messages in the retry queue.
func GetRetryMessages(tb testing.TB, r *redis.Client) []*base.TaskMessage {
	tb.Helper()
	return getZSetMessages(tb, r, base.RetryQueue)
}

// GetDeadMessages returns all task messages in the dead queue.
func GetDeadMessages(tb testing.TB, r *redis.Client) []*base.TaskMessage {
	tb.Helper()
	return getZSetMessages(tb, r, base.DeadQueue)
}

// GetScheduledEntries returns all task messages and its score in the scheduled queue.
func GetScheduledEntries(tb testing.TB, r *redis.Client) []ZSetEntry {
	tb.Helper()
	return getZSetEntries(tb, r, base.ScheduledQueue)
}

// GetRetryEntries returns all task messages and its score in the retry queue.
func GetRetryEntries(tb testing.TB, r *redis.Client) []ZSetEntry {
	tb.Helper()
	return getZSetEntries(tb, r, base.RetryQueue)
}

// GetDeadEntries returns all task messages and its score in the dead queue.
func GetDeadEntries(tb testing.TB, r *redis.Client) []ZSetEntry {
	tb.Helper()
	return getZSetEntries(tb, r, base.DeadQueue)
}

func getListMessages(tb testing.TB, r *redis.Client, list string) []*base.TaskMessage {
	data := r.LRange(list, 0, -1).Val()
	return MustUnmarshalSlice(tb, data)
}

func getZSetMessages(tb testing.TB, r *redis.Client, zset string) []*base.TaskMessage {
	data := r.ZRange(zset, 0, -1).Val()
	return MustUnmarshalSlice(tb, data)
}

func getZSetEntries(tb testing.TB, r *redis.Client, zset string) []ZSetEntry {
	data := r.ZRangeWithScores(zset, 0, -1).Val()
	var entries []ZSetEntry
	for _, z := range data {
		entries = append(entries, ZSetEntry{
			Msg:   MustUnmarshal(tb, z.Member.(string)),
			Score: z.Score,
		})
	}
	return entries
}
