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
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
)

// SortMsgOpt is a cmp.Option to sort base.TaskMessage for comparing slice of task messages.
var SortMsgOpt = cmp.Transformer("SortTaskMessages", func(in []*base.TaskMessage) []*base.TaskMessage {
	out := append([]*base.TaskMessage(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID.String() < out[j].ID.String()
	})
	return out
})

// SortZSetEntryOpt is an cmp.Option to sort ZSetEntry for comparing slice of zset entries.
var SortZSetEntryOpt = cmp.Transformer("SortZSetEntries", func(in []base.Z) []base.Z {
	out := append([]base.Z(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Message.ID.String() < out[j].Message.ID.String()
	})
	return out
})

// SortServerInfoOpt is a cmp.Option to sort base.ServerInfo for comparing slice of process info.
var SortServerInfoOpt = cmp.Transformer("SortServerInfo", func(in []*base.ServerInfo) []*base.ServerInfo {
	out := append([]*base.ServerInfo(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].PID < out[j].PID
	})
	return out
})

// SortWorkerInfoOpt is a cmp.Option to sort base.WorkerInfo for comparing slice of worker info.
var SortWorkerInfoOpt = cmp.Transformer("SortWorkerInfo", func(in []*base.WorkerInfo) []*base.WorkerInfo {
	out := append([]*base.WorkerInfo(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
})

// SortStringSliceOpt is a cmp.Option to sort string slice.
var SortStringSliceOpt = cmp.Transformer("SortStringSlice", func(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
})

// IgnoreIDOpt is an cmp.Option to ignore ID field in task messages when comparing.
var IgnoreIDOpt = cmpopts.IgnoreFields(base.TaskMessage{}, "ID")

// NewTaskMessage returns a new instance of TaskMessage given a task type and payload.
func NewTaskMessage(taskType string, payload map[string]interface{}) *base.TaskMessage {
	return &base.TaskMessage{
		ID:       uuid.New(),
		Type:     taskType,
		Queue:    base.DefaultQueueName,
		Retry:    25,
		Payload:  payload,
		Timeout:  1800, // default timeout of 30 mins
		Deadline: 0,    // no deadline
	}
}

// NewTaskMessageWithQueue returns a new instance of TaskMessage given a
// task type, payload and queue name.
func NewTaskMessageWithQueue(taskType string, payload map[string]interface{}, qname string) *base.TaskMessage {
	return &base.TaskMessage{
		ID:      uuid.New(),
		Type:    taskType,
		Queue:   qname,
		Retry:   25,
		Payload: payload,
	}
}

// TaskMessageAfterRetry returns an updated copy of t after retry.
// It increments retry count and sets the error message.
func TaskMessageAfterRetry(t base.TaskMessage, errMsg string) *base.TaskMessage {
	t.Retried = t.Retried + 1
	t.ErrorMsg = errMsg
	return &t
}

// TaskMessageWithError returns an updated copy of t with the given error message.
func TaskMessageWithError(t base.TaskMessage, errMsg string) *base.TaskMessage {
	t.ErrorMsg = errMsg
	return &t
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
func SeedEnqueuedQueue(tb testing.TB, r *redis.Client, msgs []*base.TaskMessage, qname string) {
	tb.Helper()
	r.SAdd(base.AllQueues, qname)
	seedRedisList(tb, r, base.QueueKey(qname), msgs)
}

// SeedInProgressQueue initializes the in-progress queue with the given messages.
func SeedInProgressQueue(tb testing.TB, r *redis.Client, msgs []*base.TaskMessage, qname string) {
	tb.Helper()
	seedRedisList(tb, r, base.InProgressKey(qname), msgs)
}

// SeedScheduledQueue initializes the scheduled queue with the given messages.
func SeedScheduledQueue(tb testing.TB, r *redis.Client, entries []base.Z, qname string) {
	tb.Helper()
	seedRedisZSet(tb, r, base.ScheduledKey(qname), entries)
}

// SeedRetryQueue initializes the retry queue with the given messages.
func SeedRetryQueue(tb testing.TB, r *redis.Client, entries []base.Z, qname string) {
	tb.Helper()
	seedRedisZSet(tb, r, base.RetryKey(qname), entries)
}

// SeedDeadQueue initializes the dead queue with the given messages.
func SeedDeadQueue(tb testing.TB, r *redis.Client, entries []base.Z, qname string) {
	tb.Helper()
	seedRedisZSet(tb, r, base.DeadKey(qname), entries)
}

// SeedDeadlines initializes the deadlines set with the given entries.
func SeedDeadlines(tb testing.TB, r *redis.Client, entries []base.Z, qname string) {
	tb.Helper()
	seedRedisZSet(tb, r, base.DeadlinesKey(qname), entries)
}

// SeedAllEnqueuedQueues initializes all of the specified queues with the given messages.
//
// enqueued maps a queue name to a list of messages.
func SeedAllEnqueuedQueues(tb testing.TB, r *redis.Client, enqueued map[string][]*base.TaskMessage) {
	for q, msgs := range enqueued {
		SeedEnqueuedQueue(tb, r, msgs, q)
	}
}

// SeedAllInProgressQueues initializes all of the specified in-progress queues with the given messages.
func SeedAllInProgressQueues(tb testing.TB, r *redis.Client, inprogress map[string][]*base.TaskMessage) {
	for q, msgs := range inprogress {
		SeedInProgressQueue(tb, r, msgs, q)
	}
}

// SeedAllScheduledQueues initializes all of the specified scheduled queues with the given entries.
func SeedAllScheduledQueues(tb testing.TB, r *redis.Client, scheduled map[string][]base.Z) {
	for q, entries := range scheduled {
		SeedScheduledQueue(tb, r, entries, q)
	}
}

// SeedAllRetryQueues initializes all of the specified retry queues with the given entries.
func SeedAllRetryQueues(tb testing.TB, r *redis.Client, retry map[string][]base.Z) {
	for q, entries := range retry {
		SeedRetryQueue(tb, r, entries, q)
	}
}

// SeedAllDeadQueues initializes all of the specified dead queues with the given entries.
func SeedAllDeadQueues(tb testing.TB, r *redis.Client, dead map[string][]base.Z) {
	for q, entries := range dead {
		SeedDeadQueue(tb, r, entries, q)
	}
}

// SeedAllDeadlines initializes all of the deadlines with the given entries.
func SeedAllDeadlines(tb testing.TB, r *redis.Client, deadlines map[string][]base.Z) {
	for q, entries := range deadlines {
		SeedDeadlines(tb, r, entries, q)
	}
}

func seedRedisList(tb testing.TB, c *redis.Client, key string, msgs []*base.TaskMessage) {
	data := MustMarshalSlice(tb, msgs)
	for _, s := range data {
		if err := c.LPush(key, s).Err(); err != nil {
			tb.Fatal(err)
		}
	}
}

func seedRedisZSet(tb testing.TB, c *redis.Client, key string, items []base.Z) {
	for _, item := range items {
		z := &redis.Z{Member: MustMarshal(tb, item.Message), Score: float64(item.Score)}
		if err := c.ZAdd(key, z).Err(); err != nil {
			tb.Fatal(err)
		}
	}
}

// GetEnqueuedMessages returns all enqueued messages in the given queue.
func GetEnqueuedMessages(tb testing.TB, r *redis.Client, qname string) []*base.TaskMessage {
	tb.Helper()
	return getListMessages(tb, r, base.QueueKey(qname))
}

// GetInProgressMessages returns all in-progress messages in the given queue.
func GetInProgressMessages(tb testing.TB, r *redis.Client, qname string) []*base.TaskMessage {
	tb.Helper()
	return getListMessages(tb, r, base.InProgressKey(qname))
}

// GetScheduledMessages returns all scheduled task messages in the given queue.
func GetScheduledMessages(tb testing.TB, r *redis.Client, qname string) []*base.TaskMessage {
	tb.Helper()
	return getZSetMessages(tb, r, base.ScheduledKey(qname))
}

// GetRetryMessages returns all retry messages in the given queue.
func GetRetryMessages(tb testing.TB, r *redis.Client, qname string) []*base.TaskMessage {
	tb.Helper()
	return getZSetMessages(tb, r, base.RetryKey(qname))
}

// GetDeadMessages returns all dead messages in the given queue.
func GetDeadMessages(tb testing.TB, r *redis.Client, qname string) []*base.TaskMessage {
	tb.Helper()
	return getZSetMessages(tb, r, base.DeadKey(qname))
}

// GetScheduledEntries returns all scheduled messages and its score in the given queue.
func GetScheduledEntries(tb testing.TB, r *redis.Client, qname string) []base.Z {
	tb.Helper()
	return getZSetEntries(tb, r, base.ScheduledKey(qname))
}

// GetRetryEntries returns all retry messages and its score in the given queue.
func GetRetryEntries(tb testing.TB, r *redis.Client, qname string) []base.Z {
	tb.Helper()
	return getZSetEntries(tb, r, base.RetryKey(qname))
}

// GetDeadEntries returns all dead messages and its score in the given queue.
func GetDeadEntries(tb testing.TB, r *redis.Client, qname string) []base.Z {
	tb.Helper()
	return getZSetEntries(tb, r, base.DeadKey(qname))
}

// GetDeadlinesEntries returns all task messages and its score in the deadlines set for the given queue.
func GetDeadlinesEntries(tb testing.TB, r *redis.Client, qname string) []base.Z {
	tb.Helper()
	return getZSetEntries(tb, r, base.DeadlinesKey(qname))
}

func getListMessages(tb testing.TB, r *redis.Client, list string) []*base.TaskMessage {
	data := r.LRange(list, 0, -1).Val()
	return MustUnmarshalSlice(tb, data)
}

func getZSetMessages(tb testing.TB, r *redis.Client, zset string) []*base.TaskMessage {
	data := r.ZRange(zset, 0, -1).Val()
	return MustUnmarshalSlice(tb, data)
}

func getZSetEntries(tb testing.TB, r *redis.Client, zset string) []base.Z {
	data := r.ZRangeWithScores(zset, 0, -1).Val()
	var entries []base.Z
	for _, z := range data {
		entries = append(entries, base.Z{
			Message: MustUnmarshal(tb, z.Member.(string)),
			Score:   int64(z.Score),
		})
	}
	return entries
}
