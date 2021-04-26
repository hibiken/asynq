// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package asynqtest defines test helpers for asynq and its internal packages.
package asynqtest

import (
	"encoding/json"
	"math"
	"sort"
	"testing"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
)

// EquateInt64Approx returns a Comparer option that treats int64 values
// to be equal if they are within the given margin.
func EquateInt64Approx(margin int64) cmp.Option {
	return cmp.Comparer(func(a, b int64) bool {
		return math.Abs(float64(a-b)) <= float64(margin)
	})
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

// SortSchedulerEntryOpt is a cmp.Option to sort base.SchedulerEntry for comparing slice of entries.
var SortSchedulerEntryOpt = cmp.Transformer("SortSchedulerEntry", func(in []*base.SchedulerEntry) []*base.SchedulerEntry {
	out := append([]*base.SchedulerEntry(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Spec < out[j].Spec
	})
	return out
})

// SortSchedulerEnqueueEventOpt is a cmp.Option to sort base.SchedulerEnqueueEvent for comparing slice of events.
var SortSchedulerEnqueueEventOpt = cmp.Transformer("SortSchedulerEnqueueEvent", func(in []*base.SchedulerEnqueueEvent) []*base.SchedulerEnqueueEvent {
	out := append([]*base.SchedulerEnqueueEvent(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].EnqueuedAt.Unix() < out[j].EnqueuedAt.Unix()
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
func NewTaskMessage(taskType string, payload []byte) *base.TaskMessage {
	return NewTaskMessageWithQueue(taskType, payload, base.DefaultQueueName)
}

// NewTaskMessageWithQueue returns a new instance of TaskMessage given a
// task type, payload and queue name.
func NewTaskMessageWithQueue(taskType string, payload []byte, qname string) *base.TaskMessage {
	return &base.TaskMessage{
		ID:       uuid.New(),
		Type:     taskType,
		Queue:    qname,
		Retry:    25,
		Payload:  payload,
		Timeout:  1800, // default timeout of 30 mins
		Deadline: 0,    // no deadline
	}
}

// JSON serializes the given key-value pairs into stream of bytes in JSON.
func JSON(kv map[string]interface{}) []byte {
	b, err := json.Marshal(kv)
	if err != nil {
		panic(err)
	}
	return b
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
	data, err := base.EncodeMessage(msg)
	if err != nil {
		tb.Fatal(err)
	}
	return string(data)
}

// MustUnmarshal unmarshals given string into task message struct.
// Calling test will fail if unmarshaling errors out.
func MustUnmarshal(tb testing.TB, data string) *base.TaskMessage {
	tb.Helper()
	msg, err := base.DecodeMessage([]byte(data))
	if err != nil {
		tb.Fatal(err)
	}
	return msg
}

// FlushDB deletes all the keys of the currently selected DB.
func FlushDB(tb testing.TB, r redis.UniversalClient) {
	tb.Helper()
	switch r := r.(type) {
	case *redis.Client:
		if err := r.FlushDB().Err(); err != nil {
			tb.Fatal(err)
		}
	case *redis.ClusterClient:
		err := r.ForEachMaster(func(c *redis.Client) error {
			if err := c.FlushAll().Err(); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			tb.Fatal(err)
		}
	}
}

// SeedPendingQueue initializes the specified queue with the given messages.
func SeedPendingQueue(tb testing.TB, r redis.UniversalClient, msgs []*base.TaskMessage, qname string) {
	tb.Helper()
	r.SAdd(base.AllQueues, qname)
	seedRedisList(tb, r, base.PendingKey(qname), msgs, "pending")
}

// SeedActiveQueue initializes the active queue with the given messages.
func SeedActiveQueue(tb testing.TB, r redis.UniversalClient, msgs []*base.TaskMessage, qname string) {
	tb.Helper()
	r.SAdd(base.AllQueues, qname)
	seedRedisList(tb, r, base.ActiveKey(qname), msgs, "active")
}

// SeedScheduledQueue initializes the scheduled queue with the given messages.
func SeedScheduledQueue(tb testing.TB, r redis.UniversalClient, entries []base.Z, qname string) {
	tb.Helper()
	r.SAdd(base.AllQueues, qname)
	seedRedisZSet(tb, r, base.ScheduledKey(qname), entries, "scheduled")
}

// SeedRetryQueue initializes the retry queue with the given messages.
func SeedRetryQueue(tb testing.TB, r redis.UniversalClient, entries []base.Z, qname string) {
	tb.Helper()
	r.SAdd(base.AllQueues, qname)
	seedRedisZSet(tb, r, base.RetryKey(qname), entries, "retry")
}

// SeedArchivedQueue initializes the archived queue with the given messages.
func SeedArchivedQueue(tb testing.TB, r redis.UniversalClient, entries []base.Z, qname string) {
	tb.Helper()
	r.SAdd(base.AllQueues, qname)
	seedRedisZSet(tb, r, base.ArchivedKey(qname), entries, "archived")
}

// SeedDeadlines initializes the deadlines set with the given entries.
func SeedDeadlines(tb testing.TB, r redis.UniversalClient, entries []base.Z, qname string) {
	tb.Helper()
	r.SAdd(base.AllQueues, qname)
	seedRedisZSet(tb, r, base.DeadlinesKey(qname), entries, "active")
}

// SeedAllPendingQueues initializes all of the specified queues with the given messages.
//
// pending maps a queue name to a list of messages.
func SeedAllPendingQueues(tb testing.TB, r redis.UniversalClient, pending map[string][]*base.TaskMessage) {
	tb.Helper()
	for q, msgs := range pending {
		SeedPendingQueue(tb, r, msgs, q)
	}
}

// SeedAllActiveQueues initializes all of the specified active queues with the given messages.
func SeedAllActiveQueues(tb testing.TB, r redis.UniversalClient, active map[string][]*base.TaskMessage) {
	tb.Helper()
	for q, msgs := range active {
		SeedActiveQueue(tb, r, msgs, q)
	}
}

// SeedAllScheduledQueues initializes all of the specified scheduled queues with the given entries.
func SeedAllScheduledQueues(tb testing.TB, r redis.UniversalClient, scheduled map[string][]base.Z) {
	tb.Helper()
	for q, entries := range scheduled {
		SeedScheduledQueue(tb, r, entries, q)
	}
}

// SeedAllRetryQueues initializes all of the specified retry queues with the given entries.
func SeedAllRetryQueues(tb testing.TB, r redis.UniversalClient, retry map[string][]base.Z) {
	tb.Helper()
	for q, entries := range retry {
		SeedRetryQueue(tb, r, entries, q)
	}
}

// SeedAllArchivedQueues initializes all of the specified archived queues with the given entries.
func SeedAllArchivedQueues(tb testing.TB, r redis.UniversalClient, archived map[string][]base.Z) {
	tb.Helper()
	for q, entries := range archived {
		SeedArchivedQueue(tb, r, entries, q)
	}
}

// SeedAllDeadlines initializes all of the deadlines with the given entries.
func SeedAllDeadlines(tb testing.TB, r redis.UniversalClient, deadlines map[string][]base.Z) {
	tb.Helper()
	for q, entries := range deadlines {
		SeedDeadlines(tb, r, entries, q)
	}
}

func seedRedisList(tb testing.TB, c redis.UniversalClient, key string,
	msgs []*base.TaskMessage, state string) {
	tb.Helper()
	for _, msg := range msgs {
		encoded := MustMarshal(tb, msg)
		if err := c.LPush(key, msg.ID.String()).Err(); err != nil {
			tb.Fatal(err)
		}
		key := base.TaskKey(msg.Queue, msg.ID.String())
		data := map[string]interface{}{
			"msg":      encoded,
			"state":    state,
			"timeout":  msg.Timeout,
			"deadline": msg.Deadline,
		}
		if err := c.HSet(key, data).Err(); err != nil {
			tb.Fatal(err)
		}
	}
}

func seedRedisZSet(tb testing.TB, c redis.UniversalClient, key string,
	items []base.Z, state string) {
	tb.Helper()
	for _, item := range items {
		msg := item.Message
		encoded := MustMarshal(tb, msg)
		z := &redis.Z{Member: msg.ID.String(), Score: float64(item.Score)}
		if err := c.ZAdd(key, z).Err(); err != nil {
			tb.Fatal(err)
		}
		key := base.TaskKey(msg.Queue, msg.ID.String())
		data := map[string]interface{}{
			"msg":      encoded,
			"state":    state,
			"timeout":  msg.Timeout,
			"deadline": msg.Deadline,
		}
		if err := c.HSet(key, data).Err(); err != nil {
			tb.Fatal(err)
		}
	}
}

// GetPendingMessages returns all pending messages in the given queue.
// It also asserts the state field of the task.
func GetPendingMessages(tb testing.TB, r redis.UniversalClient, qname string) []*base.TaskMessage {
	tb.Helper()
	return getMessagesFromList(tb, r, qname, base.PendingKey, "pending")
}

// GetActiveMessages returns all active messages in the given queue.
// It also asserts the state field of the task.
func GetActiveMessages(tb testing.TB, r redis.UniversalClient, qname string) []*base.TaskMessage {
	tb.Helper()
	return getMessagesFromList(tb, r, qname, base.ActiveKey, "active")
}

// GetScheduledMessages returns all scheduled task messages in the given queue.
// It also asserts the state field of the task.
func GetScheduledMessages(tb testing.TB, r redis.UniversalClient, qname string) []*base.TaskMessage {
	tb.Helper()
	return getMessagesFromZSet(tb, r, qname, base.ScheduledKey, "scheduled")
}

// GetRetryMessages returns all retry messages in the given queue.
// It also asserts the state field of the task.
func GetRetryMessages(tb testing.TB, r redis.UniversalClient, qname string) []*base.TaskMessage {
	tb.Helper()
	return getMessagesFromZSet(tb, r, qname, base.RetryKey, "retry")
}

// GetArchivedMessages returns all archived messages in the given queue.
// It also asserts the state field of the task.
func GetArchivedMessages(tb testing.TB, r redis.UniversalClient, qname string) []*base.TaskMessage {
	tb.Helper()
	return getMessagesFromZSet(tb, r, qname, base.ArchivedKey, "archived")
}

// GetScheduledEntries returns all scheduled messages and its score in the given queue.
// It also asserts the state field of the task.
func GetScheduledEntries(tb testing.TB, r redis.UniversalClient, qname string) []base.Z {
	tb.Helper()
	return getMessagesFromZSetWithScores(tb, r, qname, base.ScheduledKey, "scheduled")
}

// GetRetryEntries returns all retry messages and its score in the given queue.
// It also asserts the state field of the task.
func GetRetryEntries(tb testing.TB, r redis.UniversalClient, qname string) []base.Z {
	tb.Helper()
	return getMessagesFromZSetWithScores(tb, r, qname, base.RetryKey, "retry")
}

// GetArchivedEntries returns all archived messages and its score in the given queue.
// It also asserts the state field of the task.
func GetArchivedEntries(tb testing.TB, r redis.UniversalClient, qname string) []base.Z {
	tb.Helper()
	return getMessagesFromZSetWithScores(tb, r, qname, base.ArchivedKey, "archived")
}

// GetDeadlinesEntries returns all task messages and its score in the deadlines set for the given queue.
// It also asserts the state field of the task.
func GetDeadlinesEntries(tb testing.TB, r redis.UniversalClient, qname string) []base.Z {
	tb.Helper()
	return getMessagesFromZSetWithScores(tb, r, qname, base.DeadlinesKey, "active")
}

// Retrieves all messages stored under `keyFn(qname)` key in redis list.
func getMessagesFromList(tb testing.TB, r redis.UniversalClient, qname string,
	keyFn func(qname string) string, state string) []*base.TaskMessage {
	tb.Helper()
	ids := r.LRange(keyFn(qname), 0, -1).Val()
	var msgs []*base.TaskMessage
	for _, id := range ids {
		taskKey := base.TaskKey(qname, id)
		data := r.HGet(taskKey, "msg").Val()
		msgs = append(msgs, MustUnmarshal(tb, data))
		if gotState := r.HGet(taskKey, "state").Val(); gotState != state {
			tb.Errorf("task (id=%q) is in %q state, want %q", id, gotState, state)
		}
	}
	return msgs
}

// Retrieves all messages stored under `keyFn(qname)` key in redis zset (sorted-set).
func getMessagesFromZSet(tb testing.TB, r redis.UniversalClient, qname string,
	keyFn func(qname string) string, state string) []*base.TaskMessage {
	tb.Helper()
	ids := r.ZRange(keyFn(qname), 0, -1).Val()
	var msgs []*base.TaskMessage
	for _, id := range ids {
		taskKey := base.TaskKey(qname, id)
		msg := r.HGet(taskKey, "msg").Val()
		msgs = append(msgs, MustUnmarshal(tb, msg))
		if gotState := r.HGet(taskKey, "state").Val(); gotState != state {
			tb.Errorf("task (id=%q) is in %q state, want %q", id, gotState, state)
		}
	}
	return msgs
}

// Retrieves all messages along with their scores stored under `keyFn(qname)` key in redis zset (sorted-set).
func getMessagesFromZSetWithScores(tb testing.TB, r redis.UniversalClient,
	qname string, keyFn func(qname string) string, state string) []base.Z {
	tb.Helper()
	zs := r.ZRangeWithScores(keyFn(qname), 0, -1).Val()
	var res []base.Z
	for _, z := range zs {
		taskID := z.Member.(string)
		taskKey := base.TaskKey(qname, taskID)
		msg := r.HGet(taskKey, "msg").Val()
		res = append(res, base.Z{Message: MustUnmarshal(tb, msg), Score: int64(z.Score)})
		if gotState := r.HGet(taskKey, "state").Val(); gotState != state {
			tb.Errorf("task (id=%q) is in %q state, want %q", taskID, gotState, state)
		}
	}
	return res
}
