// Package asynqtest defines test helpers for asynq and its internal packages.
package asynqtest

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/rs/xid"
)

// ZSetEntry is an entry in redis sorted set.
type ZSetEntry struct {
	Msg   *base.TaskMessage
	Score int64
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

// NewTaskMessage returns a new instance of TaskMessage given a task type and payload.
func NewTaskMessage(taskType string, payload map[string]interface{}) *base.TaskMessage {
	return &base.TaskMessage{
		ID:      xid.New(),
		Type:    taskType,
		Queue:   "default",
		Retry:   25,
		Payload: payload,
	}
}

// MustMarshal marshals given task message and returns a json string.
// Calling test will fail if marshaling errors out.
func MustMarshal(t *testing.T, msg *base.TaskMessage) string {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// MustUnmarshal unmarshals given string into task message struct.
// Calling test will fail if unmarshaling errors out.
func MustUnmarshal(t *testing.T, data string) *base.TaskMessage {
	t.Helper()
	var msg base.TaskMessage
	err := json.Unmarshal([]byte(data), &msg)
	if err != nil {
		t.Fatal(err)
	}
	return &msg
}

// MustMarshalSlice marshals a slice of task messages and return a slice of
// json strings. Calling test will fail if marshaling errors out.
func MustMarshalSlice(t *testing.T, msgs []*base.TaskMessage) []string {
	t.Helper()
	var data []string
	for _, m := range msgs {
		data = append(data, MustMarshal(t, m))
	}
	return data
}

// MustUnmarshalSlice unmarshals a slice of strings into a slice of task message structs.
// Calling test will fail if marshaling errors out.
func MustUnmarshalSlice(t *testing.T, data []string) []*base.TaskMessage {
	t.Helper()
	var msgs []*base.TaskMessage
	for _, s := range data {
		msgs = append(msgs, MustUnmarshal(t, s))
	}
	return msgs
}

// FlushDB deletes all the keys of the currently selected DB.
func FlushDB(t *testing.T, r *redis.Client) {
	t.Helper()
	if err := r.FlushDB().Err(); err != nil {
		t.Fatal(err)
	}
}

// SeedDefaultQueue initializes the default queue with the given messages.
func SeedDefaultQueue(t *testing.T, r *redis.Client, msgs []*base.TaskMessage) {
	t.Helper()
	seedRedisList(t, r, base.DefaultQueue, msgs)
}

// SeedInProgressQueue initializes the in-progress queue with the given messages.
func SeedInProgressQueue(t *testing.T, r *redis.Client, msgs []*base.TaskMessage) {
	t.Helper()
	seedRedisList(t, r, base.InProgressQueue, msgs)
}

// SeedScheduledQueue initializes the scheduled queue with the given messages.
func SeedScheduledQueue(t *testing.T, r *redis.Client, entries []ZSetEntry) {
	t.Helper()
	seedRedisZSet(t, r, base.ScheduledQueue, entries)
}

// SeedRetryQueue initializes the retry queue with the given messages.
func SeedRetryQueue(t *testing.T, r *redis.Client, entries []ZSetEntry) {
	t.Helper()
	seedRedisZSet(t, r, base.RetryQueue, entries)
}

// SeedDeadQueue initializes the dead queue with the given messages.
func SeedDeadQueue(t *testing.T, r *redis.Client, entries []ZSetEntry) {
	t.Helper()
	seedRedisZSet(t, r, base.DeadQueue, entries)
}

func seedRedisList(t *testing.T, c *redis.Client, key string, msgs []*base.TaskMessage) {
	data := MustMarshalSlice(t, msgs)
	for _, s := range data {
		if err := c.LPush(key, s).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func seedRedisZSet(t *testing.T, c *redis.Client, key string, items []ZSetEntry) {
	for _, item := range items {
		z := &redis.Z{Member: MustMarshal(t, item.Msg), Score: float64(item.Score)}
		if err := c.ZAdd(key, z).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

// GetEnqueuedMessages returns all task messages in the default queue.
func GetEnqueuedMessages(t *testing.T, r *redis.Client) []*base.TaskMessage {
	t.Helper()
	return getListMessages(t, r, base.DefaultQueue)
}

// GetInProgressMessages returns all task messages in the in-progress queue.
func GetInProgressMessages(t *testing.T, r *redis.Client) []*base.TaskMessage {
	t.Helper()
	return getListMessages(t, r, base.InProgressQueue)
}

// GetScheduledMessages returns all task messages in the scheduled queue.
func GetScheduledMessages(t *testing.T, r *redis.Client) []*base.TaskMessage {
	t.Helper()
	return getZSetMessages(t, r, base.ScheduledQueue)
}

// GetRetryMessages returns all task messages in the retry queue.
func GetRetryMessages(t *testing.T, r *redis.Client) []*base.TaskMessage {
	t.Helper()
	return getZSetMessages(t, r, base.RetryQueue)
}

// GetDeadMessages returns all task messages in the dead queue.
func GetDeadMessages(t *testing.T, r *redis.Client) []*base.TaskMessage {
	t.Helper()
	return getZSetMessages(t, r, base.DeadQueue)
}

// GetScheduledEntries returns all task messages and its score in the scheduled queue.
func GetScheduledEntries(t *testing.T, r *redis.Client) []ZSetEntry {
	t.Helper()
	return getZSetEntries(t, r, base.ScheduledQueue)
}

// GetRetryEntries returns all task messages and its score in the retry queue.
func GetRetryEntries(t *testing.T, r *redis.Client) []ZSetEntry {
	t.Helper()
	return getZSetEntries(t, r, base.RetryQueue)
}

// GetDeadEntries returns all task messages and its score in the dead queue.
func GetDeadEntries(t *testing.T, r *redis.Client) []ZSetEntry {
	t.Helper()
	return getZSetEntries(t, r, base.DeadQueue)
}

func getListMessages(t *testing.T, r *redis.Client, list string) []*base.TaskMessage {
	data := r.LRange(list, 0, -1).Val()
	return MustUnmarshalSlice(t, data)
}

func getZSetMessages(t *testing.T, r *redis.Client, zset string) []*base.TaskMessage {
	data := r.ZRange(zset, 0, -1).Val()
	return MustUnmarshalSlice(t, data)
}

func getZSetEntries(t *testing.T, r *redis.Client, zset string) []ZSetEntry {
	data := r.ZRangeWithScores(zset, 0, -1).Val()
	var entries []ZSetEntry
	for _, z := range data {
		entries = append(entries, ZSetEntry{
			Msg:   MustUnmarshal(t, z.Member.(string)),
			Score: int64(z.Score),
		})
	}
	return entries
}
