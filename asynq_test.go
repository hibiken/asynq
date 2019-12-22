package asynq

import (
	"encoding/json"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hibiken/asynq/internal/base"
	"github.com/rs/xid"
)

// This file defines test helper functions used by
// other test files.

func init() {
	rand.Seed(time.Now().UnixNano())
}

// scheduledEntry represents an item in redis sorted set (aka ZSET).
type sortedSetEntry struct {
	msg   *base.TaskMessage
	score int64
}

func setup(t *testing.T) *redis.Client {
	t.Helper()
	r := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   14,
	})
	// Start each test with a clean slate.
	if err := r.FlushDB().Err(); err != nil {
		panic(err)
	}
	return r
}

var sortTaskOpt = cmp.Transformer("SortMsg", func(in []*Task) []*Task {
	out := append([]*Task(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Type < out[j].Type
	})
	return out
})

var sortMsgOpt = cmp.Transformer("SortMsg", func(in []*base.TaskMessage) []*base.TaskMessage {
	out := append([]*base.TaskMessage(nil), in...) // Copy input to avoid mutating it
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

var ignoreIDOpt = cmpopts.IgnoreFields(base.TaskMessage{}, "ID")

func randomTask(taskType, qname string, payload map[string]interface{}) *base.TaskMessage {
	return &base.TaskMessage{
		ID:      xid.New(),
		Type:    taskType,
		Queue:   qname,
		Retry:   defaultMaxRetry,
		Payload: make(map[string]interface{}),
	}
}

func mustMarshal(t *testing.T, task *base.TaskMessage) string {
	t.Helper()
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func mustUnmarshal(t *testing.T, data string) *base.TaskMessage {
	t.Helper()
	var task base.TaskMessage
	err := json.Unmarshal([]byte(data), &task)
	if err != nil {
		t.Fatal(err)
	}
	return &task
}

func mustMarshalSlice(t *testing.T, tasks []*base.TaskMessage) []string {
	t.Helper()
	var data []string
	for _, task := range tasks {
		data = append(data, mustMarshal(t, task))
	}
	return data
}

func mustUnmarshalSlice(t *testing.T, data []string) []*base.TaskMessage {
	t.Helper()
	var tasks []*base.TaskMessage
	for _, s := range data {
		tasks = append(tasks, mustUnmarshal(t, s))
	}
	return tasks
}
