package asynq

import (
	"encoding/json"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

// This file defines test helper functions used by
// other test files.

func init() {
	rand.Seed(time.Now().UnixNano())
}

var sortMsgOpt = cmp.Transformer("SortMsg", func(in []*taskMessage) []*taskMessage {
	out := append([]*taskMessage(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID.String() < out[j].ID.String()
	})
	return out
})

var sortTaskOpt = cmp.Transformer("SortMsg", func(in []*Task) []*Task {
	out := append([]*Task(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].Type < out[j].Type
	})
	return out
})

// setup connects to a redis database and flush all keys
// before returning an instance of rdb.
func setup(t *testing.T) *rdb {
	t.Helper()
	r := newRDB(&RedisOpt{
		Addr: "localhost:6379",
		DB:   15, // use database 15 to separate from other applications
	})
	// Start each test with a clean slate.
	if err := r.client.FlushDB().Err(); err != nil {
		panic(err)
	}
	return r
}

func randomTask(taskType, qname string, payload map[string]interface{}) *taskMessage {
	return &taskMessage{
		ID:      uuid.New(),
		Type:    taskType,
		Queue:   qname,
		Retry:   rand.Intn(100),
		Payload: make(map[string]interface{}),
	}
}

func mustMarshal(t *testing.T, task *taskMessage) string {
	t.Helper()
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func mustUnmarshal(t *testing.T, data string) *taskMessage {
	t.Helper()
	var task taskMessage
	err := json.Unmarshal([]byte(data), &task)
	if err != nil {
		t.Fatal(err)
	}
	return &task
}

func mustMarshalSlice(t *testing.T, tasks []*taskMessage) []string {
	t.Helper()
	var data []string
	for _, task := range tasks {
		data = append(data, mustMarshal(t, task))
	}
	return data
}

func mustUnmarshalSlice(t *testing.T, data []string) []*taskMessage {
	t.Helper()
	var tasks []*taskMessage
	for _, s := range data {
		tasks = append(tasks, mustUnmarshal(t, s))
	}
	return tasks
}
