package asynq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

var client *redis.Client

// setup connects to a redis database and flush all keys
// before returning an instance of rdb.
func setup() *rdb {
	client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // use database 15 to separate from other applications
	})
	// Start each test with a clean slate.
	if err := client.FlushDB().Err(); err != nil {
		panic(err)
	}
	return newRDB(client)
}

func TestPush(t *testing.T) {
	r := setup()
	msg := &taskMessage{
		ID:    uuid.New(),
		Type:  "sendEmail",
		Queue: "default",
		Retry: 10,
	}

	err := r.push(msg)
	if err != nil {
		t.Fatalf("could not push message to queue: %v", err)
	}

	res := client.LRange("asynq:queues:default", 0, -1).Val()
	if len(res) != 1 {
		t.Fatalf("len(res) = %d, want %d", len(res), 1)
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("json.Marshal(msg) failed: %v", err)
	}
	if res[0] != string(bytes) {
		t.Fatalf("res[0] = %s, want %s", res[0], string(bytes))
	}
}

func TestDequeueImmediateReturn(t *testing.T) {
	r := setup()
	msg := &taskMessage{
		ID:    uuid.New(),
		Type:  "GenerateCSVExport",
		Queue: "csv",
		Retry: 10,
	}
	r.push(msg)

	res, err := r.dequeue(time.Second, "asynq:queues:csv")
	if err != nil {
		t.Fatalf("r.bpop() failed: %v", err)
	}

	if !cmp.Equal(res, msg) {
		t.Errorf("cmp.Equal(res, msg) = %t, want %t", false, true)
	}
	jobs := client.SMembers(inProgress).Val()
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want %d", len(jobs), 1)
	}
	var tm taskMessage
	if err := json.Unmarshal([]byte(jobs[0]), &tm); err != nil {
		t.Fatalf("json.Marshal() failed: %v", err)
	}
	if diff := cmp.Diff(res, &tm); diff != "" {
		t.Errorf("cmp.Diff(res, tm) = %s", diff)
	}
}

func TestDequeueTimeout(t *testing.T) {
	r := setup()

	_, err := r.dequeue(time.Second, "asynq:queues:default")
	if err != errQueuePopTimeout {
		t.Errorf("err = %v, want %v", err, errQueuePopTimeout)
	}
}
