package asynq

import (
	"encoding/json"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

var client *redis.Client

func init() {
	rand.Seed(time.Now().UnixNano())
}

var sortStrOpt = cmp.Transformer("SortStr", func(in []string) []string {
	out := append([]string(nil), in...) // Copy input to avoid mutating it
	sort.Strings(out)
	return out
})

// setup connects to a redis database and flush all keys
// before returning an instance of rdb.
func setup(t *testing.T) *rdb {
	t.Helper()
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

func randomTask(taskType, qname string, payload map[string]interface{}) *taskMessage {
	return &taskMessage{
		ID:      uuid.New(),
		Type:    taskType,
		Queue:   qname,
		Retry:   rand.Intn(100),
		Payload: make(map[string]interface{}),
	}
}

func TestEnqueue(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg *taskMessage
	}{
		{msg: randomTask("send_email", "default",
			map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"})},
		{msg: randomTask("generate_csv", "default",
			map[string]interface{}{})},
		{msg: randomTask("sync", "default", nil)},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		err := r.enqueue(tc.msg)
		if err != nil {
			t.Error(err)
		}
		res := client.LRange(defaultQueue, 0, -1).Val()
		if len(res) != 1 {
			t.Errorf("LIST %q has length %d, want 1", defaultQueue, len(res))
			continue
		}
		if !client.SIsMember(allQueues, defaultQueue).Val() {
			t.Errorf("SISMEMBER %q %q = false, want true", allQueues, defaultQueue)
		}
		var persisted taskMessage
		if err := json.Unmarshal([]byte(res[0]), &persisted); err != nil {
			t.Error(err)
			continue
		}
		if diff := cmp.Diff(*tc.msg, persisted); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", map[string]interface{}{"subject": "hello!"})
	tests := []struct {
		queued     []*taskMessage
		want       *taskMessage
		err        error
		inProgress int64 // length of "in-progress" tasks after dequeue
	}{
		{queued: []*taskMessage{t1}, want: t1, err: nil, inProgress: 1},
		{queued: []*taskMessage{}, want: nil, err: errQueuePopTimeout, inProgress: 0},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		for _, m := range tc.queued {
			r.enqueue(m)
		}
		got, err := r.dequeue(defaultQueue, time.Second)
		if !cmp.Equal(got, tc.want) || err != tc.err {
			t.Errorf("(*rdb).dequeue(%q, time.Second) = %v, %v; want %v, %v",
				defaultQueue, got, err, tc.want, tc.err)
			continue
		}
		if l := client.LLen(inProgress).Val(); l != tc.inProgress {
			t.Errorf("LIST %q has length %d, want %d", inProgress, l, tc.inProgress)
		}
	}
}

func TestMoveAll(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("export_csv", "csv", nil)
	t3 := randomTask("sync_stuff", "sync", nil)
	json1, err := json.Marshal(t1)
	if err != nil {
		t.Fatal(err)
	}
	json2, err := json.Marshal(t2)
	if err != nil {
		t.Fatal(err)
	}
	json3, err := json.Marshal(t3)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		beforeSrc []string
		beforeDst []string
		afterSrc  []string
		afterDst  []string
	}{
		{
			beforeSrc: []string{string(json1), string(json2), string(json3)},
			beforeDst: []string{},
			afterSrc:  []string{},
			afterDst:  []string{string(json1), string(json2), string(json3)},
		},
		{
			beforeSrc: []string{},
			beforeDst: []string{string(json1), string(json2), string(json3)},
			afterSrc:  []string{},
			afterDst:  []string{string(json1), string(json2), string(json3)},
		},
		{
			beforeSrc: []string{string(json2), string(json3)},
			beforeDst: []string{string(json1)},
			afterSrc:  []string{},
			afterDst:  []string{string(json1), string(json2), string(json3)},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := client.FlushDB().Err(); err != nil {
			t.Error(err)
			continue
		}
		// seed src list.
		for _, msg := range tc.beforeSrc {
			client.LPush(inProgress, msg)
		}
		// seed dst list.
		for _, msg := range tc.beforeDst {
			client.LPush(defaultQueue, msg)
		}

		if err := r.moveAll(inProgress, defaultQueue); err != nil {
			t.Errorf("(*rdb).moveAll(%q, %q) = %v, want nil", inProgress, defaultQueue, err)
			continue
		}

		gotSrc := client.LRange(inProgress, 0, -1).Val()
		if diff := cmp.Diff(tc.afterSrc, gotSrc, sortStrOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", inProgress, diff)
		}
		gotDst := client.LRange(defaultQueue, 0, -1).Val()
		if diff := cmp.Diff(tc.afterDst, gotDst, sortStrOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", defaultQueue, diff)
		}
	}
}

func TestForward(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", defaultQueue, nil)
	t2 := randomTask("generate_csv", defaultQueue, nil)
	json1, err := json.Marshal(t1)
	if err != nil {
		t.Fatal(err)
	}
	json2, err := json.Marshal(t2)
	if err != nil {
		t.Fatal(err)
	}
	secondAgo := time.Now().Add(-time.Second)
	hourFromNow := time.Now().Add(time.Hour)

	tests := []struct {
		tasks         []*redis.Z // scheduled tasks with timestamp as a score
		wantQueued    []string   // queue after calling forward
		wantScheduled []string   // scheduled queue after calling forward
	}{
		{
			tasks: []*redis.Z{
				&redis.Z{Member: string(json1), Score: float64(secondAgo.Unix())},
				&redis.Z{Member: string(json2), Score: float64(secondAgo.Unix())}},
			wantQueued:    []string{string(json1), string(json2)},
			wantScheduled: []string{},
		},
		{
			tasks: []*redis.Z{
				&redis.Z{Member: string(json1), Score: float64(hourFromNow.Unix())},
				&redis.Z{Member: string(json2), Score: float64(secondAgo.Unix())}},
			wantQueued:    []string{string(json2)},
			wantScheduled: []string{string(json1)},
		},
		{
			tasks: []*redis.Z{
				&redis.Z{Member: string(json1), Score: float64(hourFromNow.Unix())},
				&redis.Z{Member: string(json2), Score: float64(hourFromNow.Unix())}},
			wantQueued:    []string{},
			wantScheduled: []string{string(json1), string(json2)},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.ZAdd(scheduled, tc.tasks...).Err(); err != nil {
			t.Error(err)
			continue
		}

		err = r.forward(scheduled)
		if err != nil {
			t.Errorf("(*rdb).forward(%q) = %v, want nil", scheduled, err)
			continue
		}
		gotQueued := client.LRange(defaultQueue, 0, -1).Val()
		if diff := cmp.Diff(tc.wantQueued, gotQueued, sortStrOpt); diff != "" {
			t.Errorf("%q has %d tasks, want %d tasks; (-want, +got)\n%s", defaultQueue, len(gotQueued), len(tc.wantQueued), diff)
			continue
		}
		gotScheduled := client.ZRangeByScore(scheduled, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Val()
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortStrOpt); diff != "" {
			t.Errorf("%q has %d tasks, want %d tasks; (-want, +got)\n%s", scheduled, len(gotScheduled), len(tc.wantScheduled), diff)
			continue
		}
	}
}
