package rdb

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func setup(t *testing.T) *RDB {
	t.Helper()
	r := NewRDB(redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	}))
	// Start each test with a clean slate.
	if err := r.client.FlushDB().Err(); err != nil {
		panic(err)
	}
	return r
}

var sortMsgOpt = cmp.Transformer("SortMsg", func(in []*TaskMessage) []*TaskMessage {
	out := append([]*TaskMessage(nil), in...) // Copy input to avoid mutating it
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID.String() < out[j].ID.String()
	})
	return out
})

func randomTask(taskType, qname string, payload map[string]interface{}) *TaskMessage {
	return &TaskMessage{
		ID:      uuid.New(),
		Type:    taskType,
		Queue:   qname,
		Retry:   25,
		Payload: make(map[string]interface{}),
	}
}

func mustMarshal(t *testing.T, task *TaskMessage) string {
	t.Helper()
	data, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func mustUnmarshal(t *testing.T, data string) *TaskMessage {
	t.Helper()
	var task TaskMessage
	err := json.Unmarshal([]byte(data), &task)
	if err != nil {
		t.Fatal(err)
	}
	return &task
}

func mustMarshalSlice(t *testing.T, tasks []*TaskMessage) []string {
	t.Helper()
	var data []string
	for _, task := range tasks {
		data = append(data, mustMarshal(t, task))
	}
	return data
}

func mustUnmarshalSlice(t *testing.T, data []string) []*TaskMessage {
	t.Helper()
	var tasks []*TaskMessage
	for _, s := range data {
		tasks = append(tasks, mustUnmarshal(t, s))
	}
	return tasks
}

func TestEnqueue(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg *TaskMessage
	}{
		{msg: randomTask("send_email", "default",
			map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"})},
		{msg: randomTask("generate_csv", "default",
			map[string]interface{}{})},
		{msg: randomTask("sync", "default", nil)},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		err := r.Enqueue(tc.msg)
		if err != nil {
			t.Error(err)
			continue
		}
		res := r.client.LRange(DefaultQueue, 0, -1).Val()
		if len(res) != 1 {
			t.Errorf("LIST %q has length %d, want 1", DefaultQueue, len(res))
			continue
		}
		if !r.client.SIsMember(allQueues, DefaultQueue).Val() {
			t.Errorf("SISMEMBER %q %q = false, want true", allQueues, DefaultQueue)
		}
		if diff := cmp.Diff(*tc.msg, *mustUnmarshal(t, res[0])); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", map[string]interface{}{"subject": "hello!"})
	tests := []struct {
		queued     []*TaskMessage
		want       *TaskMessage
		err        error
		inProgress int64 // length of "in-progress" tasks after dequeue
	}{
		{queued: []*TaskMessage{t1}, want: t1, err: nil, inProgress: 1},
		{queued: []*TaskMessage{}, want: nil, err: ErrDequeueTimeout, inProgress: 0},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		for _, m := range tc.queued {
			r.Enqueue(m)
		}
		got, err := r.Dequeue(time.Second)
		if !cmp.Equal(got, tc.want) || err != tc.err {
			t.Errorf("(*RDB).Dequeue(time.Second) = %v, %v; want %v, %v",
				got, err, tc.want, tc.err)
			continue
		}
		if l := r.client.LLen(InProgress).Val(); l != tc.inProgress {
			t.Errorf("LIST %q has length %d, want %d", InProgress, l, tc.inProgress)
		}
	}
}

func TestDone(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("export_csv", "csv", nil)

	tests := []struct {
		initial []*TaskMessage // initial state of the in-progress list
		target  *TaskMessage   // task to remove
		final   []*TaskMessage // final state of the in-progress list
	}{
		{
			initial: []*TaskMessage{t1, t2},
			target:  t1,
			final:   []*TaskMessage{t2},
		},
		{
			initial: []*TaskMessage{t2},
			target:  t1,
			final:   []*TaskMessage{t2},
		},
		{
			initial: []*TaskMessage{t1},
			target:  t1,
			final:   []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// set up initial state
		for _, task := range tc.initial {
			err := r.client.LPush(InProgress, mustMarshal(t, task)).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		err := r.Done(tc.target)
		if err != nil {
			t.Error(err)
			continue
		}

		var got []*TaskMessage
		data := r.client.LRange(InProgress, 0, -1).Val()
		for _, s := range data {
			got = append(got, mustUnmarshal(t, s))
		}

		if diff := cmp.Diff(tc.final, got, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*rdb).remove: (-want, +got):\n%s", DefaultQueue, diff)
			continue
		}
	}
}

func TestKill(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)

	// TODO(hibiken): add test cases for trimming
	tests := []struct {
		initial []*TaskMessage //  inital state of "dead" set
		target  *TaskMessage   // task to kill
		want    []*TaskMessage // final state of "dead" set
	}{
		{
			initial: []*TaskMessage{},
			target:  t1,
			want:    []*TaskMessage{t1},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// set up initial state
		for _, task := range tc.initial {
			err := r.client.ZAdd(Dead, &redis.Z{Member: mustMarshal(t, task), Score: float64(time.Now().Unix())}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		err := r.Kill(tc.target)
		if err != nil {
			t.Error(err)
			continue
		}

		actual := r.client.ZRange(Dead, 0, -1).Val()
		got := mustUnmarshalSlice(t, actual)
		if diff := cmp.Diff(tc.want, got, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*rdb).kill: (-want, +got):\n%s", Dead, diff)
			continue
		}
	}
}

func TestRestoreUnfinished(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("export_csv", "csv", nil)
	t3 := randomTask("sync_stuff", "sync", nil)

	tests := []struct {
		beforeSrc []*TaskMessage
		beforeDst []*TaskMessage
		afterSrc  []*TaskMessage
		afterDst  []*TaskMessage
	}{
		{
			beforeSrc: []*TaskMessage{t1, t2, t3},
			beforeDst: []*TaskMessage{},
			afterSrc:  []*TaskMessage{},
			afterDst:  []*TaskMessage{t1, t2, t3},
		},
		{
			beforeSrc: []*TaskMessage{},
			beforeDst: []*TaskMessage{t1, t2, t3},
			afterSrc:  []*TaskMessage{},
			afterDst:  []*TaskMessage{t1, t2, t3},
		},
		{
			beforeSrc: []*TaskMessage{t2, t3},
			beforeDst: []*TaskMessage{t1},
			afterSrc:  []*TaskMessage{},
			afterDst:  []*TaskMessage{t1, t2, t3},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Error(err)
			continue
		}
		// seed src list.
		for _, msg := range tc.beforeSrc {
			r.client.LPush(InProgress, mustMarshal(t, msg))
		}
		// seed dst list.
		for _, msg := range tc.beforeDst {
			r.client.LPush(DefaultQueue, mustMarshal(t, msg))
		}

		if err := r.RestoreUnfinished(); err != nil {
			t.Errorf("(*RDB).RestoreUnfinished() = %v, want nil", err)
			continue
		}

		src := r.client.LRange(InProgress, 0, -1).Val()
		gotSrc := mustUnmarshalSlice(t, src)
		if diff := cmp.Diff(tc.afterSrc, gotSrc, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", InProgress, diff)
		}
		dst := r.client.LRange(DefaultQueue, 0, -1).Val()
		gotDst := mustUnmarshalSlice(t, dst)
		if diff := cmp.Diff(tc.afterDst, gotDst, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", DefaultQueue, diff)
		}
	}
}

func TestForward(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("generate_csv", "default", nil)
	secondAgo := time.Now().Add(-time.Second)
	hourFromNow := time.Now().Add(time.Hour)

	tests := []struct {
		tasks         []*redis.Z     // scheduled tasks with timestamp as a score
		wantQueued    []*TaskMessage // queue after calling forward
		wantScheduled []*TaskMessage // scheduled queue after calling forward
	}{
		{
			tasks: []*redis.Z{
				&redis.Z{Member: mustMarshal(t, t1), Score: float64(secondAgo.Unix())},
				&redis.Z{Member: mustMarshal(t, t2), Score: float64(secondAgo.Unix())}},
			wantQueued:    []*TaskMessage{t1, t2},
			wantScheduled: []*TaskMessage{},
		},
		{
			tasks: []*redis.Z{
				&redis.Z{Member: mustMarshal(t, t1), Score: float64(hourFromNow.Unix())},
				&redis.Z{Member: mustMarshal(t, t2), Score: float64(secondAgo.Unix())}},
			wantQueued:    []*TaskMessage{t2},
			wantScheduled: []*TaskMessage{t1},
		},
		{
			tasks: []*redis.Z{
				&redis.Z{Member: mustMarshal(t, t1), Score: float64(hourFromNow.Unix())},
				&redis.Z{Member: mustMarshal(t, t2), Score: float64(hourFromNow.Unix())}},
			wantQueued:    []*TaskMessage{},
			wantScheduled: []*TaskMessage{t1, t2},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		if err := r.client.ZAdd(Scheduled, tc.tasks...).Err(); err != nil {
			t.Error(err)
			continue
		}

		err := r.Forward(Scheduled)
		if err != nil {
			t.Errorf("(*RDB).Forward(%q) = %v, want nil", Scheduled, err)
			continue
		}
		queued := r.client.LRange(DefaultQueue, 0, -1).Val()
		gotQueued := mustUnmarshalSlice(t, queued)
		if diff := cmp.Diff(tc.wantQueued, gotQueued, sortMsgOpt); diff != "" {
			t.Errorf("%q has %d tasks, want %d tasks; (-want, +got)\n%s", DefaultQueue, len(gotQueued), len(tc.wantQueued), diff)
		}
		scheduled := r.client.ZRangeByScore(Scheduled, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Val()
		gotScheduled := mustUnmarshalSlice(t, scheduled)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("%q has %d tasks, want %d tasks; (-want, +got)\n%s", scheduled, len(gotScheduled), len(tc.wantScheduled), diff)
		}
	}
}

func TestSchedule(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg       *TaskMessage
		processAt time.Time
	}{
		{
			randomTask("send_email", "default", map[string]interface{}{"subject": "hello"}),
			time.Now().Add(15 * time.Minute),
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}

		err := r.Schedule(tc.msg, tc.processAt)
		if err != nil {
			t.Error(err)
			continue
		}

		res, err := r.client.ZRangeWithScores(Scheduled, 0, -1).Result()
		if err != nil {
			t.Error(err)
			continue
		}

		desc := fmt.Sprintf("(*RDB).Schedule(%v, %v)", tc.msg, tc.processAt)
		if len(res) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted", desc, len(res), Scheduled)
			continue
		}

		if res[0].Score != float64(tc.processAt.Unix()) {
			t.Errorf("%s inserted an item with score %f, want %f", desc, res[0].Score, float64(tc.processAt.Unix()))
			continue
		}
	}
}

func TestRetryLater(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg       *TaskMessage
		processAt time.Time
	}{
		{
			randomTask("send_email", "default", map[string]interface{}{"subject": "hello"}),
			time.Now().Add(15 * time.Minute),
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}

		err := r.RetryLater(tc.msg, tc.processAt)
		if err != nil {
			t.Error(err)
			continue
		}

		res, err := r.client.ZRangeWithScores(Retry, 0, -1).Result()
		if err != nil {
			t.Error(err)
			continue
		}

		desc := fmt.Sprintf("(*RDB).RetryLater(%v, %v)", tc.msg, tc.processAt)
		if len(res) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted", desc, len(res), Retry)
			continue
		}

		if res[0].Score != float64(tc.processAt.Unix()) {
			t.Errorf("%s inserted an item with score %f, want %f", desc, res[0].Score, float64(tc.processAt.Unix()))
			continue
		}
	}
}
