package asynq

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
)

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
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		err := r.enqueue(tc.msg)
		if err != nil {
			t.Error(err)
			continue
		}
		res := r.client.LRange(defaultQueue, 0, -1).Val()
		if len(res) != 1 {
			t.Errorf("LIST %q has length %d, want 1", defaultQueue, len(res))
			continue
		}
		if !r.client.SIsMember(allQueues, defaultQueue).Val() {
			t.Errorf("SISMEMBER %q %q = false, want true", allQueues, defaultQueue)
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
		queued     []*taskMessage
		want       *taskMessage
		err        error
		inProgress int64 // length of "in-progress" tasks after dequeue
	}{
		{queued: []*taskMessage{t1}, want: t1, err: nil, inProgress: 1},
		{queued: []*taskMessage{}, want: nil, err: errDequeueTimeout, inProgress: 0},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
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
		if l := r.client.LLen(inProgress).Val(); l != tc.inProgress {
			t.Errorf("LIST %q has length %d, want %d", inProgress, l, tc.inProgress)
		}
	}
}

func TestRemove(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("export_csv", "csv", nil)

	tests := []struct {
		initial []*taskMessage // initial state of the list
		target  *taskMessage   // task to remove
		final   []*taskMessage // final state of the list
	}{
		{
			initial: []*taskMessage{t1, t2},
			target:  t1,
			final:   []*taskMessage{t2},
		},
		{
			initial: []*taskMessage{t2},
			target:  t1,
			final:   []*taskMessage{t2},
		},
		{
			initial: []*taskMessage{t1},
			target:  t1,
			final:   []*taskMessage{},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// set up initial state
		for _, task := range tc.initial {
			err := r.client.LPush(defaultQueue, mustMarshal(t, task)).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		err := r.remove(defaultQueue, tc.target)
		if err != nil {
			t.Error(err)
			continue
		}

		var got []*taskMessage
		data := r.client.LRange(defaultQueue, 0, -1).Val()
		for _, s := range data {
			got = append(got, mustUnmarshal(t, s))
		}

		if diff := cmp.Diff(tc.final, got, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*rdb).remove: (-want, +got):\n%s", defaultQueue, diff)
			continue
		}
	}
}

func TestKill(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)

	// TODO(hibiken): add test cases for trimming
	tests := []struct {
		initial []*taskMessage //  inital state of "dead" set
		target  *taskMessage   // task to kill
		want    []*taskMessage // final state of "dead" set
	}{
		{
			initial: []*taskMessage{},
			target:  t1,
			want:    []*taskMessage{t1},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		// set up initial state
		for _, task := range tc.initial {
			err := r.client.ZAdd(dead, &redis.Z{Member: mustMarshal(t, task), Score: float64(time.Now().Unix())}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		err := r.kill(tc.target)
		if err != nil {
			t.Error(err)
			continue
		}

		actual := r.client.ZRange(dead, 0, -1).Val()
		got := mustUnmarshalSlice(t, actual)
		if diff := cmp.Diff(tc.want, got, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*rdb).kill: (-want, +got):\n%s", dead, diff)
			continue
		}
	}
}

func TestMoveAll(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("export_csv", "csv", nil)
	t3 := randomTask("sync_stuff", "sync", nil)

	tests := []struct {
		beforeSrc []*taskMessage
		beforeDst []*taskMessage
		afterSrc  []*taskMessage
		afterDst  []*taskMessage
	}{
		{
			beforeSrc: []*taskMessage{t1, t2, t3},
			beforeDst: []*taskMessage{},
			afterSrc:  []*taskMessage{},
			afterDst:  []*taskMessage{t1, t2, t3},
		},
		{
			beforeSrc: []*taskMessage{},
			beforeDst: []*taskMessage{t1, t2, t3},
			afterSrc:  []*taskMessage{},
			afterDst:  []*taskMessage{t1, t2, t3},
		},
		{
			beforeSrc: []*taskMessage{t2, t3},
			beforeDst: []*taskMessage{t1},
			afterSrc:  []*taskMessage{},
			afterDst:  []*taskMessage{t1, t2, t3},
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
			r.client.LPush(inProgress, mustMarshal(t, msg))
		}
		// seed dst list.
		for _, msg := range tc.beforeDst {
			r.client.LPush(defaultQueue, mustMarshal(t, msg))
		}

		if err := r.moveAll(inProgress, defaultQueue); err != nil {
			t.Errorf("(*rdb).moveAll(%q, %q) = %v, want nil", inProgress, defaultQueue, err)
			continue
		}

		src := r.client.LRange(inProgress, 0, -1).Val()
		gotSrc := mustUnmarshalSlice(t, src)
		if diff := cmp.Diff(tc.afterSrc, gotSrc, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", inProgress, diff)
		}
		dst := r.client.LRange(defaultQueue, 0, -1).Val()
		gotDst := mustUnmarshalSlice(t, dst)
		if diff := cmp.Diff(tc.afterDst, gotDst, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", defaultQueue, diff)
		}
	}
}

func TestForward(t *testing.T) {
	r := setup(t)
	t1 := randomTask("send_email", defaultQueue, nil)
	t2 := randomTask("generate_csv", defaultQueue, nil)
	secondAgo := time.Now().Add(-time.Second)
	hourFromNow := time.Now().Add(time.Hour)

	tests := []struct {
		tasks         []*redis.Z     // scheduled tasks with timestamp as a score
		wantQueued    []*taskMessage // queue after calling forward
		wantScheduled []*taskMessage // scheduled queue after calling forward
	}{
		{
			tasks: []*redis.Z{
				&redis.Z{Member: mustMarshal(t, t1), Score: float64(secondAgo.Unix())},
				&redis.Z{Member: mustMarshal(t, t2), Score: float64(secondAgo.Unix())}},
			wantQueued:    []*taskMessage{t1, t2},
			wantScheduled: []*taskMessage{},
		},
		{
			tasks: []*redis.Z{
				&redis.Z{Member: mustMarshal(t, t1), Score: float64(hourFromNow.Unix())},
				&redis.Z{Member: mustMarshal(t, t2), Score: float64(secondAgo.Unix())}},
			wantQueued:    []*taskMessage{t2},
			wantScheduled: []*taskMessage{t1},
		},
		{
			tasks: []*redis.Z{
				&redis.Z{Member: mustMarshal(t, t1), Score: float64(hourFromNow.Unix())},
				&redis.Z{Member: mustMarshal(t, t2), Score: float64(hourFromNow.Unix())}},
			wantQueued:    []*taskMessage{},
			wantScheduled: []*taskMessage{t1, t2},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		if err := r.client.ZAdd(scheduled, tc.tasks...).Err(); err != nil {
			t.Error(err)
			continue
		}

		err := r.forward(scheduled)
		if err != nil {
			t.Errorf("(*rdb).forward(%q) = %v, want nil", scheduled, err)
			continue
		}
		queued := r.client.LRange(defaultQueue, 0, -1).Val()
		gotQueued := mustUnmarshalSlice(t, queued)
		if diff := cmp.Diff(tc.wantQueued, gotQueued, sortMsgOpt); diff != "" {
			t.Errorf("%q has %d tasks, want %d tasks; (-want, +got)\n%s", defaultQueue, len(gotQueued), len(tc.wantQueued), diff)
			continue
		}
		scheduled := r.client.ZRangeByScore(scheduled, &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Val()
		gotScheduled := mustUnmarshalSlice(t, scheduled)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("%q has %d tasks, want %d tasks; (-want, +got)\n%s", scheduled, len(gotScheduled), len(tc.wantScheduled), diff)
			continue
		}
	}
}

func TestSchedule(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg       *taskMessage
		processAt time.Time
		zset      string
	}{
		{
			randomTask("send_email", "default", map[string]interface{}{"subject": "hello"}),
			time.Now().Add(15 * time.Minute),
			scheduled,
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}

		err := r.schedule(tc.zset, tc.processAt, tc.msg)
		if err != nil {
			t.Error(err)
			continue
		}

		res, err := r.client.ZRangeWithScores(tc.zset, 0, -1).Result()
		if err != nil {
			t.Error(err)
			continue
		}

		desc := fmt.Sprintf("(*rdb).schedule(%q, %v, %v)", tc.zset, tc.processAt, tc.msg)
		if len(res) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted", desc, len(res), tc.zset)
			continue
		}

		if res[0].Score != float64(tc.processAt.Unix()) {
			t.Errorf("%s inserted an item with score %f, want %f", desc, res[0].Score, float64(tc.processAt.Unix()))
			continue
		}
	}
}
