package rdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestEnqueue(t *testing.T) {
	r := setup(t)
	tests := []struct {
		msg *TaskMessage
	}{
		{msg: newTaskMessage("send_email", map[string]interface{}{"to": "exampleuser@gmail.com", "from": "noreply@example.com"})},
		{msg: newTaskMessage("generate_csv", map[string]interface{}{})},
		{msg: newTaskMessage("sync", nil)},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case.

		err := r.Enqueue(tc.msg)
		if err != nil {
			t.Errorf("(*RDB).Enqueue = %v, want nil", err)
			continue
		}
		res := r.client.LRange(defaultQ, 0, -1).Val()
		if len(res) != 1 {
			t.Errorf("%q has length %d, want 1", defaultQ, len(res))
			continue
		}
		if diff := cmp.Diff(tc.msg, mustUnmarshal(t, res[0])); diff != "" {
			t.Errorf("persisted data differed from the original input (-want, +got)\n%s", diff)
		}
	}
}

func TestDequeue(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", map[string]interface{}{"subject": "hello!"})
	tests := []struct {
		enqueued   []*TaskMessage
		want       *TaskMessage
		err        error
		inProgress int64 // length of "in-progress" tasks after dequeue
	}{
		{enqueued: []*TaskMessage{t1}, want: t1, err: nil, inProgress: 1},
		{enqueued: []*TaskMessage{}, want: nil, err: ErrDequeueTimeout, inProgress: 0},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDefaultQueue(t, r, tc.enqueued)

		got, err := r.Dequeue(time.Second)
		if !cmp.Equal(got, tc.want) || err != tc.err {
			t.Errorf("(*RDB).Dequeue(time.Second) = %v, %v; want %v, %v",
				got, err, tc.want, tc.err)
			continue
		}
		if l := r.client.LLen(inProgressQ).Val(); l != tc.inProgress {
			t.Errorf("%q has length %d, want %d", inProgressQ, l, tc.inProgress)
		}
	}
}

func TestDone(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("export_csv", nil)

	tests := []struct {
		inProgress     []*TaskMessage // initial state of the in-progress list
		target         *TaskMessage   // task to remove
		wantInProgress []*TaskMessage // final state of the in-progress list
	}{
		{
			inProgress:     []*TaskMessage{t1, t2},
			target:         t1,
			wantInProgress: []*TaskMessage{t2},
		},
		{
			inProgress:     []*TaskMessage{t2},
			target:         t1,
			wantInProgress: []*TaskMessage{t2},
		},
		{
			inProgress:     []*TaskMessage{t1},
			target:         t1,
			wantInProgress: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedInProgressQueue(t, r, tc.inProgress)

		err := r.Done(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Done(task) = %v, want nil", err)
			continue
		}

		data := r.client.LRange(inProgressQ, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, data)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*RDB).Done: (-want, +got):\n%s", inProgressQ, diff)
			continue
		}
	}
}

func TestRequeue(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("export_csv", nil)

	tests := []struct {
		enqueued       []*TaskMessage // initial state of the default queue
		inProgress     []*TaskMessage // initial state of the in-progress list
		target         *TaskMessage   // task to requeue
		wantEnqueued   []*TaskMessage // final state of the default queue
		wantInProgress []*TaskMessage // final state of the in-progress list
	}{
		{
			enqueued:       []*TaskMessage{},
			inProgress:     []*TaskMessage{t1, t2},
			target:         t1,
			wantEnqueued:   []*TaskMessage{t1},
			wantInProgress: []*TaskMessage{t2},
		},
		{
			enqueued:       []*TaskMessage{t1},
			inProgress:     []*TaskMessage{t2},
			target:         t2,
			wantEnqueued:   []*TaskMessage{t1, t2},
			wantInProgress: []*TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDefaultQueue(t, r, tc.enqueued)
		seedInProgressQueue(t, r, tc.inProgress)

		err := r.Requeue(tc.target)
		if err != nil {
			t.Errorf("(*RDB).Requeue(task) = %v, want nil", err)
			continue
		}

		gotEnqueuedRaw := r.client.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", defaultQ, diff)
		}

		gotInProgressRaw := r.client.LRange(inProgressQ, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, gotInProgressRaw)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q: (-want, +got):\n%s", inProgressQ, diff)
		}
	}
}

func TestKill(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("reindex", nil)
	t3 := newTaskMessage("generate_csv", nil)
	errMsg := "SMTP server not responding"
	t1AfterKill := &TaskMessage{
		ID:       t1.ID,
		Type:     t1.Type,
		Payload:  t1.Payload,
		Queue:    t1.Queue,
		Retry:    t1.Retry,
		Retried:  t1.Retried,
		ErrorMsg: errMsg,
	}
	now := time.Now()

	// TODO(hibiken): add test cases for trimming
	tests := []struct {
		inProgress     []*TaskMessage
		dead           []sortedSetEntry
		target         *TaskMessage // task to kill
		wantInProgress []*TaskMessage
		wantDead       []sortedSetEntry
	}{
		{
			inProgress: []*TaskMessage{t1, t2},
			dead: []sortedSetEntry{
				{t3, now.Add(-time.Hour).Unix()},
			},
			target:         t1,
			wantInProgress: []*TaskMessage{t2},
			wantDead: []sortedSetEntry{
				{t1AfterKill, now.Unix()},
				{t3, now.Add(-time.Hour).Unix()},
			},
		},
		{
			inProgress:     []*TaskMessage{t1, t2, t3},
			dead:           []sortedSetEntry{},
			target:         t1,
			wantInProgress: []*TaskMessage{t2, t3},
			wantDead: []sortedSetEntry{
				{t1AfterKill, now.Unix()},
			},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedInProgressQueue(t, r, tc.inProgress)
		seedDeadQueue(t, r, tc.dead)

		err := r.Kill(tc.target, errMsg)
		if err != nil {
			t.Errorf("(*RDB).Kill(%v, %v) = %v, want nil", tc.target, errMsg, err)
			continue
		}

		gotInProgressRaw := r.client.LRange(inProgressQ, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, gotInProgressRaw)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", inProgressQ, diff)
		}

		var gotDead []sortedSetEntry
		data := r.client.ZRangeWithScores(deadQ, 0, -1).Val()
		for _, z := range data {
			gotDead = append(gotDead, sortedSetEntry{
				msg:   mustUnmarshal(t, z.Member.(string)),
				score: int64(z.Score),
			})
		}

		cmpOpt := cmp.AllowUnexported(sortedSetEntry{})
		if diff := cmp.Diff(tc.wantDead, gotDead, cmpOpt, sortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q after calling (*RDB).Kill: (-want, +got):\n%s", deadQ, diff)
		}
	}
}

func TestRestoreUnfinished(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("export_csv", nil)
	t3 := newTaskMessage("sync_stuff", nil)

	tests := []struct {
		inProgress     []*TaskMessage
		enqueued       []*TaskMessage
		wantInProgress []*TaskMessage
		wantEnqueued   []*TaskMessage
	}{
		{
			inProgress:     []*TaskMessage{t1, t2, t3},
			enqueued:       []*TaskMessage{},
			wantInProgress: []*TaskMessage{},
			wantEnqueued:   []*TaskMessage{t1, t2, t3},
		},
		{
			inProgress:     []*TaskMessage{},
			enqueued:       []*TaskMessage{t1, t2, t3},
			wantInProgress: []*TaskMessage{},
			wantEnqueued:   []*TaskMessage{t1, t2, t3},
		},
		{
			inProgress:     []*TaskMessage{t2, t3},
			enqueued:       []*TaskMessage{t1},
			wantInProgress: []*TaskMessage{},
			wantEnqueued:   []*TaskMessage{t1, t2, t3},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedInProgressQueue(t, r, tc.inProgress)
		seedDefaultQueue(t, r, tc.enqueued)

		if err := r.RestoreUnfinished(); err != nil {
			t.Errorf("(*RDB).RestoreUnfinished() = %v, want nil", err)
			continue
		}

		gotInProgressRaw := r.client.LRange(inProgressQ, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, gotInProgressRaw)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", inProgressQ, diff)
		}
		gotEnqueuedRaw := r.client.LRange(defaultQ, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q (-want, +got)\n%s", defaultQ, diff)
		}
	}
}

func TestCheckAndEnqueue(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("generate_csv", nil)
	t3 := newTaskMessage("gen_thumbnail", nil)
	secondAgo := time.Now().Add(-time.Second)
	hourFromNow := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []sortedSetEntry
		retry         []sortedSetEntry
		wantQueued    []*TaskMessage
		wantScheduled []*TaskMessage
		wantRetry     []*TaskMessage
	}{
		{
			scheduled: []sortedSetEntry{
				{t1, secondAgo.Unix()},
				{t2, secondAgo.Unix()}},
			retry: []sortedSetEntry{
				{t3, secondAgo.Unix()}},
			wantQueued:    []*TaskMessage{t1, t2, t3},
			wantScheduled: []*TaskMessage{},
			wantRetry:     []*TaskMessage{},
		},
		{
			scheduled: []sortedSetEntry{
				{t1, hourFromNow.Unix()},
				{t2, secondAgo.Unix()}},
			retry: []sortedSetEntry{
				{t3, secondAgo.Unix()}},
			wantQueued:    []*TaskMessage{t2, t3},
			wantScheduled: []*TaskMessage{t1},
			wantRetry:     []*TaskMessage{},
		},
		{
			scheduled: []sortedSetEntry{
				{t1, hourFromNow.Unix()},
				{t2, hourFromNow.Unix()}},
			retry: []sortedSetEntry{
				{t3, hourFromNow.Unix()}},
			wantQueued:    []*TaskMessage{},
			wantScheduled: []*TaskMessage{t1, t2},
			wantRetry:     []*TaskMessage{t3},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedScheduledQueue(t, r, tc.scheduled)
		seedRetryQueue(t, r, tc.retry)

		err := r.CheckAndEnqueue()
		if err != nil {
			t.Errorf("(*RDB).CheckScheduled() = %v, want nil", err)
			continue
		}
		queued := r.client.LRange(defaultQ, 0, -1).Val()
		gotQueued := mustUnmarshalSlice(t, queued)
		if diff := cmp.Diff(tc.wantQueued, gotQueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", defaultQ, diff)
		}
		scheduled := r.client.ZRange(scheduledQ, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, scheduled)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", scheduledQ, diff)
		}
		retry := r.client.ZRange(retryQ, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, retry)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", retryQ, diff)
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
			newTaskMessage("send_email", map[string]interface{}{"subject": "hello"}),
			time.Now().Add(15 * time.Minute),
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case

		desc := fmt.Sprintf("(*RDB).Schedule(%v, %v)", tc.msg, tc.processAt)
		err := r.Schedule(tc.msg, tc.processAt)
		if err != nil {
			t.Errorf("%s = %v, want nil", desc, err)
			continue
		}

		res := r.client.ZRangeWithScores(scheduledQ, 0, -1).Val()
		if len(res) != 1 {
			t.Errorf("%s inserted %d items to %q, want 1 items inserted", desc, len(res), scheduledQ)
			continue
		}
		if res[0].Score != float64(tc.processAt.Unix()) {
			t.Errorf("%s inserted an item with score %f, want %f", desc, res[0].Score, float64(tc.processAt.Unix()))
			continue
		}
	}
}

func TestRetry(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", map[string]interface{}{"subject": "Hola!"})
	t2 := newTaskMessage("gen_thumbnail", map[string]interface{}{"path": "some/path/to/image.jpg"})
	t3 := newTaskMessage("reindex", nil)
	t1.Retried = 10
	errMsg := "SMTP server is not responding"
	t1AfterRetry := &TaskMessage{
		ID:       t1.ID,
		Type:     t1.Type,
		Payload:  t1.Payload,
		Queue:    t1.Queue,
		Retry:    t1.Retry,
		Retried:  t1.Retried + 1,
		ErrorMsg: errMsg,
	}
	now := time.Now()

	tests := []struct {
		inProgress     []*TaskMessage
		retry          []sortedSetEntry
		msg            *TaskMessage
		processAt      time.Time
		errMsg         string
		wantInProgress []*TaskMessage
		wantRetry      []sortedSetEntry
	}{
		{
			inProgress: []*TaskMessage{t1, t2},
			retry: []sortedSetEntry{
				{t3, now.Add(time.Minute).Unix()},
			},
			msg:            t1,
			processAt:      now.Add(5 * time.Minute),
			errMsg:         errMsg,
			wantInProgress: []*TaskMessage{t2},
			wantRetry: []sortedSetEntry{
				{t1AfterRetry, now.Add(5 * time.Minute).Unix()},
				{t3, now.Add(time.Minute).Unix()},
			},
		},
	}

	for _, tc := range tests {
		flushDB(t, r)
		seedInProgressQueue(t, r, tc.inProgress)
		seedRetryQueue(t, r, tc.retry)

		err := r.Retry(tc.msg, tc.processAt, tc.errMsg)
		if err != nil {
			t.Errorf("(*RDB).Retry = %v, want nil", err)
			continue
		}

		gotInProgressRaw := r.client.LRange(inProgressQ, 0, -1).Val()
		gotInProgress := mustUnmarshalSlice(t, gotInProgressRaw)
		if diff := cmp.Diff(tc.wantInProgress, gotInProgress, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", inProgressQ, diff)
		}

		gotRetryRaw := r.client.ZRangeWithScores(retryQ, 0, -1).Val()
		var gotRetry []sortedSetEntry
		for _, z := range gotRetryRaw {
			gotRetry = append(gotRetry, sortedSetEntry{
				msg:   mustUnmarshal(t, z.Member.(string)),
				score: int64(z.Score),
			})
		}
		cmpOpt := cmp.AllowUnexported(sortedSetEntry{})
		if diff := cmp.Diff(tc.wantRetry, gotRetry, cmpOpt, sortZSetEntryOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", retryQ, diff)
		}
	}
}
