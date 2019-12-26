package rdb

import (
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/rs/xid"
)

// TODO(hibiken): Replace this with cmpopts.EquateApproxTime once it becomes availalble.
// https://github.com/google/go-cmp/issues/166
//
// EquateApproxTime returns a Comparer options that
// determine two time.Time values to be equal if they
// are within the given time interval of one another.
// Note that if both times have a monotonic clock reading,
// the monotonic time difference will be used.
//
// The zero time is treated specially: it is only considered
// equal to another zero time value.
//
// It will panic if margin is negative.
func EquateApproxTime(margin time.Duration) cmp.Option {
	if margin < 0 {
		panic("negative duration in EquateApproxTime")
	}
	return cmp.FilterValues(func(x, y time.Time) bool {
		return !x.IsZero() && !y.IsZero()
	}, cmp.Comparer(timeApproximator{margin}.compare))
}

type timeApproximator struct {
	margin time.Duration
}

func (a timeApproximator) compare(x, y time.Time) bool {
	// Avoid subtracting times to avoid overflow when the
	// difference is larger than the largest representible duration.
	if x.After(y) {
		// Ensure x is always before y
		x, y = y, x
	}
	// We're within the margin if x+margin >= y.
	// Note: time.Time doesn't have AfterOrEqual method hence the negation.
	return !x.Add(a.margin).Before(y)
}

func TestCurrentStats(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := newTaskMessage("reindex", nil)
	m3 := newTaskMessage("gen_thumbnail", map[string]interface{}{"src": "some/path/to/img"})
	m4 := newTaskMessage("sync", nil)

	now := time.Now()

	tests := []struct {
		enqueued   []*base.TaskMessage
		inProgress []*base.TaskMessage
		scheduled  []sortedSetEntry
		retry      []sortedSetEntry
		dead       []sortedSetEntry
		processed  int
		failed     int
		want       *Stats
	}{
		{
			enqueued:   []*base.TaskMessage{m1},
			inProgress: []*base.TaskMessage{m2},
			scheduled: []sortedSetEntry{
				{m3, time.Now().Add(time.Hour).Unix()},
				{m4, time.Now().Unix()}},
			retry:     []sortedSetEntry{},
			dead:      []sortedSetEntry{},
			processed: 120,
			failed:    2,
			want: &Stats{
				Enqueued:   1,
				InProgress: 1,
				Scheduled:  2,
				Retry:      0,
				Dead:       0,
				Processed:  120,
				Failed:     2,
				Timestamp:  now,
			},
		},
		{
			enqueued:   []*base.TaskMessage{},
			inProgress: []*base.TaskMessage{},
			scheduled: []sortedSetEntry{
				{m3, time.Now().Unix()},
				{m4, time.Now().Unix()}},
			retry: []sortedSetEntry{
				{m1, time.Now().Add(time.Minute).Unix()}},
			dead: []sortedSetEntry{
				{m2, time.Now().Add(-time.Hour).Unix()}},
			processed: 90,
			failed:    10,
			want: &Stats{
				Enqueued:   0,
				InProgress: 0,
				Scheduled:  2,
				Retry:      1,
				Dead:       1,
				Processed:  90,
				Failed:     10,
				Timestamp:  now,
			},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDefaultQueue(t, r, tc.enqueued)
		seedInProgressQueue(t, r, tc.inProgress)
		seedScheduledQueue(t, r, tc.scheduled)
		seedRetryQueue(t, r, tc.retry)
		seedDeadQueue(t, r, tc.dead)
		processedKey := base.ProcessedKey(now)
		failedKey := base.FailureKey(now)
		r.client.Set(processedKey, tc.processed, 0)
		r.client.Set(failedKey, tc.failed, 0)

		got, err := r.CurrentStats()
		if err != nil {
			t.Errorf("r.CurrentStats() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}

		if diff := cmp.Diff(tc.want, got, timeCmpOpt); diff != "" {
			t.Errorf("r.CurrentStats() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestCurrentStatsWithoutData(t *testing.T) {
	r := setup(t)

	want := &Stats{
		Enqueued:   0,
		InProgress: 0,
		Scheduled:  0,
		Retry:      0,
		Dead:       0,
		Processed:  0,
		Failed:     0,
		Timestamp:  time.Now(),
	}

	got, err := r.CurrentStats()
	if err != nil {
		t.Fatalf("r.CurrentStats() = %v, %v, want %+v, nil", got, err, want)
	}

	if diff := cmp.Diff(want, got, timeCmpOpt); diff != "" {
		t.Errorf("r.CurrentStats() = %v, %v, want %+v, nil; (-want, +got)\n%s", got, err, want, diff)
	}
}

func TestRedisInfo(t *testing.T) {
	r := setup(t)

	info, err := r.RedisInfo()
	if err != nil {
		t.Fatalf("RDB.RedisInfo() returned error: %v", err)
	}

	wantKeys := []string{
		"redis_version",
		"uptime_in_days",
		"connected_clients",
		"used_memory_human",
		"used_memory_peak_human",
		"used_memory_peak_perc",
	}

	for _, key := range wantKeys {
		if _, ok := info[key]; !ok {
			t.Errorf("RDB.RedisInfo() = %v is missing entry for %q", info, key)
		}
	}
}

func TestListEnqueued(t *testing.T) {
	r := setup(t)

	m1 := newTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := newTaskMessage("reindex", nil)
	t1 := &EnqueuedTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload}
	t2 := &EnqueuedTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload}
	tests := []struct {
		enqueued []*base.TaskMessage
		want     []*EnqueuedTask
	}{
		{
			enqueued: []*base.TaskMessage{m1, m2},
			want:     []*EnqueuedTask{t1, t2},
		},
		{
			enqueued: []*base.TaskMessage{},
			want:     []*EnqueuedTask{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDefaultQueue(t, r, tc.enqueued)

		got, err := r.ListEnqueued()
		if err != nil {
			t.Errorf("r.ListEnqueued() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*EnqueuedTask) []*EnqueuedTask {
			out := append([]*EnqueuedTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("r.ListEnqueued() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListInProgress(t *testing.T) {
	r := setup(t)

	m1 := newTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := newTaskMessage("reindex", nil)
	t1 := &InProgressTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload}
	t2 := &InProgressTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload}
	tests := []struct {
		inProgress []*base.TaskMessage
		want       []*InProgressTask
	}{
		{
			inProgress: []*base.TaskMessage{m1, m2},
			want:       []*InProgressTask{t1, t2},
		},
		{
			inProgress: []*base.TaskMessage{},
			want:       []*InProgressTask{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedInProgressQueue(t, r, tc.inProgress)

		got, err := r.ListInProgress()
		if err != nil {
			t.Errorf("r.ListInProgress() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*InProgressTask) []*InProgressTask {
			out := append([]*InProgressTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})
		if diff := cmp.Diff(tc.want, got, sortOpt); diff != "" {
			t.Errorf("r.ListInProgress() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListScheduled(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", map[string]interface{}{"subject": "hello"})
	m2 := newTaskMessage("reindex", nil)
	p1 := time.Now().Add(30 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	t1 := &ScheduledTask{ID: m1.ID, Type: m1.Type, Payload: m1.Payload, ProcessAt: p1, Score: p1.Unix()}
	t2 := &ScheduledTask{ID: m2.ID, Type: m2.Type, Payload: m2.Payload, ProcessAt: p2, Score: p2.Unix()}

	tests := []struct {
		scheduled []sortedSetEntry
		want      []*ScheduledTask
	}{
		{
			scheduled: []sortedSetEntry{
				{m1, p1.Unix()},
				{m2, p2.Unix()},
			},
			want: []*ScheduledTask{t1, t2},
		},
		{
			scheduled: []sortedSetEntry{},
			want:      []*ScheduledTask{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedScheduledQueue(t, r, tc.scheduled)

		got, err := r.ListScheduled()
		if err != nil {
			t.Errorf("r.ListScheduled() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*ScheduledTask) []*ScheduledTask {
			out := append([]*ScheduledTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})

		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListScheduled() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListRetry(t *testing.T) {
	r := setup(t)
	m1 := &base.TaskMessage{
		ID:       xid.New(),
		Type:     "send_email",
		Queue:    "default",
		Payload:  map[string]interface{}{"subject": "hello"},
		ErrorMsg: "email server not responding",
		Retry:    25,
		Retried:  10,
	}
	m2 := &base.TaskMessage{
		ID:       xid.New(),
		Type:     "reindex",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "search engine not responding",
		Retry:    25,
		Retried:  2,
	}
	p1 := time.Now().Add(5 * time.Minute)
	p2 := time.Now().Add(24 * time.Hour)
	t1 := &RetryTask{
		ID:        m1.ID,
		Type:      m1.Type,
		Payload:   m1.Payload,
		ProcessAt: p1,
		ErrorMsg:  m1.ErrorMsg,
		Retried:   m1.Retried,
		Retry:     m1.Retry,
		Score:     p1.Unix(),
	}
	t2 := &RetryTask{
		ID:        m2.ID,
		Type:      m2.Type,
		Payload:   m2.Payload,
		ProcessAt: p2,
		ErrorMsg:  m2.ErrorMsg,
		Retried:   m2.Retried,
		Retry:     m2.Retry,
		Score:     p2.Unix(),
	}

	tests := []struct {
		retry []sortedSetEntry
		want  []*RetryTask
	}{
		{
			retry: []sortedSetEntry{
				{m1, p1.Unix()},
				{m2, p2.Unix()},
			},
			want: []*RetryTask{t1, t2},
		},
		{
			retry: []sortedSetEntry{},
			want:  []*RetryTask{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedRetryQueue(t, r, tc.retry)

		got, err := r.ListRetry()
		if err != nil {
			t.Errorf("r.ListRetry() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*RetryTask) []*RetryTask {
			out := append([]*RetryTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})

		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListRetry() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

func TestListDead(t *testing.T) {
	r := setup(t)
	m1 := &base.TaskMessage{
		ID:       xid.New(),
		Type:     "send_email",
		Queue:    "default",
		Payload:  map[string]interface{}{"subject": "hello"},
		ErrorMsg: "email server not responding",
	}
	m2 := &base.TaskMessage{
		ID:       xid.New(),
		Type:     "reindex",
		Queue:    "default",
		Payload:  nil,
		ErrorMsg: "search engine not responding",
	}
	f1 := time.Now().Add(-5 * time.Minute)
	f2 := time.Now().Add(-24 * time.Hour)
	t1 := &DeadTask{
		ID:           m1.ID,
		Type:         m1.Type,
		Payload:      m1.Payload,
		LastFailedAt: f1,
		ErrorMsg:     m1.ErrorMsg,
		Score:        f1.Unix(),
	}
	t2 := &DeadTask{
		ID:           m2.ID,
		Type:         m2.Type,
		Payload:      m2.Payload,
		LastFailedAt: f2,
		ErrorMsg:     m2.ErrorMsg,
		Score:        f2.Unix(),
	}

	tests := []struct {
		dead []sortedSetEntry
		want []*DeadTask
	}{
		{
			dead: []sortedSetEntry{
				{m1, f1.Unix()},
				{m2, f2.Unix()},
			},
			want: []*DeadTask{t1, t2},
		},
		{
			dead: []sortedSetEntry{},
			want: []*DeadTask{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDeadQueue(t, r, tc.dead)

		got, err := r.ListDead()
		if err != nil {
			t.Errorf("r.ListDead() = %v, %v, want %v, nil", got, err, tc.want)
			continue
		}
		sortOpt := cmp.Transformer("SortMsg", func(in []*DeadTask) []*DeadTask {
			out := append([]*DeadTask(nil), in...) // Copy input to avoid mutating it
			sort.Slice(out, func(i, j int) bool {
				return out[i].ID.String() < out[j].ID.String()
			})
			return out
		})

		if diff := cmp.Diff(tc.want, got, sortOpt, timeCmpOpt); diff != "" {
			t.Errorf("r.ListDead() = %v, %v, want %v, nil; (-want, +got)\n%s", got, err, tc.want, diff)
			continue
		}
	}
}

var timeCmpOpt = EquateApproxTime(time.Second)

func TestEnqueueDeadTask(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("gen_thumbnail", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		dead         []sortedSetEntry
		score        int64
		id           xid.ID
		want         error // expected return value from calling EnqueueDeadTask
		wantDead     []*base.TaskMessage
		wantEnqueued []*base.TaskMessage
	}{
		{
			dead: []sortedSetEntry{
				{t1, s1},
				{t2, s2},
			},
			score:        s2,
			id:           t2.ID,
			want:         nil,
			wantDead:     []*base.TaskMessage{t1},
			wantEnqueued: []*base.TaskMessage{t2},
		},
		{
			dead: []sortedSetEntry{
				{t1, s1},
				{t2, s2},
			},
			score:        123,
			id:           t2.ID,
			want:         ErrTaskNotFound,
			wantDead:     []*base.TaskMessage{t1, t2},
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDeadQueue(t, r, tc.dead)

		got := r.EnqueueDeadTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueDeadTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}
		gotEnqueuedRaw := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DefaultQueue, diff)
		}
		gotDeadRaw := r.client.ZRange(base.DeadQueue, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, gotDeadRaw)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.DeadQueue, diff)
		}
	}
}

func TestEnqueueRetryTask(t *testing.T) {
	r := setup(t)

	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("gen_thumbnail", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()
	tests := []struct {
		retry        []sortedSetEntry
		score        int64
		id           xid.ID
		want         error // expected return value from calling EnqueueRetryTask
		wantRetry    []*base.TaskMessage
		wantEnqueued []*base.TaskMessage
	}{
		{
			retry: []sortedSetEntry{
				{t1, s1},
				{t2, s2},
			},
			score:        s2,
			id:           t2.ID,
			want:         nil,
			wantRetry:    []*base.TaskMessage{t1},
			wantEnqueued: []*base.TaskMessage{t2},
		},
		{
			retry: []sortedSetEntry{
				{t1, s1},
				{t2, s2},
			},
			score:        123,
			id:           t2.ID,
			want:         ErrTaskNotFound,
			wantRetry:    []*base.TaskMessage{t1, t2},
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case

		seedRetryQueue(t, r, tc.retry) // initialize retry queue

		got := r.EnqueueRetryTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueRetryTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}
		gotEnqueuedRaw := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DefaultQueue, diff)
		}
		gotRetryRaw := r.client.ZRange(base.RetryQueue, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}

func TestEnqueueScheduledTask(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("gen_thumbnail", nil)
	s1 := time.Now().Add(-5 * time.Minute).Unix()
	s2 := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		scheduled     []sortedSetEntry
		score         int64
		id            xid.ID
		want          error // expected return value from calling EnqueueScheduledTask
		wantScheduled []*base.TaskMessage
		wantEnqueued  []*base.TaskMessage
	}{
		{
			scheduled: []sortedSetEntry{
				{t1, s1},
				{t2, s2},
			},
			score:         s2,
			id:            t2.ID,
			want:          nil,
			wantScheduled: []*base.TaskMessage{t1},
			wantEnqueued:  []*base.TaskMessage{t2},
		},
		{
			scheduled: []sortedSetEntry{
				{t1, s1},
				{t2, s2},
			},
			score:         123,
			id:            t2.ID,
			want:          ErrTaskNotFound,
			wantScheduled: []*base.TaskMessage{t1, t2},
			wantEnqueued:  []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedScheduledQueue(t, r, tc.scheduled)

		got := r.EnqueueScheduledTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.EnqueueRetryTask(%s, %d) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}
		gotEnqueuedRaw := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DefaultQueue, diff)
		}
		gotScheduledRaw := r.client.ZRange(base.ScheduledQueue, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q, (-want, +got)\n%s", base.ScheduledQueue, diff)
		}
	}
}

func TestEnqueueAllScheduledTasks(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("gen_thumbnail", nil)
	t3 := newTaskMessage("reindex", nil)

	tests := []struct {
		desc         string
		scheduled    []sortedSetEntry
		want         int64
		wantEnqueued []*base.TaskMessage
	}{
		{
			desc: "with tasks in scheduled queue",
			scheduled: []sortedSetEntry{
				{t1, time.Now().Add(time.Hour).Unix()},
				{t2, time.Now().Add(time.Hour).Unix()},
				{t3, time.Now().Add(time.Hour).Unix()},
			},
			want:         3,
			wantEnqueued: []*base.TaskMessage{t1, t2, t3},
		},
		{
			desc:         "with empty scheduled queue",
			scheduled:    []sortedSetEntry{},
			want:         0,
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedScheduledQueue(t, r, tc.scheduled)

		got, err := r.EnqueueAllScheduledTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllScheduledTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllScheduledTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
		}

		gotEnqueuedRaw := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.DefaultQueue, diff)
		}
	}
}

func TestEnqueueAllRetryTasks(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("gen_thumbnail", nil)
	t3 := newTaskMessage("reindex", nil)

	tests := []struct {
		description  string
		retry        []sortedSetEntry
		want         int64
		wantEnqueued []*base.TaskMessage
	}{
		{
			description: "with tasks in retry queue",
			retry: []sortedSetEntry{
				{t1, time.Now().Add(time.Hour).Unix()},
				{t2, time.Now().Add(time.Hour).Unix()},
				{t3, time.Now().Add(time.Hour).Unix()},
			},
			want:         3,
			wantEnqueued: []*base.TaskMessage{t1, t2, t3},
		},
		{
			description:  "with empty retry queue",
			retry:        []sortedSetEntry{},
			want:         0,
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedRetryQueue(t, r, tc.retry)

		got, err := r.EnqueueAllRetryTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllRetryTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllRetryTasks = %v, %v; want %v, nil",
				tc.description, got, err, tc.want)
		}

		gotEnqueuedRaw := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.description, base.DefaultQueue, diff)
		}
	}
}

func TestEnqueueAllDeadTasks(t *testing.T) {
	r := setup(t)
	t1 := newTaskMessage("send_email", nil)
	t2 := newTaskMessage("gen_thumbnail", nil)
	t3 := newTaskMessage("reindex", nil)

	tests := []struct {
		desc         string
		dead         []sortedSetEntry
		want         int64
		wantEnqueued []*base.TaskMessage
	}{
		{
			desc: "with tasks in dead queue",
			dead: []sortedSetEntry{
				{t1, time.Now().Add(-time.Minute).Unix()},
				{t2, time.Now().Add(-time.Minute).Unix()},
				{t3, time.Now().Add(-time.Minute).Unix()},
			},
			want:         3,
			wantEnqueued: []*base.TaskMessage{t1, t2, t3},
		},
		{
			desc:         "with empty dead queue",
			dead:         []sortedSetEntry{},
			want:         0,
			wantEnqueued: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDeadQueue(t, r, tc.dead)

		got, err := r.EnqueueAllDeadTasks()
		if err != nil {
			t.Errorf("%s; r.EnqueueAllDeadTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
			continue
		}

		if got != tc.want {
			t.Errorf("%s; r.EnqueueAllDeadTasks = %v, %v; want %v, nil",
				tc.desc, got, err, tc.want)
		}

		gotEnqueuedRaw := r.client.LRange(base.DefaultQueue, 0, -1).Val()
		gotEnqueued := mustUnmarshalSlice(t, gotEnqueuedRaw)
		if diff := cmp.Diff(tc.wantEnqueued, gotEnqueued, sortMsgOpt); diff != "" {
			t.Errorf("%s; mismatch found in %q; (-want, +got)\n%s", tc.desc, base.DefaultQueue, diff)
		}
	}
}

func TestKillRetryTask(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", nil)
	m2 := newTaskMessage("reindex", nil)
	t1 := time.Now().Add(time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		retry     []sortedSetEntry
		dead      []sortedSetEntry
		id        xid.ID
		score     int64
		want      error
		wantRetry []sortedSetEntry
		wantDead  []sortedSetEntry
	}{
		{
			retry: []sortedSetEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			dead:  []sortedSetEntry{},
			id:    m1.ID,
			score: t1.Unix(),
			want:  nil,
			wantRetry: []sortedSetEntry{
				{m2, t2.Unix()},
			},
			wantDead: []sortedSetEntry{
				{m1, time.Now().Unix()},
			},
		},
		{
			retry: []sortedSetEntry{
				{m1, t1.Unix()},
			},
			dead: []sortedSetEntry{
				{m2, t2.Unix()},
			},
			id:    m2.ID,
			score: t2.Unix(),
			want:  ErrTaskNotFound,
			wantRetry: []sortedSetEntry{
				{m1, t1.Unix()},
			},
			wantDead: []sortedSetEntry{
				{m2, t2.Unix()},
			},
		},
	}

	for _, tc := range tests {
		flushDB(t, r)
		seedRetryQueue(t, r, tc.retry)
		seedDeadQueue(t, r, tc.dead)

		got := r.KillRetryTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("(*RDB).KillRetryTask(%v, %v) = %v, want %v",
				tc.id, tc.score, got, tc.want)
			continue
		}

		gotRetryRaw := r.client.ZRangeWithScores(base.RetryQueue, 0, -1).Val()
		var gotRetry []sortedSetEntry
		for _, z := range gotRetryRaw {
			gotRetry = append(gotRetry, sortedSetEntry{
				Msg:   mustUnmarshal(t, z.Member.(string)),
				Score: int64(z.Score),
			})
		}
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.RetryQueue, diff)
		}

		gotDeadRaw := r.client.ZRangeWithScores(base.DeadQueue, 0, -1).Val()
		var gotDead []sortedSetEntry
		for _, z := range gotDeadRaw {
			gotDead = append(gotDead, sortedSetEntry{
				Msg:   mustUnmarshal(t, z.Member.(string)),
				Score: int64(z.Score),
			})
		}
		if diff := cmp.Diff(tc.wantDead, gotDead, sortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.DeadQueue, diff)
		}
	}
}

func TestKillScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", nil)
	m2 := newTaskMessage("reindex", nil)
	t1 := time.Now().Add(time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []sortedSetEntry
		dead          []sortedSetEntry
		id            xid.ID
		score         int64
		want          error
		wantScheduled []sortedSetEntry
		wantDead      []sortedSetEntry
	}{
		{
			scheduled: []sortedSetEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			dead:  []sortedSetEntry{},
			id:    m1.ID,
			score: t1.Unix(),
			want:  nil,
			wantScheduled: []sortedSetEntry{
				{m2, t2.Unix()},
			},
			wantDead: []sortedSetEntry{
				{m1, time.Now().Unix()},
			},
		},
		{
			scheduled: []sortedSetEntry{
				{m1, t1.Unix()},
			},
			dead: []sortedSetEntry{
				{m2, t2.Unix()},
			},
			id:    m2.ID,
			score: t2.Unix(),
			want:  ErrTaskNotFound,
			wantScheduled: []sortedSetEntry{
				{m1, t1.Unix()},
			},
			wantDead: []sortedSetEntry{
				{m2, t2.Unix()},
			},
		},
	}

	for _, tc := range tests {
		flushDB(t, r)
		seedScheduledQueue(t, r, tc.scheduled)
		seedDeadQueue(t, r, tc.dead)

		got := r.KillScheduledTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("(*RDB).KillScheduledTask(%v, %v) = %v, want %v",
				tc.id, tc.score, got, tc.want)
			continue
		}

		gotScheduledRaw := r.client.ZRangeWithScores(base.ScheduledQueue, 0, -1).Val()
		var gotScheduled []sortedSetEntry
		for _, z := range gotScheduledRaw {
			gotScheduled = append(gotScheduled, sortedSetEntry{
				Msg:   mustUnmarshal(t, z.Member.(string)),
				Score: int64(z.Score),
			})
		}
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.ScheduledQueue, diff)
		}

		gotDeadRaw := r.client.ZRangeWithScores(base.DeadQueue, 0, -1).Val()
		var gotDead []sortedSetEntry
		for _, z := range gotDeadRaw {
			gotDead = append(gotDead, sortedSetEntry{
				Msg:   mustUnmarshal(t, z.Member.(string)),
				Score: int64(z.Score),
			})
		}
		if diff := cmp.Diff(tc.wantDead, gotDead, sortZSetEntryOpt, timeCmpOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want,+got)\n%s",
				base.DeadQueue, diff)
		}
	}
}

func TestDeleteDeadTask(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", nil)
	m2 := newTaskMessage("reindex", nil)
	t1 := time.Now().Add(-5 * time.Minute)
	t2 := time.Now().Add(-time.Hour)

	tests := []struct {
		dead     []sortedSetEntry
		id       xid.ID
		score    int64
		want     error
		wantDead []*base.TaskMessage
	}{
		{
			dead: []sortedSetEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			id:       m1.ID,
			score:    t1.Unix(),
			want:     nil,
			wantDead: []*base.TaskMessage{m2},
		},
		{
			dead: []sortedSetEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			id:       m1.ID,
			score:    t2.Unix(), // id and score mismatch
			want:     ErrTaskNotFound,
			wantDead: []*base.TaskMessage{m1, m2},
		},
		{
			dead:     []sortedSetEntry{},
			id:       m1.ID,
			score:    t1.Unix(),
			want:     ErrTaskNotFound,
			wantDead: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDeadQueue(t, r, tc.dead)

		got := r.DeleteDeadTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteDeadTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotDeadRaw := r.client.ZRange(base.DeadQueue, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, gotDeadRaw)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadQueue, diff)
		}
	}
}

func TestDeleteRetryTask(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", nil)
	m2 := newTaskMessage("reindex", nil)
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		retry     []sortedSetEntry
		id        xid.ID
		score     int64
		want      error
		wantRetry []*base.TaskMessage
	}{
		{
			retry: []sortedSetEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			id:        m1.ID,
			score:     t1.Unix(),
			want:      nil,
			wantRetry: []*base.TaskMessage{m2},
		},
		{
			retry: []sortedSetEntry{
				{m1, t1.Unix()},
			},
			id:        m2.ID,
			score:     t2.Unix(),
			want:      ErrTaskNotFound,
			wantRetry: []*base.TaskMessage{m1},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedRetryQueue(t, r, tc.retry)

		got := r.DeleteRetryTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteRetryTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotRetryRaw := r.client.ZRange(base.RetryQueue, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}

func TestDeleteScheduledTask(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", nil)
	m2 := newTaskMessage("reindex", nil)
	t1 := time.Now().Add(5 * time.Minute)
	t2 := time.Now().Add(time.Hour)

	tests := []struct {
		scheduled     []sortedSetEntry
		id            xid.ID
		score         int64
		want          error
		wantScheduled []*base.TaskMessage
	}{
		{
			scheduled: []sortedSetEntry{
				{m1, t1.Unix()},
				{m2, t2.Unix()},
			},
			id:            m1.ID,
			score:         t1.Unix(),
			want:          nil,
			wantScheduled: []*base.TaskMessage{m2},
		},
		{
			scheduled: []sortedSetEntry{
				{m1, t1.Unix()},
			},
			id:            m2.ID,
			score:         t2.Unix(),
			want:          ErrTaskNotFound,
			wantScheduled: []*base.TaskMessage{m1},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedScheduledQueue(t, r, tc.scheduled)

		got := r.DeleteScheduledTask(tc.id, tc.score)
		if got != tc.want {
			t.Errorf("r.DeleteScheduledTask(%v, %v) = %v, want %v", tc.id, tc.score, got, tc.want)
			continue
		}

		gotScheduledRaw := r.client.ZRange(base.ScheduledQueue, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledQueue, diff)
		}
	}
}

func TestDeleteAllDeadTasks(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", nil)
	m2 := newTaskMessage("reindex", nil)
	m3 := newTaskMessage("gen_thumbnail", nil)

	tests := []struct {
		dead     []sortedSetEntry
		wantDead []*base.TaskMessage
	}{
		{
			dead: []sortedSetEntry{
				{m1, time.Now().Unix()},
				{m2, time.Now().Unix()},
				{m3, time.Now().Unix()},
			},
			wantDead: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedDeadQueue(t, r, tc.dead)

		err := r.DeleteAllDeadTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotDeadRaw := r.client.ZRange(base.DeadQueue, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, gotDeadRaw)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.DeadQueue, diff)
		}
	}
}

func TestDeleteAllRetryTasks(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", nil)
	m2 := newTaskMessage("reindex", nil)
	m3 := newTaskMessage("gen_thumbnail", nil)

	tests := []struct {
		retry     []sortedSetEntry
		wantRetry []*base.TaskMessage
	}{
		{
			retry: []sortedSetEntry{
				{m1, time.Now().Unix()},
				{m2, time.Now().Unix()},
				{m3, time.Now().Unix()},
			},
			wantRetry: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedRetryQueue(t, r, tc.retry)

		err := r.DeleteAllRetryTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotRetryRaw := r.client.ZRange(base.RetryQueue, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, gotRetryRaw)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.RetryQueue, diff)
		}
	}
}

func TestDeleteAllScheduledTasks(t *testing.T) {
	r := setup(t)
	m1 := newTaskMessage("send_email", nil)
	m2 := newTaskMessage("reindex", nil)
	m3 := newTaskMessage("gen_thumbnail", nil)

	tests := []struct {
		scheduled     []sortedSetEntry
		wantScheduled []*base.TaskMessage
	}{
		{
			scheduled: []sortedSetEntry{
				{m1, time.Now().Add(time.Minute).Unix()},
				{m2, time.Now().Add(time.Minute).Unix()},
				{m3, time.Now().Add(time.Minute).Unix()},
			},
			wantScheduled: []*base.TaskMessage{},
		},
	}

	for _, tc := range tests {
		flushDB(t, r) // clean up db before each test case
		seedScheduledQueue(t, r, tc.scheduled)

		err := r.DeleteAllScheduledTasks()
		if err != nil {
			t.Errorf("r.DeleteAllDeaadTasks = %v, want nil", err)
		}

		gotScheduledRaw := r.client.ZRange(base.ScheduledQueue, 0, -1).Val()
		gotScheduled := mustUnmarshalSlice(t, gotScheduledRaw)
		if diff := cmp.Diff(tc.wantScheduled, gotScheduled, sortMsgOpt); diff != "" {
			t.Errorf("mismatch found in %q; (-want, +got)\n%s", base.ScheduledQueue, diff)
		}
	}
}
