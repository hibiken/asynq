package asynq

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/rs/xid"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestRetry(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)
	errMsg := "email server not responding"
	// t1 is a task with max-retry count reached.
	t1 := &rdb.TaskMessage{Type: "send_email", Retry: 10, Retried: 10, Queue: "default", ID: xid.New()}
	// t2 is t1 with updated error message.
	t2 := *t1
	t2.ErrorMsg = errMsg
	// t3 is a task which hasn't reached max-retry count.
	t3 := &rdb.TaskMessage{Type: "send_email", Retry: 10, Retried: 5, Queue: "default", ID: xid.New()}
	// t4 is t3 after retry.
	t4 := *t3
	t4.Retried++
	t4.ErrorMsg = errMsg

	tests := []struct {
		desc      string             // test case description
		msg       *rdb.TaskMessage   // task to retry
		err       error              // error that caused retry
		wantDead  []*rdb.TaskMessage // state "dead" queue should be in
		wantRetry []*rdb.TaskMessage // state "retry" queue should be in
	}{
		{
			desc:      "With retry exhausted task",
			msg:       t1,
			err:       fmt.Errorf(errMsg),
			wantDead:  []*rdb.TaskMessage{&t2},
			wantRetry: []*rdb.TaskMessage{},
		},
		{
			desc:      "With retry-able task",
			msg:       t3,
			err:       fmt.Errorf(errMsg),
			wantDead:  []*rdb.TaskMessage{},
			wantRetry: []*rdb.TaskMessage{&t4},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}

		retryTask(rdbClient, tc.msg, tc.err)

		deadQueue := r.ZRange(deadQ, 0, -1).Val()
		gotDead := mustUnmarshalSlice(t, deadQueue)
		if diff := cmp.Diff(tc.wantDead, gotDead, sortMsgOpt); diff != "" {
			t.Errorf("%s;\nmismatch found in %q after retryTask(); (-want, +got)\n%s", tc.desc, deadQ, diff)
		}

		retryQueue := r.ZRange(retryQ, 0, -1).Val()
		gotRetry := mustUnmarshalSlice(t, retryQueue)
		if diff := cmp.Diff(tc.wantRetry, gotRetry, sortMsgOpt); diff != "" {
			t.Errorf("%s;\nmismatch found in %q after retryTask(); (-want, +got)\n%s", tc.desc, deadQ, diff)
		}
	}
}
