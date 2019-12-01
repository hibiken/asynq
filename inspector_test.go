package asynq

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestCurrentStats(t *testing.T) {
	r := setup(t)
	inspector := &Inspector{r}
	t1 := randomTask("send_email", "default", nil)
	t2 := randomTask("send_email", "default", nil)
	t3 := randomTask("gen_export", "default", nil)
	t4 := randomTask("gen_thumbnail", "default", nil)
	t5 := randomTask("send_email", "default", nil)

	tests := []struct {
		queue      []*taskMessage
		inProgress []*taskMessage
		scheduled  []*taskMessage
		retry      []*taskMessage
		dead       []*taskMessage
		want       *Stats
	}{
		{
			queue:      []*taskMessage{t1},
			inProgress: []*taskMessage{t2, t3},
			scheduled:  []*taskMessage{t4},
			retry:      []*taskMessage{},
			dead:       []*taskMessage{t5},
			want: &Stats{
				Queued:     1,
				InProgress: 2,
				Scheduled:  1,
				Retry:      0,
				Dead:       1,
			},
		},
		{
			queue:      []*taskMessage{},
			inProgress: []*taskMessage{},
			scheduled:  []*taskMessage{t1, t2, t4},
			retry:      []*taskMessage{t3},
			dead:       []*taskMessage{t5},
			want: &Stats{
				Queued:     0,
				InProgress: 0,
				Scheduled:  3,
				Retry:      1,
				Dead:       1,
			},
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}
		for _, msg := range tc.queue {
			err := r.client.LPush(defaultQueue, mustMarshal(t, msg)).Err()
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.inProgress {
			err := r.client.LPush(inProgress, mustMarshal(t, msg)).Err()
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.scheduled {
			err := r.client.ZAdd(scheduled, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Add(time.Hour).Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.retry {
			err := r.client.ZAdd(retry, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Add(time.Hour).Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, msg := range tc.dead {
			err := r.client.ZAdd(dead, &redis.Z{
				Member: mustMarshal(t, msg),
				Score:  float64(time.Now().Unix()),
			}).Err()
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := inspector.CurrentStats()
		if err != nil {
			t.Error(err)
			continue
		}
		ignoreOpt := cmpopts.IgnoreFields(*tc.want, "Timestamp")
		if diff := cmp.Diff(tc.want, got, ignoreOpt); diff != "" {
			t.Errorf("(*Inspector).CurrentStats() = %+v, want %+v; (-want, +got)\n%s",
				got, tc.want, diff)
			continue
		}
	}
}
