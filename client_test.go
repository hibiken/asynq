package asynq

import (
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	r := setup(t)
	client := &Client{rdb: r}

	tests := []struct {
		task              *Task
		processAt         time.Time
		wantQueueSize     int64
		wantScheduledSize int64
	}{
		{
			task:              &Task{Type: "send_email", Payload: map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}},
			processAt:         time.Now(),
			wantQueueSize:     1,
			wantScheduledSize: 0,
		},
		{
			task:              &Task{Type: "send_email", Payload: map[string]interface{}{"to": "customer@gmail.com", "from": "merchant@example.com"}},
			processAt:         time.Now().Add(2 * time.Hour),
			wantQueueSize:     0,
			wantScheduledSize: 1,
		},
	}

	for _, tc := range tests {
		// clean up db before each test case.
		if err := r.client.FlushDB().Err(); err != nil {
			t.Fatal(err)
		}

		err := client.Process(tc.task, tc.processAt)
		if err != nil {
			t.Error(err)
			continue
		}

		if l := r.client.LLen(defaultQueue).Val(); l != tc.wantQueueSize {
			t.Errorf("%q has length %d, want %d", defaultQueue, l, tc.wantQueueSize)
		}

		if l := r.client.ZCard(scheduled).Val(); l != tc.wantScheduledSize {
			t.Errorf("%q has length %d, want %d", scheduled, l, tc.wantScheduledSize)
		}
	}
}
