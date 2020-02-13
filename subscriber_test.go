// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
)

func TestSubscriber(t *testing.T) {
	r := setup(t)
	rdbClient := rdb.NewRDB(r)

	tests := []struct {
		registeredID string // ID for which cancel func is registered
		publishID    string // ID to be published
		wantCalled   bool   // whether cancel func should be called
	}{
		{"abc123", "abc123", true},
		{"abc456", "abc123", false},
	}

	for _, tc := range tests {
		called := false
		fakeCancelFunc := func() {
			called = true
		}
		cancelations := base.NewCancelations()
		cancelations.Add(tc.registeredID, fakeCancelFunc)

		subscriber := newSubscriber(rdbClient, cancelations)
		subscriber.start()

		if err := rdbClient.PublishCancelation(tc.publishID); err != nil {
			subscriber.terminate()
			t.Fatalf("could not publish cancelation message: %v", err)
		}

		// allow for redis to publish message
		time.Sleep(time.Second)

		if called != tc.wantCalled {
			if tc.wantCalled {
				t.Errorf("fakeCancelFunc was not called, want the function to be called")
			} else {
				t.Errorf("fakeCancelFunc was called, want the function to not be called")
			}
		}

		subscriber.terminate()
	}
}
