package timeutil

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestSleep(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		sleep   time.Duration
		wantErr error
	}{
		{
			name:    "success",
			timeout: 30 * time.Millisecond,
			sleep:   10 * time.Millisecond,
			wantErr: nil,
		},
		{
			name:    "timeout",
			timeout: 10 * time.Millisecond,
			sleep:   30 * time.Millisecond,
			wantErr: context.DeadlineExceeded,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			wg := sync.WaitGroup{}
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := Sleep(ctx, tc.sleep)
				require.ErrorIs(t, tc.wantErr, err)
			}()
			time.Sleep(20 * time.Millisecond)
			cancel()
			wg.Wait()
		})
	}
}
