package timeutil

import (
	"context"
	"time"
)

// Sleep will wait for the specified duration or return on context
// expiration.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
