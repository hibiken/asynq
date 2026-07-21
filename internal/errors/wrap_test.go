package errors

import (
	"fmt"
	stderrors "errors"
	"testing"
)

func TestEWrapsUnderlyingError(t *testing.T) {
	base := stderrors.New("i/o timeout")
	err := E(Op("rdb.Enqueue"), Unknown, fmt.Errorf("redis eval error: %w", base))
	if !stderrors.Is(err, base) {
		t.Fatalf("errors.Is failed: %v", err)
	}
	if got := err.Error(); got != "UNKNOWN: redis eval error: i/o timeout" {
		t.Fatalf("Error() = %q", got)
	}
}
