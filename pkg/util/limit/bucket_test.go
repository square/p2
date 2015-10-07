package limit

import (
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {
	// Time isn't eliminated from the calculations, but with a high enough rate, it can be
	// mitigated.
	start := time.Now()
	rate := time.Hour
	b := &TokenBucket{
		count:   10,
		max:     20,
		updated: start,
		rate:    rate,
	}

	if c := b.Count(); c != 10 {
		t.Error("unexpected count:", c)
	}
	if _, ok := b.TryUse(30); ok {
		t.Error("unexpected success")
	}
	if _, ok := b.TryUse(5); !ok {
		t.Error("unexpected failure")
	}
	if c := b.Count(); c != 5 {
		t.Error("unexpected count:", c)
	}

	// Test time by varying when the bucket thinks it was last updated
	t.Log("starting time tests")
	b.updated = start.Add(-2 * rate)
	if c := b.Count(); c != 7 {
		t.Error("unexpected count:", c)
	}
	if _, ok := b.TryUse(7); !ok {
		t.Error("unexpected failure")
	}
	if b.count != 0 {
		t.Error("internal bucket state not updated. count =", b.count)
	}
	if c := b.Count(); c != 0 {
		t.Error("unexpected count:", c)
	}

	t.Log("starting bucket overflow tests")
	b.updated = start.Add(-30 * rate)
	if c := b.Count(); c != b.max {
		t.Error("unexpected count:", c)
	}
	if _, ok := b.TryUse(b.max + 1); ok {
		t.Error("unexpected success")
	}
	if _, ok := b.TryUse(b.max); !ok {
		t.Error("unexpected failure")
	}
	if c := b.Count(); c != 0 {
		t.Error("unexpected count:", c)
	}
}
