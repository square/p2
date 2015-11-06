package limit

import (
	"fmt"
	"time"
)

// TokenBucket implements the token bucket rate-limiting algorithm. Once configured with
// its parameters, users can get the current number of tokens in the bucket or attempt to
// use some tokens (non-blocking).
type TokenBucket struct {
	count   int64
	max     int64
	updated time.Time
	rate    time.Duration
}

// NewTokenBucket creates a new token bucket. The first parameters specify the inital and
// maximum token counts of this bucket. One token is added every `refillRate` intervals.
func NewTokenBucket(
	initCount int64,
	maxCount int64,
	refillRate time.Duration,
) (*TokenBucket, error) {
	if initCount < 0 || maxCount <= 0 || refillRate.Nanoseconds() <= 0 {
		return nil, fmt.Errorf("invalid parameters")
	}
	return &TokenBucket{
		count:   initCount,
		max:     maxCount,
		updated: time.Now(),
		rate:    refillRate,
	}, nil
}

func (b *TokenBucket) countTime() (int64, time.Time) {
	now := time.Now()
	tokens := b.count + now.Sub(b.updated).Nanoseconds()/b.rate.Nanoseconds()
	// Check for overflow
	if tokens < b.count || tokens > b.max {
		tokens = b.max
	}
	return tokens, now
}

// Count retrieves the number of tokens currently in the bucket.
func (b *TokenBucket) Count() int64 {
	c, _ := b.countTime()
	return c
}

// TryUse attempts to take the given number of tokens from the bucket in a non-blocking
// manner. The return value shows the current count of tokens and whether the operation
// succeeded (if it didn't succeed, the token count is unchanged).
func (b *TokenBucket) TryUse(count int64) (int64, bool) {
	existing, now := b.countTime()
	if count < 0 {
		return existing, false
	}
	if count > existing {
		return existing, false
	}
	b.count = existing - count
	b.updated = now
	return b.count, true
}
