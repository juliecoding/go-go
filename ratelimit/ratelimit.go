// Package ratelimit is exercise 3: a token bucket rate limiter.
//
// The stubs only exist so the package compiles. For the purest version of
// the drill, delete everything below the package clause and start from
// nothing.
//
// Requirements
//   - The bucket holds at most `burst` tokens and starts full.
//   - Tokens refill continuously at `rate` per second. Compute the refill
//     lazily from elapsed time on each call rather than running a ticker
//     goroutine.
//   - Allow takes a token if one is available and reports whether it did.
//     It never blocks.
//   - Wait blocks until a token is available or ctx is done, returning
//     ctx.Err() in the second case. It must not hold the lock while waiting.
//   - Safe for concurrent use. Passes `go test -race`.
//
// Stretch
//   - Per-key limiting (one bucket per client IP) that evicts idle buckets.
//   - HTTP middleware that responds 429 with a Retry-After header. Wire it
//     into exercise 4.
//   - Compare your API with golang.org/x/time/rate (Allow, Wait, Reserve).
//
// Talking points
//   - Token bucket vs. leaky bucket vs. fixed window vs. sliding window.
//   - Why lazy refill beats a ticker goroutine (no leak, no drift, no work
//     when idle).
//   - Why use time.Since on a monotonic clock reading instead of comparing
//     wall-clock times?
package ratelimit

import "context"

// TokenBucket is a rate limiter that allows bursts of up to burst events
// and a sustained rate of rate events per second.
type TokenBucket struct {
	// TODO
}

// NewTokenBucket returns a full bucket.
func NewTokenBucket(rate float64, burst int) *TokenBucket {
	panic("TODO")
}

// Allow reports whether an event may happen now, consuming a token if so.
func (b *TokenBucket) Allow() bool {
	panic("TODO")
}

// Wait blocks until a token is available or ctx is done.
func (b *TokenBucket) Wait(ctx context.Context) error {
	panic("TODO")
}
