// Tests for the token bucket.
//
// Run them with:  go test -race ./answers/ratelimit
//
// A rate limiter is all about time ("10 per second"), so nearly every test
// runs inside synctest.Test, the fake clock. time.Sleep(time.Hour) finishes
// instantly, and time.Since(start) afterward says exactly 1h0m0s. That lets
// us assert things like "Wait took exactly 100ms" with no wiggle room and no
// flaky timing.
package ratelimit

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// countAllowed calls Allow until it returns false and reports how many
// calls succeeded.
//
// ELI5: "Keep grabbing tokens out of the cup until it's empty, and count
// how many you got." This is a test helper: a small function that keeps the
// tests short.
func countAllowed(b *TokenBucket) int {
	n := 0
	for b.Allow() { // a for loop with only a condition is Go's "while" loop
		n++
	}
	return n
}

// Each row: make a bucket, empty it, wait a while, then count how many
// tokens have dripped back in.
func TestAllow(t *testing.T) {
	tests := []struct {
		name  string
		rate  float64
		burst int
		// Drain the bucket, sleep for idle, then count what Allow permits.
		idle time.Duration
		want int
	}{
		{"no refill without time passing", 10, 5, 0, 0},
		{"refills at rate", 10, 5, 300 * time.Millisecond, 3}, // 0.3s * 10/s = 3
		// 0.5s * 3/s = 1.5 tokens. We can use 1, and the 0.5 left over is
		// kept for later rather than thrown away.
		{"fractional tokens carry over", 3, 5, 500 * time.Millisecond, 1}, // 1.5 tokens
		{"refill caps at burst", 10, 5, time.Hour, 5},                     // an hour idle still only fills the cup to 5
		// 0.5 per second = one token every 2s. At 1.999s: not yet.
		{"slow rate", 0.5, 1, 1999 * time.Millisecond, 0},
		{"slow rate after full period", 0.5, 1, 2 * time.Second, 1}, // at exactly 2s: there it is
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := NewTokenBucket(tt.rate, tt.burst)
				// A brand-new bucket should start full. This also empties it.
				if got := countAllowed(b); got != tt.burst {
					t.Fatalf("new bucket allowed %d, want a full burst of %d", got, tt.burst)
				}
				time.Sleep(tt.idle) // instant on the fake clock
				if got := countAllowed(b); got != tt.want {
					t.Errorf("after %v idle, allowed %d, want %d", tt.idle, got, tt.want)
				}
			})
		})
	}
}

// Wait should nap exactly as long as it takes for the next token, no more.
func TestWaitBlocksUntilTokenIsDue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewTokenBucket(10, 1) // one token every 100ms
		if err := b.Wait(t.Context()); err != nil {
			t.Fatal(err) // the first token is already there
		}

		// The cup is empty now, so the next Wait has to nap until the next
		// drip, which is 100ms away.
		start := time.Now()
		if err := b.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 100*time.Millisecond {
			t.Errorf("Wait took %v, want 100ms", got)
		}
	})
}

// Wait should give up when the caller says "never mind," and giving up must
// not quietly eat a token.
func TestWaitReturnsWhenContextDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewTokenBucket(1, 1)
		b.Allow() // drain: the next token is 1s away

		// WithTimeout: a context that cancels itself after 250ms. It's the
		// "I'll wait, but only this long" version of WithCancel.
		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := b.Wait(ctx)
		// A timeout reports DeadlineExceeded, not Canceled. Those are the
		// two errors a context can give you.
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
		if got := time.Since(start); got != 250*time.Millisecond {
			t.Errorf("Wait returned after %v, want 250ms", got)
		}
		// A cancelled Wait must not have consumed anything: the token that
		// arrives at 1s is still available.
		time.Sleep(750 * time.Millisecond)
		if !b.Allow() {
			t.Error("token was lost by the cancelled Wait")
		}
	})
}

// The "never nap while holding the lock" rule, tested directly.
//
// ELI5: one goroutine is napping inside Wait. If it took the lock into its
// nap with it, our Allow call below would be stuck at the door. Inside a
// synctest bubble, "everyone is stuck and nobody can ever wake up" gets
// detected as a deadlock, and the test fails instead of hanging forever.
func TestWaitDoesNotHoldLockWhileSleeping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := NewTokenBucket(1, 1)
		b.Allow()

		go b.Wait(t.Context()) // blocks for 1s
		synctest.Wait()        // until that goroutine is parked in Wait

		// If Wait slept while holding b.mu, this would deadlock and
		// synctest would fail the test.
		if b.Allow() {
			t.Error("Allow succeeded on an empty bucket")
		}
		time.Sleep(time.Second) // let the background Wait finish
		// (If we skipped that sleep, the Wait goroutine would still be
		// running when the test ends, and synctest would flag it as a leak.)
	})
}

// Lots of goroutines all waiting at once: in total, they should get tokens
// at exactly the promised rate. No faster, and no slower.
func TestConcurrentWaitersGetExactlyRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A const block: named numbers, set at compile time.
		const (
			rate    = 10
			burst   = 10
			waiters = 20
			perG    = 5 // tokens each goroutine wants
		)
		b := NewTokenBucket(rate, burst)
		var got atomic.Int64 // a counter goroutines can bump safely

		start := time.Now()
		var wg sync.WaitGroup
		for range waiters {
			wg.Go(func() {
				for range perG {
					if err := b.Wait(t.Context()); err != nil {
						// t.Error, not t.Fatal: we're on a non-test goroutine.
						t.Error(err)
						return
					}
					got.Add(1)
				}
			})
		}
		wg.Wait()

		// 100 tokens: 10 from the initial burst, then 90 at 10/s = 9s.
		if got.Load() != waiters*perG {
			t.Fatalf("got %d tokens, want %d", got.Load(), waiters*perG)
		}
		want := time.Duration(waiters*perG-burst) * time.Second / rate
		// Allow a hair of slack for float rounding in the refill math.
		// (Floats can't represent every decimal exactly, the same reason
		// 0.1 + 0.2 != 0.3 in most languages. So the total might come out a
		// few nanoseconds long.)
		if elapsed := time.Since(start); elapsed < want || elapsed > want+time.Millisecond {
			t.Errorf("took %v, want %v", elapsed, want)
		}
	})
}

// Every nonsense setting should panic straight away, not misbehave later.
func TestNewTokenBucketPanicsOnBadInput(t *testing.T) {
	tests := []struct {
		name  string
		rate  float64
		burst int
	}{
		{"zero rate", 0, 1},
		{"negative rate", -1, 1},
		{"NaN rate", math.NaN(), 1},
		{"infinite rate", math.Inf(1), 1},
		{"zero burst", 1, 0},
		{"negative burst", 1, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// How to test that something panics: recover() catches a panic,
			// and only works inside a deferred function. It returns nil if
			// nothing panicked. So "recover() == nil" here means "we expected
			// a panic and didn't get one."
			defer func() {
				if recover() == nil {
					t.Error("expected a panic")
				}
			}()
			NewTokenBucket(tt.rate, tt.burst)
		})
	}
}

// Run it with:  go test -bench=Allow -cpu=1,4,8 ./answers/ratelimit
//
// Measures how fast Allow is when many goroutines hammer it at once. The
// huge rate and burst mean the bucket never runs dry, so we're timing the
// bookkeeping and the lock rather than the limiting. Expect it to get slower
// per call as you add cores, because everyone shares one mutex. That's the
// point where people start splitting into per-key buckets.
func BenchmarkAllowParallel(b *testing.B) {
	tb := NewTokenBucket(1e9, 1e6) // 1e9 = 1,000,000,000
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}
