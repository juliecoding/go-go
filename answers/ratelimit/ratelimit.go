// Package ratelimit is the answer to exercise 3: a token bucket rate
// limiter.
//
// # The big picture (ELI5)
//
// Picture an arcade game that costs one token per play, with a token machine
// that drips out new tokens at a steady speed (say, 10 per second) into a cup
// that only holds so many (say, 5). If the cup is full, new tokens spill on
// the floor and are lost.
//
//   - Want to play? Take a token from the cup. No token, no play.
//   - Haven't played in a while? The cup has filled up, so you can play
//     5 times in a row, quickly. That's the "burst."
//   - Playing nonstop? You can only go as fast as tokens drip in. That's
//     the "rate."
//
// That's all a token bucket is. It lets short bursts through but holds
// traffic to a steady average: the same idea behind "100 API calls per
// minute" limits.
//
// The clever part: nobody actually stands by the machine dripping tokens.
// Instead, whenever someone asks for a token, we do the math: "it's been 0.3
// seconds since anyone checked, and tokens drip at 10 per second, so 3 new
// tokens have dripped in since then." Add them, cap at the cup size, done.
// That's "lazy refill." No background goroutine, no timer, nothing running
// while nobody's playing.
//
// # Design notes (the interview version)
//
//   - No background goroutine. Each call works out how many tokens have
//     accrued since the last call (elapsed * rate), adds them, and caps the
//     total at burst. An idle limiter does no work and can't leak.
//   - Tokens are a float64 so fractional refill isn't lost. At 3 per second,
//     100ms of waiting is worth 0.3 of a token.
//   - time.Now() carries a monotonic clock reading, and Sub/Since use it, so
//     wall-clock jumps (NTP, daylight saving) can't create or destroy tokens.
//   - Wait never sleeps while holding the lock. It computes how long until a
//     token is due, unlocks, sleeps on a timer (or until ctx is done), then
//     loops and tries again, since another goroutine may have won the token.
//     That also means waiters aren't served first-come, first-served.
//
// golang.org/x/time/rate is the production version of this. It also offers
// Reserve, and its Wait returns immediately if ctx's deadline is too soon
// for a token to arrive.
package ratelimit

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// TokenBucket is a rate limiter that allows bursts of up to burst events
// and a sustained rate of rate events per second.
type TokenBucket struct {
	mu     sync.Mutex // guards every field below; lock it before touching them
	rate   float64    // tokens added per second
	burst  float64    // bucket capacity
	tokens float64
	last   time.Time // when tokens was last brought up to date
}

// NewTokenBucket returns a full bucket. It panics if rate or burst is not
// positive, since that is a programming error rather than a runtime
// condition.
//
// When to panic vs. return an error: return an error for things that can
// go wrong at runtime even when the code is correct (a network call fails,
// a user types bad input). Panic for "the programmer wrote something
// impossible," like a zero-rate limiter. It's the same reason indexing past
// the end of a slice panics.
func NewTokenBucket(rate float64, burst int) *TokenBucket {
	// NaN ("not a number") and Inf (infinity) are real float64 values that
	// sneak through a plain rate <= 0 check, because every comparison with
	// NaN is false. They'd break the math below, so reject them explicitly.
	if rate <= 0 || math.IsInf(rate, 0) || math.IsNaN(rate) || burst <= 0 {
		panic(fmt.Sprintf("ratelimit: invalid rate %v or burst %d", rate, burst))
	}
	return &TokenBucket{
		rate:   rate,
		burst:  float64(burst), // Go never converts types for you; you convert explicitly
		tokens: float64(burst), // start with a full cup
		last:   time.Now(),
	}
}

// refill brings b.tokens up to date. The caller must hold b.mu.
//
// "The caller must hold b.mu" is a common Go comment convention. refill
// doesn't lock anything itself, because the functions that call it already
// hold the lock. (Go's mutexes aren't reentrant: if refill called Lock again,
// the goroutine would wait on itself forever.)
func (b *TokenBucket) refill(now time.Time) {
	elapsed := now.Sub(b.last).Seconds() // e.g. 0.3 (seconds since last check)
	// e.g. 0.3s * 10 per second = 3 new tokens. min() caps it at the cup size.
	b.tokens = min(b.burst, b.tokens+elapsed*b.rate)
	b.last = now
}

// Allow reports whether an event may happen now, consuming a token if so.
// It never blocks.
//
// ELI5: "Is there a token in the cup? If so, take it and say yes. If not,
// say no right away." It never waits. Use it when the right response to "too
// fast" is to refuse, like an HTTP server replying 429 Too Many Requests.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill(time.Now())
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// Wait blocks until a token is available and consumes it, or returns
// ctx.Err() if ctx is done first.
//
// ELI5: "Is there a token? If not, work out when the next one will drip in,
// then take a nap until then and check again." It's also willing to give up
// if whoever asked says "never mind" (the context is cancelled).
//
// The one rule that matters most: never nap while holding the lock. If we
// did, every other goroutine calling Allow or Wait would be stuck at the
// door until our nap ended, even though they only wanted a quick look.
func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		b.refill(time.Now())
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil // got one
		}
		// How long until one full token drips in? With 0.4 tokens in the
		// cup at a rate of 10 per second, we need 0.6 more, so
		// 0.6 / 10 = 0.06s = 60ms.
		//
		// Round up so a tiny shortfall never produces a zero-length wait
		// (which would spin without letting time pass).
		wait := time.Duration(math.Ceil((1 - b.tokens) / b.rate * float64(time.Second)))
		b.mu.Unlock() // unlock BEFORE napping

		// A Timer is a one-shot alarm. It sends on timer.C once, after wait.
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			// Loop around and try again.
			// (Another goroutine may have grabbed the token while we slept,
			// which is why this is a loop and not a single check.)
		case <-ctx.Done():
			// The caller gave up. Stop the alarm so it doesn't fire for no
			// reason, then report why we're returning: context.Canceled or
			// context.DeadlineExceeded.
			timer.Stop()
			return ctx.Err()
		}
	}
}
