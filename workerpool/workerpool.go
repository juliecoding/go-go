// Package workerpool is exercise 1: a worker pool with bounded concurrency,
// context cancellation, and error collection.
//
// The stub below only exists so the package compiles and exercise 5's tests
// have something to target. For the purest version of the drill, delete
// everything below the package clause and start from nothing.
//
// Requirements
//   - Run calls fn once for each job, with at most `workers` calls in flight.
//   - Results come back in the same order as jobs, whichever finishes first.
//   - Every error fn returns is collected and returned together
//     (errors.Join), or nil if every job succeeded.
//   - If ctx is cancelled, stop starting new jobs, let in-flight jobs observe
//     the cancellation, and return promptly with an error that wraps ctx.Err().
//   - No goroutine leaks: everything Run starts has exited before Run returns.
//   - Decide what workers <= 0 means (panic? error? clamp to 1?) and be ready
//     to defend the choice.
//
// Stretch
//   - Add a fail-fast option: the first error cancels the remaining jobs.
//   - Rewrite it with golang.org/x/sync/errgroup and SetLimit, then compare.
//
// Talking points
//   - Fixed N workers reading a jobs channel vs. a goroutine per job gated by
//     a semaphore channel: what are the tradeoffs?
//   - Who closes each channel, and why never the receiver?
//   - How do results stay ordered without a mutex? (Hint: each index is
//     written by exactly one goroutine.)
package workerpool

import (
	"context"
	"errors"
)

func Run[J, R any](ctx context.Context, workers int, jobs []J, fn func(context.Context, J) (R, error)) ([]R, error) {
	if workers <= 0 {
		return nil, errors.New("Hey, workers needs to be 1 or greater")
	}

	return nil, nil
}
