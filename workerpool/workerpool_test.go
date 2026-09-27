// Exercise 5: a table-driven test suite for the exercise 1 worker pool.
//
// Run it with:  go test -race -count=10 ./workerpool
//
// Table cases (one table-driven test, a t.Run subtest per case), at least:
//   - no jobs
//   - every job succeeds, and results come back in job order
//   - some jobs fail: every error is reported, and errors.Is finds a
//     sentinel error through the joined result
//   - workers == 1 (strictly sequential)
//   - workers > len(jobs)
//
// Requirements
//   - Mark the subtests t.Parallel() where it's safe, and know why the
//     loop-variable capture problem no longer bites here as of Go 1.22.
//   - Concurrency bound: track in-flight calls with an atomic counter and
//     assert the observed maximum never exceeds workers.
//   - Cancellation: cancel ctx partway through and assert Run returns
//     promptly with an error where errors.Is(err, context.Canceled) is true.
//     Use testing/synctest (Go 1.25+) so the test never actually sleeps.
//   - Leaks: prove Run leaves no goroutines behind (synctest reports
//     leftover goroutines, or use go.uber.org/goleak).
//   - A benchmark using b.Loop() that compares several worker counts via
//     b.Run subbenchmarks.
//
// Stretch
//   - A fuzz test (testing.F) asserting len(results) == len(jobs) for
//     arbitrary inputs.
//   - Use t.Cleanup instead of defer for shared setup, and say why it's
//     better with parallel subtests.
//
// Talking points
//   - Why -count=10 together with -race? What does the race detector catch,
//     and what can it miss?
//   - Testing timing-dependent code without time.Sleep.
package workerpool

import "testing"

func TestRun(t *testing.T) {
	t.Skip("TODO")
}
