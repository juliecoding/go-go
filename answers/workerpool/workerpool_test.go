// This file is the answer to exercise 5: a table-driven test suite for the
// worker pool.
//
// Run it with:  go test -race -count=10 ./answers/workerpool
//
// # How Go tests work (ELI5)
//
//   - Any file ending in _test.go is a test file. `go build` ignores it and
//     `go test` runs it.
//   - Any function named TestXxx(t *testing.T) is a test. The t is your
//     remote control: t.Errorf says "this is wrong, but keep going," and
//     t.Fatalf says "this is wrong, stop this test right now."
//   - There are no assert functions built in. You write a plain if, and call
//     t.Errorf when the answer is wrong. That's the Go way.
//   - This file is in the same package (workerpool), so it can see
//     unexported (lowercase) names as well as exported ones.
//   - -race turns on the race detector, which watches for two goroutines
//     touching the same memory at the same time without coordinating.
//     -count=10 runs every test 10 times, because concurrency bugs often
//     only show up once in a while.
//
// # The fake clock (synctest)
//
// Most tests run inside testing/synctest bubbles. Inside a bubble, time is
// fake: time.Sleep returns as soon as every goroutine in the bubble is
// blocked, and the clock jumps forward. That makes timing assertions exact
// ("this took precisely 3s") and instant. synctest.Test also fails the test
// if goroutines are left blocked when the test function returns, so every
// test here doubles as a goroutine-leak check.
//
// ELI5: it's like watching a movie where the projector skips every scene in
// which everyone is asleep. A test that "waits an hour" finishes instantly,
// and the clock on the wall still says exactly one hour passed. And if the
// movie ends with someone still stuck waiting in a room, synctest complains.
// That stuck someone is a leaked goroutine.
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// A "sentinel" error: one specific, named error value that tests (and
// callers) can look for with errors.Is. Think of it as a labeled jar. However
// many other errors it gets wrapped inside, errors.Is can still find it.
var errBoom = errors.New("boom")

// TestRun is a table-driven test, the most common test style in Go.
//
// ELI5: instead of writing five nearly identical tests, you write a list of
// "scenarios" (the table) and one loop that runs every scenario the same way.
// Adding a new case is just adding one more row to the table.
func TestRun(t *testing.T) {
	// A tiny job function a lot of the cases share: take n, return n*2.
	// The _ means "there's a parameter here, but I'm not going to use it."
	double := func(_ context.Context, n int) (int, error) { return n * 2, nil }

	// The table. []struct{...}{...} declares an anonymous struct type and a
	// slice of values of that type in one go. Each {...} below is one row.
	tests := []struct {
		name    string
		workers int
		jobs    []int
		fn      func(context.Context, int) (int, error)

		want         []int
		wantErr      error // checked with errors.Is; nil means no error expected
		wantErrCount int   // how many errors the joined error should contain
	}{
		{
			name:    "no jobs",
			workers: 3,
			jobs:    nil,
			fn:      double,
			want:    []int{},
		},
		{
			name:    "all succeed in job order",
			workers: 3,
			jobs:    []int{1, 2, 3, 4, 5, 6},
			// Earlier jobs sleep longer, so they finish last. Order has to
			// come from the indexes, not from completion order.
			// (Job 1 sleeps 9s and job 6 sleeps 4s. If Run stored results in
			// the order jobs *finished*, this test would catch it.)
			fn: func(_ context.Context, n int) (int, error) {
				time.Sleep(time.Duration(10-n) * time.Second)
				return n * 2, nil
			},
			want: []int{2, 4, 6, 8, 10, 12},
		},
		{
			name:    "some fail and every error is reported",
			workers: 2,
			jobs:    []int{1, 2, 3, 4},
			// Odd jobs fail. %w in fmt.Errorf "wraps" errBoom inside a new
			// error that has a more helpful message, like putting the labeled
			// jar inside a bigger box with a note on it. errors.Is can still
			// find the jar.
			fn: func(_ context.Context, n int) (int, error) {
				if n%2 == 1 {
					return 0, fmt.Errorf("job %d: %w", n, errBoom)
				}
				return n * 2, nil
			},
			want:         []int{0, 4, 0, 8}, // failed jobs leave a zero value behind
			wantErr:      errBoom,
			wantErrCount: 2,
		},
		{
			name:    "one worker",
			workers: 1,
			jobs:    []int{1, 2, 3},
			fn:      double,
			want:    []int{2, 4, 6},
		},
		{
			name:    "more workers than jobs",
			workers: 50,
			jobs:    []int{1, 2},
			fn:      double,
			want:    []int{2, 4},
		},
	}

	for _, tt := range tests {
		// Since Go 1.22 each iteration gets its own tt, so parallel subtests
		// no longer need the old `tt := tt` line.
		// (Before 1.22, every iteration shared ONE tt variable. Parallel
		// subtests start later, after the loop has moved on, so they'd all
		// see the last row. That was one of Go's most famous gotchas.)
		//
		// t.Run makes a named subtest. You can run just one with
		// go test -run 'TestRun/one_worker'
		t.Run(tt.name, func(t *testing.T) {
			// t.Parallel says "this subtest can run at the same time as the
			// other subtests." Safe here because the rows share nothing.
			t.Parallel()

			// Everything inside this function runs on the fake clock.
			synctest.Test(t, func(t *testing.T) {
				// t.Context() is a context that's cancelled automatically
				// when the test ends. Handy, and new in Go 1.24.
				got, err := Run(t.Context(), tt.workers, tt.jobs, tt.fn)

				// slices.Equal compares two slices element by element.
				// (You can't use == on slices in Go.)
				if !slices.Equal(got, tt.want) {
					t.Errorf("results = %v, want %v", got, tt.want)
				}
				if tt.wantErr == nil {
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					return
				}
				// errors.Is digs through all the wrapping, looking for errBoom.
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want one wrapping %v", err, tt.wantErr)
				}
				// errors.Join returns a value with Unwrap() []error.
				//
				// err.(SomeInterface) is a "type assertion": "I believe this
				// error also has an Unwrap() []error method. Give me a view of
				// it that lets me call that method." The ", ok" form doesn't
				// panic if we're wrong; it just sets ok to false.
				joined, ok := err.(interface{ Unwrap() []error })
				if !ok {
					t.Fatalf("err is %T, want a joined error", err)
				}
				if n := len(joined.Unwrap()); n != tt.wantErrCount {
					t.Errorf("got %d errors, want %d: %v", n, tt.wantErrCount, err)
				}
			})
		})
	}
}

// Zero or negative workers should be refused, and no work should happen.
func TestRunInvalidWorkers(t *testing.T) {
	for _, workers := range []int{0, -1} {
		// fmt.Sprint(workers) turns the number into the subtest's name,
		// like "0" or "-1".
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			got, err := Run(t.Context(), workers, []int{1}, func(context.Context, int) (int, error) {
				// A tripwire: if this ever runs, the test fails.
				t.Error("fn should not be called")
				return 0, nil
			})
			if err == nil {
				t.Fatal("expected an error")
			}
			if got != nil {
				t.Errorf("results = %v, want nil", got)
			}
		})
	}
}

// Prove the pool never runs more than `workers` jobs at once.
//
// ELI5: every job raises its hand when it starts and lowers it when it's
// done. We keep a note of the most hands that were ever up at the same time.
// That number must equal the number of workers: no more, and (since there's
// plenty of work) no less.
func TestRunRespectsConcurrencyLimit(t *testing.T) {
	const numJobs = 20
	for _, workers := range []int{1, 3, 8, 50} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// atomic.Int64 is a number that many goroutines can safely
				// change at once without a mutex. Each Add/Load/
				// CompareAndSwap happens in one indivisible step.
				var inFlight, peak atomic.Int64
				fn := func(_ context.Context, n int) (int, error) {
					cur := inFlight.Add(1) // hand up; cur is how many are up now
					// defer runs this line when fn returns, no matter how it
					// returns. Hand down.
					defer inFlight.Add(-1)

					for { // record the highest value seen, without a mutex
						// CompareAndSwap(old, cur) means: "if peak still equals
						// old, set it to cur; otherwise do nothing and tell me
						// I lost." If another goroutine changed peak in the
						// meantime, go around and try again. This retry loop
						// is the standard lock-free "update the max" pattern.
						old := peak.Load()
						if cur <= old || peak.CompareAndSwap(old, cur) {
							break
						}
					}
					time.Sleep(time.Second) // pretend to work (on the fake clock)
					return n, nil
				}

				start := time.Now()
				if _, err := Run(t.Context(), workers, make([]int, numJobs), fn); err != nil {
					t.Fatal(err)
				}

				wantPeak := min(workers, numJobs)
				if got := peak.Load(); got != int64(wantPeak) {
					t.Errorf("peak concurrency = %d, want %d", got, wantPeak)
				}
				// With fake time this is exact: ceil(jobs/workers) rounds of 1s.
				// e.g. 20 jobs with 3 workers = 7 rounds = exactly 7s.
				// (a+b-1)/b is the integer-math trick for "a divided by b,
				// rounded up.")
				wantElapsed := time.Duration((numJobs+wantPeak-1)/wantPeak) * time.Second
				if got := time.Since(start); got != wantElapsed {
					t.Errorf("took %v, want %v", got, wantElapsed)
				}
			})
		})
	}
}

// Cancelling partway through should stop the pool quickly and cleanly.
func TestRunCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// WithCancel gives you a new context plus a cancel function. Calling
		// cancel() is pressing the "stop!" button. "defer cancel()" is a
		// habit worth building: always release a context when you're done
		// with it, or its resources leak.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var started atomic.Int64
		// A well-behaved job: it takes 1 second, but it quits early if the
		// stop button is pressed. select waits for whichever happens first.
		fn := func(ctx context.Context, n int) (int, error) {
			started.Add(1)
			select {
			case <-time.After(time.Second): // finished the work
				return n, nil
			case <-ctx.Done(): // told to stop
				return 0, ctx.Err()
			}
		}

		// 10 jobs, 2 workers, 1s each. Cancelling at 1.5s means round one
		// (jobs 0 and 1) finishes, round two (jobs 2 and 3) is interrupted,
		// and jobs 4 through 9 never start.
		//
		// time.AfterFunc(d, f) is an alarm clock: "in d, call f." Here it
		// presses the stop button 1.5 seconds in.
		time.AfterFunc(1500*time.Millisecond, cancel)
		start := time.Now()
		got, err := Run(ctx, 2, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fn)

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		// Exactly 1.5s on the fake clock. On a real clock you'd have to
		// allow some wiggle room, and the test would really take 1.5s.
		if elapsed := time.Since(start); elapsed != 1500*time.Millisecond {
			t.Errorf("Run returned after %v, want 1.5s", elapsed)
		}
		if n := started.Load(); n != 4 {
			t.Errorf("%d jobs started, want 4", n)
		}
		// Only jobs 0 and 1 finished. Their results are 0 and 1, and every
		// other slot keeps its zero value.
		if want := []int{0, 1, 0, 0, 0, 0, 0, 0, 0, 0}; !slices.Equal(got, want) {
			t.Errorf("results = %v, want %v", got, want)
		}
	})
}

// If the stop button was pressed before Run even starts, no job should run.
func TestRunAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // pressed before we begin

	// When a worker is idle and ctx is done, both select cases are ready and
	// Go picks one at random. Without Run's ctx.Err() check before the
	// select, fn would get called about half the time. Repeating makes that
	// bug fail reliably instead of intermittently.
	// (Flipping a coin once might come up heads by luck. Flipping it 50
	// times and getting heads every time basically never happens.)
	for range 50 {
		_, err := Run(ctx, 4, []int{1, 2, 3}, func(context.Context, int) (int, error) {
			// t.Error, not t.Fatal: this runs on a worker goroutine, and
			// Fatal may only be called from the test's own goroutine.
			t.Error("fn should not be called when ctx is already cancelled")
			return 0, nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	}
}

// FuzzRun checks invariants that should hold for any input size and worker
// count. Run it with:  go test -fuzz=FuzzRun ./answers/workerpool
//
// ELI5: fuzzing is letting a robot invent inputs. You give it a few examples
// (f.Add), and it mutates them into thousands of new ones, looking for
// anything that breaks a rule you wrote down. If it finds one, it saves that
// input under testdata/ so it becomes a regular test forever after.
// Plain `go test` (without -fuzz) only runs the f.Add examples.
func FuzzRun(f *testing.F) {
	// The starting examples: (workers, numJobs).
	f.Add(uint8(3), uint8(10))
	f.Add(uint8(1), uint8(0))
	f.Add(uint8(0), uint8(5))

	// uint8 keeps the numbers between 0 and 255, so the robot can't ask for
	// a billion goroutines.
	f.Fuzz(func(t *testing.T, workers, numJobs uint8) {
		jobs := make([]int, numJobs)
		for i := range jobs {
			jobs[i] = i
		}
		got, err := Run(t.Context(), int(workers), jobs, func(_ context.Context, n int) (int, error) {
			return n + 1, nil
		})

		// The rules that must always hold, whatever the inputs:
		// 1. zero workers is an error
		if workers == 0 {
			if err == nil {
				t.Fatal("expected an error for zero workers")
			}
			return
		}
		// 2. otherwise, no error
		if err != nil {
			t.Fatal(err)
		}
		// 3. exactly one result per job
		if len(got) != len(jobs) {
			t.Fatalf("len(results) = %d, want %d", len(got), len(jobs))
		}
		// 4. every result is in the right slot
		for i, v := range got {
			if v != i+1 {
				t.Fatalf("results[%d] = %d, want %d", i, v, i+1)
			}
		}
	})
}

// BenchmarkRun compares worker counts on a CPU-bound job.
// Run it with:  go test -bench=Run -benchmem ./answers/workerpool
//
// ELI5: a benchmark is a stopwatch. Go runs the loop body over and over and
// reports how long one run takes (ns/op). -benchmem also reports how much
// memory each run allocates. Expect more workers to help up to about the
// number of CPU cores you have, and then stop helping, since this work is
// pure CPU with nothing to wait on.
func BenchmarkRun(b *testing.B) {
	jobs := make([]int, 256)
	for i := range jobs {
		jobs[i] = i
	}
	// Busywork that keeps a CPU core busy for a little while.
	work := func(_ context.Context, n int) (int, error) {
		sum := 0
		for i := range 10_000 { // underscores in numbers are just for readability
			sum += i ^ n // ^ is XOR here, not "to the power of"
		}
		return sum, nil
	}

	for _, workers := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			for b.Loop() { // Go 1.24+: replaces for i := 0; i < b.N; i++
				if _, err := Run(context.Background(), workers, jobs, work); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
