// Package workerpool is the answer to exercise 1: a worker pool with bounded
// concurrency, context cancellation, and error collection.
//
// # The big picture (ELI5)
//
// Picture a bakery with a pile of orders (the jobs) and a few bakers (the
// workers). You don't hire one baker per order. With 1,000 orders you'd have
// 1,000 bakers crammed into the kitchen. Instead you hire, say, 4 bakers, and
// a manager hands out orders one at a time. When a baker finishes, they come
// back for another order. When the pile is empty, the manager says "that's
// everything," the bakers go home, and the manager waits until every baker
// has left before locking up.
//
// In this file:
//   - the bakers are goroutines (Go's lightweight threads)
//   - the manager is the Run function itself
//   - handing out an order is sending a number on a channel
//   - "that's everything" is close(channel)
//   - waiting for every baker to leave is a sync.WaitGroup
//   - "the shop is closing early!" is the context being cancelled
//
// # Design notes (the interview version)
//
//   - Fixed workers reading from a channel of job indexes. Workers never
//     outnumber jobs, so a 2-job run with 100 workers starts only 2 goroutines.
//   - Workers send nothing back. Each writes results[i] and errs[i] for the
//     indexes it received. Every index is written by exactly one goroutine,
//     and wg.Wait() happens-before the reads, so no mutex is needed and order
//     is preserved for free.
//   - The feeder (Run itself) is the only sender on the indexes channel, so
//     it is the one that closes it. Closing is what lets the workers' range
//     loops end, which is what lets wg.Wait return: no leaks.
//   - Cancellation is cooperative. Run stops handing out new jobs as soon as
//     ctx is done, but jobs already running only stop early if fn watches ctx.
//
// The alternative design is a goroutine per job, gated by a semaphore
// channel (sem <- struct{}{} before starting, <-sem when done). It's simpler
// to write, but it creates len(jobs) goroutines up front instead of `workers`.
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Run applies fn to each job using at most workers concurrent calls.
//
// The results are in job order. Jobs that never ran because ctx was
// cancelled, and jobs whose fn returned an error, hold the zero value of R.
// The returned error joins every error fn returned, plus ctx.Err() if the
// run was cancelled, so errors.Is works for any of them.
//
// A non-positive workers count is a caller bug. Run reports it as an error
// rather than panicking or silently clamping to 1.
//
// Reading the signature, piece by piece:
//   - [J, R any] makes this a generic function. J is "whatever type a job
//     is" and R is "whatever type a result is." The caller decides. With
//     jobs []string and an fn that returns an int, J is string and R is int.
//     "any" means J and R can be absolutely any type.
//   - ctx context.Context is the "stop early" signal. By convention it is
//     always the first parameter.
//   - fn is the actual work: a function that takes one job and returns one
//     result or an error. Run doesn't know or care what the work is.
//   - It returns every result, in order, plus one error that bundles up
//     everything that went wrong.
func Run[J, R any](ctx context.Context, workers int, jobs []J, fn func(context.Context, J) (R, error)) ([]R, error) {
	// Zero bakers means the orders will never get done, so refuse up front.
	// fmt.Errorf builds an error value with a formatted message. In Go,
	// errors are ordinary values you return; nothing gets "thrown."
	if workers <= 0 {
		return nil, fmt.Errorf("workerpool: workers must be positive, got %d", workers)
	}

	// Two slots for every job, set up before anyone starts working:
	// results[3] will hold job 3's answer and errs[3] will hold job 3's error.
	// make([]R, n) creates a slice of n zero values (0 for ints, "" for
	// strings, nil for pointers), so a job that never runs just keeps its
	// zero value.
	results := make([]R, len(jobs))
	errs := make([]error, len(jobs))

	// A channel is a pipe that goroutines use to hand values to each other
	// safely. This one carries job numbers (0, 1, 2, ...), not the jobs
	// themselves. Handing out index 3 means "go do jobs[3] and put the answer
	// in results[3]."
	//
	// Unbuffered on purpose: a successful send means a worker has actually
	// taken the job. With a buffer, jobs could sit queued after cancellation.
	// (Unbuffered is like handing something directly to a person: both of
	// you have to be there at the same moment. A buffered channel is like a
	// mailbox you can drop things into and walk away.)
	indexes := make(chan int)

	// A WaitGroup is a counter of "people still working." Each worker adds 1
	// when it starts and subtracts 1 when it finishes. wg.Wait() blocks until
	// the counter gets back to 0.
	var wg sync.WaitGroup

	// Start the workers.
	//   - min(workers, len(jobs)): never start more workers than there are
	//     jobs. Three orders don't need ten bakers. (min is built into Go
	//     since 1.21.)
	//   - "for range n" loops n times (Go 1.22+). There's no loop variable
	//     because we don't need one.
	//   - wg.Go(f) (Go 1.25+) does three things: adds 1 to the counter,
	//     starts f in a new goroutine, and subtracts 1 when f returns. Before
	//     1.25 you wrote wg.Add(1); go func() { defer wg.Done(); ... }().
	for range min(workers, len(jobs)) {
		wg.Go(func() {
			// "for i := range indexes" means: keep taking numbers out of the
			// channel, one at a time. If the channel is empty, wait. When the
			// channel is closed and empty, the loop ends and this worker goes
			// home. This is the baker coming back to the manager again and
			// again.
			for i := range indexes {
				// Do job i and store the answer in slot i. No lock is needed:
				// only this worker ever got index i, so nobody else writes
				// results[i] or errs[i]. Every goroutine writes to its own
				// slots and never touches anyone else's.
				results[i], errs[i] = fn(ctx, jobs[i])
			}
		})
	}

	// Now the manager hands out orders. "feed:" is a label, a name for the
	// loop below, so that "break feed" from inside the select can break out
	// of the whole for loop. A plain "break" inside a select only breaks out
	// of the select, which is a classic Go gotcha.
feed:
	for i := range jobs {
		// select chooses at random when several cases are ready, so check
		// ctx first. Otherwise a cancelled run could still hand out one more
		// job whenever a worker happens to be free.
		//
		// ctx.Err() returns nil while the context is still live, and an
		// error (context.Canceled or context.DeadlineExceeded) once it's done.
		if ctx.Err() != nil {
			break
		}

		// select is like a switch statement for channels. It waits until one
		// of its cases can go ahead, then does that one:
		//   - "indexes <- i": a free worker took job i. Move on to the next.
		//   - "<-ctx.Done()": someone cancelled while we were waiting for a
		//     free worker. Stop handing out work.
		// Without the ctx case, a cancelled Run could sit here waiting for a
		// worker for as long as the in-flight jobs take.
		select {
		case indexes <- i:
		case <-ctx.Done():
			break feed
		}
	}

	// "That's everything, go home." Closing the channel is what makes each
	// worker's for-range loop end. Forget this line and the workers wait
	// forever for more jobs. That's a goroutine leak, and wg.Wait() below
	// would never return.
	//
	// The rule of thumb: whoever sends on a channel closes it. Receivers
	// never close, since they can't know whether more values are coming.
	close(indexes)

	// Wait for every worker to finish its current job and leave. After this
	// line, no goroutine touches results or errs again, so it's safe to read
	// them. (The Go memory model guarantees that everything the workers did
	// "happens before" wg.Wait returns.)
	wg.Wait()

	// If we stopped because of cancellation, put that error first in the
	// list, so the caller can check errors.Is(err, context.Canceled).
	// append([]error{err}, errs...) builds a new slice: err, then everything
	// in errs. The "..." spreads a slice out into separate arguments.
	if err := ctx.Err(); err != nil {
		errs = append([]error{err}, errs...)
	}

	// errors.Join bundles many errors into one. It skips the nils (the jobs
	// that succeeded), and if every entry is nil it returns nil, which means
	// "no error."
	return results, errors.Join(errs...) // Join skips nils and returns nil if all are nil
}
