# Go Concurrency Glossary

## Core ideas
- **Concurrency:** structuring a program as independently running tasks. It's about *dealing with* many things at once.
- **Parallelism:** actually *executing* things at the same instant on multiple CPU cores. Concurrency makes parallelism possible but doesn't guarantee it.
- **Thread (OS thread):** a unit of execution scheduled by the operating system. Relatively heavy (about 1–8 MB of stack), and switching between threads is expensive.
- **Goroutine:** Go's lightweight thread, managed by the Go runtime rather than the OS. Its stack starts around 2 KB, so running hundreds of thousands is normal.
- **Scheduler (GMP model):** the Go runtime piece that maps many goroutines (**G**) onto a few OS threads (**M**), using logical processors (**P**) that each hold a queue of runnable goroutines.
- **GOMAXPROCS:** the number of Ps, meaning how many goroutines can run Go code truly in parallel. It defaults to the number of CPU cores.
- **Preemption:** the scheduler pausing a running goroutine so others get a turn. Go has done this asynchronously since 1.14, so a tight loop can't hog a core forever.
- **Work stealing:** an idle P takes runnable goroutines from a busy P's queue, which keeps all cores busy.
- **Context switch:** saving one task's state and loading another's. It's far cheaper between goroutines than between OS threads.

## Channels
- **Channel:** a typed pipe goroutines use to pass values to each other safely.
- **Unbuffered channel:** a send blocks until a receiver takes the value, so both sides meet at that moment. This is a *synchronization point*.
- **Buffered channel:** has capacity N. Sends only block when it's full, and receives only block when it's empty.
- **Send / receive:** `ch <- v` puts a value in and `v := <-ch` takes one out.
- **Directional channel:** `chan<- T` can only be sent on and `<-chan T` can only be received from. Using them in function signatures documents intent.
- **Close:** `close(ch)` signals that no more values are coming. Receivers drain whatever's left, then get the zero value with `ok == false`.
- **Closed-channel rules:** sending on a closed channel panics, closing it twice panics, and receiving from it returns the zero value immediately.
- **Nil channel:** sending or receiving on it blocks forever. In a `select`, setting a channel variable to nil is a handy way to switch off that case.
- **`range` over a channel:** receives values until the channel is closed and drained.
- **`select`:** waits on several channel operations and runs whichever is ready first. If several are ready, it picks one at random.
- **`default` case:** makes a `select` non-blocking. If nothing is ready, the `default` branch runs.

## The `sync` package
- **Mutex (`sync.Mutex`):** a lock that lets one goroutine at a time into a *critical section*.
- **Critical section:** code that touches shared state and must not run in two goroutines at once.
- **RWMutex:** many readers *or* one writer at a time. It pays off for read-heavy data.
- **Reentrant lock:** a lock the same thread can take twice. Go's mutexes are **not** reentrant, so locking twice deadlocks.
- **WaitGroup:** a counter for "how many tasks are still running." `Wait()` blocks until it reaches zero, and `wg.Go(f)` (Go 1.25+) combines Add, `go`, and Done.
- **`sync.Once`:** guarantees a function runs exactly once, even if many goroutines call it.
- **`sync.Cond`:** lets goroutines wait for a condition and be woken with Signal or Broadcast. Rarely needed, since channels usually do the job.
- **`sync.Map`:** a concurrent map tuned for write-once/read-many keys, or goroutines using disjoint keys. Usually a plain map plus a mutex is better.
- **`sync.Pool`:** a cache of reusable temporary objects that cuts down on allocations. Its contents can be discarded at any garbage collection.
- **Atomic operation (`sync/atomic`):** an indivisible read or modify on a single value. It needs no lock, but only works for simple values like counters and flags.
- **Compare-and-swap (CAS):** atomically "set this to new *only if* it still equals old." It's the building block of lock-free code.

## Bugs and hazards
- **Race condition:** the result depends on the timing of goroutines, for example two goroutines doing read-modify-write on one counter.
- **Data race:** two goroutines access the same memory at the same time, at least one of them writes, and nothing synchronizes them. The Go memory model says the behavior is undefined.
- **Race detector:** the `-race` flag. It instruments memory accesses and reports data races as they happen at runtime. It only catches races on code paths that actually run.
- **Deadlock:** goroutines each waiting on something that will never happen, often each other's locks. Go crashes with "all goroutines are asleep" if *every* goroutine is stuck.
- **Livelock:** goroutines keep running and reacting to each other but make no progress, like two people repeatedly stepping aside in a hallway.
- **Starvation:** a goroutine never gets the resource it needs because others keep winning it.
- **Goroutine leak:** a goroutine blocked forever, for example on a channel nobody will ever send to. Leaks never get cleaned up and pile up over time.
- **Lock contention:** many goroutines fighting over one lock, so they spend their time waiting instead of working.
- **Priority inversion:** high-priority work stuck waiting on a lock held by low-priority work.
- **Thundering herd:** many waiters woken at once, all rushing for a resource only one of them can get.

## Memory model
- **Memory model:** the rules for when a write in one goroutine is guaranteed to be visible to a read in another.
- **Happens-before:** a guaranteed ordering between two events. If A happens-before B, then B sees A's effects. Channel operations, locks, and `wg.Wait` all create these orderings.
- **Synchronization:** anything that creates a happens-before edge between goroutines. Without it, you get no visibility guarantees at all.
- **Visibility:** whether one goroutine's writes can be seen by another. It's not automatic, because CPUs and compilers cache values and reorder instructions.

## Context
- **`context.Context`:** carries cancellation signals, deadlines, and request-scoped values across API boundaries and goroutines.
- **Cancellation:** telling in-progress work to stop. It's *cooperative*: code has to check `ctx.Done()` for it to have any effect.
- **Deadline / timeout:** a context that cancels itself at a set time (WithDeadline) or after a set duration (WithTimeout).
- **Cancellation propagation:** cancelling a parent context automatically cancels every context derived from it.
- **`ctx.Err()`:** returns nil while the context is live, and context.Canceled or context.DeadlineExceeded once it's done.
- **Cause:** with `WithCancelCause` and `context.Cause`, you can attach a specific reason for the cancellation.

## Patterns
- **Worker pool:** a fixed number of goroutines pulling jobs from a shared channel. *(See `answers/workerpool`.)*
- **Fan-out:** several goroutines reading from one channel to spread the work.
- **Fan-in:** merging several channels into one.
- **Pipeline:** stages connected by channels, where each stage transforms values and passes them on.
- **Semaphore:** limits concurrency to N. In Go, it's often a buffered channel of size N: send to acquire a slot, receive to release it.
- **errgroup (`x/sync/errgroup`):** a WaitGroup that also collects the first error and cancels the other goroutines' shared context. `SetLimit` bounds how many run at once.
- **Producer/consumer:** one side makes work and the other side processes it, with a channel between them as the queue.
- **Backpressure:** a slow consumer naturally slows the producer down because the channel fills up. It's a feature, not a bug.
- **Rate limiting:** capping how often something can happen, for example with a token bucket. *(See `answers/ratelimit`.)*
- **Debounce / throttle:** debounce acts only after events stop arriving for a while; throttle acts at most once per time window.
- **Singleflight (`x/sync/singleflight`):** collapses duplicate concurrent calls with the same key into one call, and everyone waiting shares the result.
- **Graceful shutdown:** stop accepting new work, finish in-flight work, then exit. *(See `answers/server`.)*
- **Idempotency:** doing an operation twice has the same effect as doing it once. This is what makes retries safe.

## Go proverbs
- **"Don't communicate by sharing memory; share memory by communicating."** Prefer handing data off over channels to having goroutines share it under locks.
- **"Channels orchestrate; mutexes serialize."** Use channels to coordinate work between goroutines, and a mutex to protect a piece of state.
- **"Never start a goroutine without knowing how it will stop."** Every goroutine needs an exit path, or it leaks.
