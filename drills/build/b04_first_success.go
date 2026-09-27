//go:build ignore

// Drill b04: first success wins (~45 min, stretch: select + context)
//
// Implement firstSuccess:
//
//   - Run every fetcher concurrently.
//   - Return the first successful result, and cancel the others through ctx.
//   - If every fetcher fails, return a non-nil error.
//   - If ctx is cancelled or times out first, return ctx.Err().
//   - Don't leak goroutines: everything you start must eventually finish,
//     even the fetchers that lose.
//
// Run: go run -race drills/build/b04_first_success.go
package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

type fetcher func(ctx context.Context) (string, error)

func firstSuccess(ctx context.Context, fetchers []fetcher) (string, error) {
	return "", nil
}

// ---- checks below: don't edit ----

var errFetch = errors.New("fetch failed")

// slow succeeds with val after d, or returns early if ctx is cancelled
// (counting the cancellation if cancelled is non-nil).
func slow(d time.Duration, val string, cancelled *atomic.Int32) fetcher {
	return func(ctx context.Context) (string, error) {
		select {
		case <-time.After(d):
			return val, nil
		case <-ctx.Done():
			if cancelled != nil {
				cancelled.Add(1)
			}
			return "", ctx.Err()
		}
	}
}

func failing(d time.Duration) fetcher {
	return func(ctx context.Context) (string, error) {
		time.Sleep(d)
		return "", errFetch
	}
}

var failures int

func check(name string, ok bool) {
	if ok {
		fmt.Println("PASS", name)
		return
	}
	failures++
	fmt.Println("FAIL", name)
}

func main() {
	before := runtime.NumGoroutine()
	bg := context.Background()

	start := time.Now()
	got, err := firstSuccess(bg, []fetcher{
		slow(50*time.Millisecond, "a", nil),
		slow(10*time.Millisecond, "b", nil),
		slow(100*time.Millisecond, "c", nil),
	})
	check("fastest wins", err == nil && got == "b")
	check("returns without waiting for the slow ones", time.Since(start) < 40*time.Millisecond)

	got, err = firstSuccess(bg, []fetcher{
		failing(5 * time.Millisecond),
		slow(20*time.Millisecond, "ok", nil),
	})
	check("failures are skipped", err == nil && got == "ok")

	_, err = firstSuccess(bg, []fetcher{failing(5 * time.Millisecond), failing(10 * time.Millisecond)})
	check("all fail -> error", err != nil)

	ctx, cancel := context.WithTimeout(bg, 20*time.Millisecond)
	start = time.Now()
	_, err = firstSuccess(ctx, []fetcher{slow(200*time.Millisecond, "late", nil)})
	cancel()
	check("respects ctx timeout", errors.Is(err, context.DeadlineExceeded))
	check("returns promptly on timeout", time.Since(start) < 100*time.Millisecond)

	var cancelled atomic.Int32
	firstSuccess(bg, []fetcher{
		slow(10*time.Millisecond, "winner", &cancelled),
		slow(200*time.Millisecond, "loser1", &cancelled),
		slow(200*time.Millisecond, "loser2", &cancelled),
	})
	time.Sleep(30 * time.Millisecond)
	check("losers are cancelled", cancelled.Load() == 2)

	time.Sleep(300 * time.Millisecond)
	check("no leaked goroutines", runtime.NumGoroutine() <= before)

	fmt.Printf("\n%d failure(s)\n", failures)
}
