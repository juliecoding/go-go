//go:build ignore

// Drill b02: fetch with a concurrency limit (~35 min)
//
// Implement fetchAll:
//
//   - Call fetch once for every url.
//   - Run at most `limit` fetches at the same time (but do run them concurrently).
//   - Return the results in the same order as urls.
//   - If any fetch fails, return a non-nil error that wraps the original
//     (any one of the errors is fine).
//
// Stretch (talk through first):
//   - After the first error, stop starting new fetches.
//   - How would a caller cancel the whole thing? (hint: context)
//
// Run: go run -race drills/build/b02_fetch_limit.go
package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

func fetchAll(urls []string, limit int, fetch func(url string) (int, error)) ([]int, error) {
	return nil, nil
}

// ---- checks below: don't edit ----

var errBadURL = errors.New("bad url")

// newFakeFetch returns a fetch func that sleeps, returns len(url), fails for
// urls starting with "bad", and records the peak number of concurrent calls.
func newFakeFetch(delay time.Duration) (func(string) (int, error), func() int32) {
	var inFlight, peak atomic.Int32
	fetch := func(url string) (int, error) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(delay)
		if strings.HasPrefix(url, "bad") {
			return 0, fmt.Errorf("fetch %s: %w", url, errBadURL)
		}
		return len(url), nil
	}
	return fetch, peak.Load
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
	urls := []string{"a", "bb", "ccc", "dddd", "eeeee", "ffffff", "g", "hh", "iii", "jjjj"}
	want := []int{1, 2, 3, 4, 5, 6, 1, 2, 3, 4}

	fetch, peak := newFakeFetch(20 * time.Millisecond)
	start := time.Now()
	got, err := fetchAll(urls, 3, fetch)
	elapsed := time.Since(start)

	check("no error", err == nil)
	check("results in input order", slices.Equal(got, want))
	check("never more than 3 in flight", peak() <= 3)
	check("actually concurrent (3 in flight at peak)", peak() == 3)
	check("faster than sequential", elapsed < 150*time.Millisecond)

	fetch, _ = newFakeFetch(5 * time.Millisecond)
	_, err = fetchAll([]string{"ok", "bad1", "ok2", "bad2"}, 2, fetch)
	check("returns an error when a fetch fails", err != nil)
	check("error wraps the original (errors.Is)", errors.Is(err, errBadURL))

	fetch, _ = newFakeFetch(5 * time.Millisecond)
	got, err = fetchAll(nil, 3, fetch)
	check("empty input", err == nil && len(got) == 0)

	fmt.Printf("\n%d failure(s)\n", failures)
}
