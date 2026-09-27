// Tests for the cache.
//
// Run them with:  go test -race ./answers/cache
//
// Most of these tests use synctest, the fake clock. Inside
// synctest.Test(t, func(t *testing.T) { ... }), time.Sleep(time.Hour)
// returns instantly, but time.Now() really does move forward by exactly one
// hour. That's perfect for a cache, whose whole job is "things expire after
// a while": we can test "expires after a minute" without waiting a minute.
package cache

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// The basics: put something in, get it back, overwrite it, delete it.
func TestGetSetDelete(t *testing.T) {
	// [string, int] fills in the generic types: keys are strings and values
	// are ints. 0 means "no janitor," which keeps this test simple.
	c := New[string, int](0)
	defer c.Close() // always clean up, even if the test fails partway

	// Asking for something that was never stored: we get the zero value (0)
	// and false, meaning "not found."
	if v, ok := c.Get("missing"); ok || v != 0 {
		t.Errorf("Get(missing) = %v, %v; want 0, false", v, ok)
	}

	c.Set("a", 1, 0) // ttl 0 = never expires
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Errorf("Get(a) = %v, %v; want 1, true", v, ok)
	}

	c.Set("a", 2, 0) // same key again: replaces the old value
	if v, _ := c.Get("a"); v != 2 {
		t.Errorf("after overwrite Get(a) = %v, want 2", v)
	}

	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Error("Get(a) found a deleted key")
	}
	c.Delete("never-set") // must not panic
}

// A table of "store it, wait this long, is it still there?" scenarios.
//
// Look at the first two rows: one nanosecond before the deadline it's still
// there, and exactly at the deadline it's gone. Testing right at the edges
// is where off-by-one bugs get caught. It's only possible because the fake
// clock is exact.
func TestExpiry(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		wait    time.Duration
		wantHit bool
	}{
		{"before expiry", time.Minute, time.Minute - time.Nanosecond, true},
		{"exactly at expiry", time.Minute, time.Minute, false},
		{"after expiry", time.Minute, time.Hour, false},
		{"zero TTL never expires", 0, 1000 * time.Hour, true},
		{"negative TTL never expires", -time.Second, 1000 * time.Hour, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := New[string, string](0) // no janitor: test lazy expiry alone
				defer c.Close()

				c.Set("k", "v", tt.ttl)
				time.Sleep(tt.wait) // instant on the fake clock
				if _, ok := c.Get("k"); ok != tt.wantHit {
					t.Errorf("hit = %v, want %v", ok, tt.wantHit)
				}
			})
		})
	}
}

// Putting fresh food in the fridge gets a fresh "use by" date. The old date
// doesn't carry over.
func TestOverwriteResetsTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New[string, int](0)
		defer c.Close()

		c.Set("k", 1, 10*time.Second)
		time.Sleep(8 * time.Second)
		c.Set("k", 2, 10*time.Second)
		time.Sleep(8 * time.Second) // 16s after the first Set, 8s after the second

		if v, ok := c.Get("k"); !ok || v != 2 {
			t.Errorf("Get = %v, %v; want 2, true", v, ok)
		}
	})
}

// Check that the janitor really does throw out expired entries on schedule.
//
// Len() counts everything in the fridge, expired or not, so it shows whether
// the janitor has actually cleaned up yet.
//
// synctest.Wait() means "pause until every other goroutine in the bubble is
// stuck waiting on something." Here that means "let the janitor finish the
// cleaning it just woke up to do" before we look.
func TestJanitorSweepsExpiredEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New[int, int](10 * time.Second) // janitor cleans every 10s
		defer c.Close()

		c.Set(1, 1, time.Second) // expires at 1s
		c.Set(2, 2, time.Minute) // expires at 60s
		c.Set(3, 3, 0)           // never expires

		// At 5s, key 1 has expired, but the janitor hasn't come by yet,
		// so it's still taking up space.
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if n := c.Len(); n != 3 {
			t.Fatalf("before first sweep Len = %d, want 3", n)
		}

		time.Sleep(6 * time.Second) // 11s: the 10s sweep has run
		synctest.Wait()             // let the janitor finish its sweep
		if n := c.Len(); n != 2 {
			t.Fatalf("after first sweep Len = %d, want 2", n)
		}

		time.Sleep(time.Minute) // 71s: the 70s sweep removed key 2
		synctest.Wait()
		if n := c.Len(); n != 1 {
			t.Fatalf("after later sweeps Len = %d, want 1", n)
		}
	})
}

// Close must actually send the janitor home, and calling it twice must be
// harmless.
func TestCloseStopsJanitor(t *testing.T) {
	// If Close left the janitor running, synctest.Test would fail this test
	// for leaving a goroutine behind in the bubble.
	// (That's the leak check doing its job. We don't have to count
	// goroutines ourselves.)
	synctest.Test(t, func(t *testing.T) {
		c := New[string, int](time.Second)
		c.Close()
		c.Close() // idempotent: must not panic on a closed channel

		// Still usable after Close, just without sweeping.
		c.Set("k", 1, time.Second)
		time.Sleep(time.Hour)
		// With no janitor, the expired entry is still in the fridge...
		if n := c.Len(); n != 1 {
			t.Errorf("Len = %d after Close, want 1 (no sweeping)", n)
		}
		// ...but Get still refuses to hand it out.
		if _, ok := c.Get("k"); ok {
			t.Error("Get returned an expired entry")
		}
	})
}

// TestConcurrentAccess is only meaningful under the race detector:
// go test -race ./answers/cache
//
// ELI5: 16 goroutines all grab at the fridge at once, 1,000 times each,
// randomly storing, reading, and deleting. This test has no assertions of
// its own. Its job is to create chaos while the race detector watches. If
// any method forgot to take the lock, -race reports "DATA RACE" and the test
// fails. Try deleting the RLock/RUnlock lines in Get and running it.
func TestConcurrentAccess(t *testing.T) {
	// A real (not fake) 1ms janitor, so sweeps happen in the middle of all
	// the chaos too.
	c := New[int, int](time.Millisecond)
	defer c.Close()

	var wg sync.WaitGroup
	for g := range 16 {
		// wg.Go starts a goroutine and tracks it (Go 1.25+).
		wg.Go(func() {
			for i := range 1000 {
				// % 64 squeezes everything into 64 keys, so goroutines
				// constantly collide on the same keys. Collisions are what
				// we want here.
				k := (g*1000 + i) % 64
				switch i % 3 {
				case 0:
					c.Set(k, i, time.Duration(rand.IntN(5))*time.Millisecond)
				case 1:
					c.Get(k)
				case 2:
					c.Delete(k)
				}
			}
		})
	}
	wg.Wait() // don't let the test end while goroutines are still running
}

// BenchmarkGetParallel measures read-heavy throughput, the case RWMutex is
// meant for. Try swapping in sync.Mutex and comparing.
// Run it with:  go test -bench=GetParallel -cpu=1,4,8 ./answers/cache
//
// -cpu=1,4,8 runs the benchmark three times, pretending the machine has 1,
// then 4, then 8 CPU cores. With RWMutex, reads should scale better as cores
// are added, because readers don't block each other.
func BenchmarkGetParallel(b *testing.B) {
	c := New[string, int](0)
	defer c.Close()
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprint(i)
		c.Set(keys[i], i, time.Hour)
	}

	// RunParallel starts a goroutine for each core. Each one keeps calling
	// Get until the benchmark says stop (pb.Next() returns false).
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(keys[i%len(keys)])
			i++
		}
	})
}
