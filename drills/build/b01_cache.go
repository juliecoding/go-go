//go:build ignore

// Drill b01: TTL cache (~30 min)
//
// Implement an in-memory string cache that is safe for concurrent use.
//
//   - Set(key, value) stores a value. Each entry expires ttl after it was last Set.
//   - Get(key) returns the value and true, or "" and false if missing or expired.
//   - Delete(key) removes the entry (no-op if missing).
//   - Len() returns the number of unexpired entries.
//
// Stretch (talk through first, then build if there's time):
//   - Expired entries are never removed from memory. How would you clean them up?
//   - If you add a background cleanup goroutine, how does it stop?
//
// Run: go run -race drills/build/b01_cache.go
package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Cache struct {
	// TODO
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{}
}

func (c *Cache) Set(key, value string) {
}

func (c *Cache) Get(key string) (string, bool) {
	return "", false
}

func (c *Cache) Delete(key string) {
}

func (c *Cache) Len() int {
	return 0
}

// ---- checks below: don't edit ----

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
	c := NewCache(time.Minute)
	c.Set("a", "1")
	v, ok := c.Get("a")
	check("set then get", ok && v == "1")

	_, ok = c.Get("missing")
	check("missing key", !ok)

	c.Set("a", "2")
	v, _ = c.Get("a")
	check("overwrite", v == "2")

	c.Delete("a")
	_, ok = c.Get("a")
	check("delete", !ok)
	c.Delete("never-set")
	check("delete missing is a no-op", true)

	short := NewCache(50 * time.Millisecond)
	short.Set("x", "1")
	time.Sleep(80 * time.Millisecond)
	_, ok = short.Get("x")
	check("entry expires after ttl", !ok)
	check("Len ignores expired entries", short.Len() == 0)

	short.Set("y", "1")
	time.Sleep(30 * time.Millisecond)
	short.Set("y", "2")
	time.Sleep(30 * time.Millisecond)
	v, ok = short.Get("y")
	check("Set refreshes ttl", ok && v == "2")

	conc := NewCache(time.Minute)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			k := strconv.Itoa(i)
			conc.Set(k, k)
			conc.Get(k)
			conc.Len()
		})
	}
	wg.Wait()
	check("concurrent sets (run with -race)", conc.Len() == 50)

	fmt.Printf("\n%d failure(s)\n", failures)
}
