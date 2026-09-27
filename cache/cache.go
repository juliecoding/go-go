// Package cache is exercise 2: a concurrency-safe in-memory cache with TTL
// expiry.
//
// The stubs only exist so the package compiles. For the purest version of
// the drill, delete everything below the package clause and start from
// nothing.
//
// Requirements
//   - Set stores a value with its own TTL; Get never returns an expired value,
//     even if the cleanup sweep hasn't reached it yet (lazy expiry on read).
//   - Safe for concurrent use by many goroutines. Passes `go test -race`.
//   - A background goroutine sweeps expired entries every cleanupInterval so
//     memory doesn't grow forever with keys that are never read again.
//   - Close stops that goroutine, is safe to call more than once, and leaves
//     no goroutine behind.
//   - The zero value of V is returned alongside false on a miss.
//
// Stretch
//   - Add a max size with LRU eviction (container/list + map).
//   - Make time testable: inject a clock, or test expiry with testing/synctest
//     so you never sleep in a test.
//   - Benchmark sync.Mutex vs. sync.RWMutex under read-heavy load.
//
// Talking points
//   - Why does Get with lazy expiry need a write lock (or not) if it deletes?
//   - When would sync.Map be the better choice here, and when is it worse?
//   - Why is a janitor goroutine without a Close method a leak?
package cache

import "time"

// Cache is a generic key-value cache with per-entry expiry.
type Cache[K comparable, V any] struct {
	// TODO
}

// New returns a Cache that sweeps expired entries every cleanupInterval.
func New[K comparable, V any](cleanupInterval time.Duration) *Cache[K, V] {
	panic("TODO")
}

// Set stores val under key until ttl has elapsed.
func (c *Cache[K, V]) Set(key K, val V, ttl time.Duration) {
	panic("TODO")
}

// Get returns the value for key and whether it was present and unexpired.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	panic("TODO")
}

// Delete removes key, if present.
func (c *Cache[K, V]) Delete(key K) {
	panic("TODO")
}

// Close stops the background cleanup goroutine.
func (c *Cache[K, V]) Close() {
	panic("TODO")
}
