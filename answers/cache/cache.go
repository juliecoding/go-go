// Package cache is the answer to exercise 2: a concurrency-safe in-memory
// cache with TTL expiry.
//
// # The big picture (ELI5)
//
// A cache is a fridge. You put food in (Set) with a "use by" date (the TTL,
// "time to live"), and you take food out (Get). Two rules:
//
//  1. Never hand anyone expired food, even if it's still sitting in the
//     fridge. Get checks the date every time.
//  2. Once in a while, someone cleans out the fridge and throws the expired
//     stuff away, so it doesn't pile up forever. That's the "janitor," a
//     background goroutine that wakes up on a timer.
//
// Lots of people use the fridge at once (goroutines), so there's a rule for
// the door, too: any number of people can look inside at the same time, but
// when someone is putting things in or taking things out, everyone else waits.
// That's exactly what a sync.RWMutex (read/write lock) does.
//
// When the fridge is no longer needed, you call Close, which tells the
// janitor to go home and waits until they've actually left. A janitor who
// never goes home is a goroutine leak.
//
// # Design notes (the interview version)
//
//   - One map guarded by a sync.RWMutex. Gets take the read lock, so many
//     readers can proceed at once; Set, Delete, and the sweep take the write
//     lock.
//   - Get checks the expiry itself (lazy expiry) but does not delete. Deleting
//     would need the write lock and would serialize every read that hits an
//     expired key. The janitor removes those entries on its next sweep.
//   - The janitor is a goroutine with a time.Ticker. Close signals it through
//     a stop channel and then waits on a done channel, so when Close returns
//     the goroutine has definitely exited. sync.Once makes Close idempotent,
//     because closing an already-closed channel panics.
//   - sync.Map would be a poor fit here. It's tuned for keys that are written
//     once and read many times, or for goroutines touching disjoint keys. It
//     has no way to do the sweep atomically, and it isn't generic.
package cache

import (
	"sync"
	"time"
)

// entry is one item in the fridge: the food, plus its "use by" date.
//
// [V any] makes entry generic: V is whatever type of value the cache stores
// (string, int, a *User, anything). Lowercase "entry" means it's unexported:
// code outside this package can't see it. It's an internal detail.
type entry[V any] struct {
	val     V
	expires time.Time // the zero Time means the entry never expires
}

// expired reports whether the entry's "use by" moment has arrived.
//
// It's a method: (e entry[V]) is the "receiver," the thing the method is
// called on, like `this` or `self` in other languages. Writing e.expired(now)
// calls it.
//
// !now.Before(expires) is a careful way to say "now is at or after expires."
// Using now.After(expires) instead would let an entry live one extra instant
// at exactly its deadline. Off-by-one bugs happen with time, too.
func (e entry[V]) expired(now time.Time) bool {
	return !e.expires.IsZero() && !now.Before(e.expires)
}

// Cache is a generic key-value cache with per-entry expiry.
// Create one with New and Close it when you're done with it.
//
// K comparable: keys must be a type that works with ==, because map keys
// have to be comparable. Strings and ints work; slices don't.
// V any: values can be anything at all.
type Cache[K comparable, V any] struct {
	// mu is the lock on the fridge door. Convention: put the mutex right
	// above the fields it protects. Here, that's items.
	mu    sync.RWMutex
	items map[K]entry[V]

	// Two channels used only as signals. They never carry any data.
	// struct{} is a type that takes up zero bytes: "the message is that a
	// message happened."
	stop      chan struct{} // closed by Close to mean "janitor, please go home"
	done      chan struct{} // closed by the janitor to mean "OK, I've left"
	closeOnce sync.Once     // makes sure stop only ever gets closed one time
}

// New returns a Cache that sweeps expired entries every cleanupInterval.
// If cleanupInterval <= 0 there is no background sweep; expired entries are
// still never returned, but they stay in memory until overwritten or deleted.
//
// Why a New function? A struct containing a map must have that map created
// with make before use (writing to a nil map panics), and the janitor has to
// be started. Go has no constructors, so a New function is the convention.
// It returns a pointer (*Cache) because a Cache contains a lock, and a lock
// must never be copied. Everyone has to share the same one.
func New[K comparable, V any](cleanupInterval time.Duration) *Cache[K, V] {
	c := &Cache[K, V]{
		items: make(map[K]entry[V]),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	if cleanupInterval > 0 {
		// "go" starts a function running in the background, as a new
		// goroutine, and moves on immediately without waiting for it.
		go c.janitor(cleanupInterval)
	} else {
		// No janitor was hired, so mark "the janitor has left" right away.
		// Otherwise Close would wait forever for someone who never came.
		close(c.done)
	}
	return c
}

// Set stores val under key until ttl has elapsed, replacing any existing
// value and TTL. A ttl <= 0 means the entry never expires.
func (c *Cache[K, V]) Set(key K, val V, ttl time.Duration) {
	// Build the entry before taking the lock. The less time spent holding
	// the lock, the less time everyone else spends waiting at the door.
	e := entry[V]{val: val}
	if ttl > 0 {
		e.expires = time.Now().Add(ttl) // "use by" = now + ttl
	}
	c.mu.Lock() // the full lock: nobody else can read or write
	c.items[key] = e
	c.mu.Unlock()
}

// Get returns the value for key and whether it was present and unexpired.
//
// Returning (value, bool) is the "comma ok" idiom, the same shape as reading
// a map with v, ok := m[k]. The bool answers "did you find it?", which is
// different from "the value happened to be zero."
func (c *Cache[K, V]) Get(key K) (V, bool) {
	// RLock is the shared "just looking" lock. Many goroutines can hold it at
	// the same time. It only blocks while someone holds the full Lock.
	c.mu.RLock()
	e, ok := c.items[key]
	c.mu.RUnlock()

	// Checking the date happens after unlocking. e is our own copy of the
	// entry, so nobody else can change it underneath us.
	if !ok || e.expired(time.Now()) {
		// "var zero V" makes the zero value of whatever V is: 0, "", nil,
		// and so on. It's the generic way to say "nothing."
		var zero V
		return zero, false
	}
	return e.val, true
}

// Delete removes key, if present.
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	delete(c.items, key) // deleting a key that isn't there is fine; no panic
	c.mu.Unlock()
}

// Len reports how many entries are stored, including expired entries that
// haven't been swept yet.
func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	// defer = "do this when the function returns." It's handy for making
	// sure an unlock always happens, even with several return paths.
	defer c.mu.RUnlock()
	return len(c.items)
}

// Close stops the background cleanup goroutine and waits for it to exit.
// It is safe to call more than once. The cache remains usable afterward;
// it just stops sweeping.
func (c *Cache[K, V]) Close() {
	// Closing a channel twice panics. sync.Once guarantees the function runs
	// only the first time, however many times (or from however many
	// goroutines) Close is called.
	c.closeOnce.Do(func() { close(c.stop) })

	// Receiving from a closed channel returns immediately, so this line
	// means "wait here until the janitor has closed done." Every later call
	// to Close sails straight through.
	<-c.done
}

// janitor runs in its own goroutine for the life of the cache.
func (c *Cache[K, V]) janitor(interval time.Duration) {
	// Deferred calls run in reverse order (last in, first out), so when this
	// function returns, ticker.Stop() runs first and close(c.done) runs
	// last. That's the "I've left" signal Close is waiting for.
	defer close(c.done)

	// A Ticker is an alarm that goes off over and over, every interval,
	// by sending the current time on its channel, ticker.C.
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Loop forever, waiting for whichever happens first each time around:
	// the alarm (time to clean) or the stop signal (time to go home).
	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-c.stop:
			// When stop is closed, this case becomes ready and stays ready
			// forever. Closing a channel is how you broadcast a message to
			// every listener at once.
			return
		}
	}
}

// deleteExpired is one pass of cleaning out the fridge.
func (c *Cache[K, V]) deleteExpired() {
	now := time.Now() // read the clock once, not once per item
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.items {
		if e.expired(now) {
			delete(c.items, k) // deleting during range is allowed in Go
		}
	}
}
