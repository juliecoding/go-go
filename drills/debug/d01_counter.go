//go:build ignore

// Drill d01: hit counter
//
// 100 goroutines each record 1,000 hits, so the final count should be 100,000.
// Run it several times. Find the bug, explain it, fix it.
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Counter struct {
	hits atomic.Int64
}

func (c *Counter) Hit() {
	c.hits.Add(1)
}

func (c *Counter) Total() int {
	return int(c.hits.Load())
}

func main() {
	var c Counter
	var wg sync.WaitGroup

	for range 100 {
		wg.Go(func() {
			for range 1000 {
				c.Hit()
			}
		})
	}
	wg.Wait()

	if got := c.Total(); got == 100_000 {
		fmt.Println("PASS: total =", got)
	} else {
		fmt.Println("FAIL: total =", got, "want 100000")
	}
}
