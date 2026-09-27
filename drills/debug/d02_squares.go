//go:build ignore

// Drill d02: collecting squares
//
// Compute the squares of 1..10 concurrently and collect them. Order doesn't
// matter, but all 10 should be there and they should sum to 385.
// Find the bug, explain it, fix it.
package main

import (
	"fmt"
	"sync"
)

func main() {
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		squares []int
	)

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			mu.Lock()
			squares = append(squares, i*i)
			mu.Unlock()
		}()

		// Alternatively!
		// wg.Go(func() {
		// 	mu.Lock()
		// 	squares = append(squares, i*i)
		// 	mu.Unlock()
		// })
	}

	wg.Wait()

	sum := 0
	for _, s := range squares {
		sum += s
	}
	if len(squares) == 10 && sum == 385 {
		fmt.Println("PASS:", squares)
	} else {
		fmt.Printf("FAIL: got %d squares (sum %d), want 10 (sum 385)\n", len(squares), sum)
	}
}
