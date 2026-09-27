//go:build ignore

// Drill d07: inventory
//
// 50 goroutines each add 100 widgets, so stock should end at 5,000.
// The inventory has a mutex, so this should be safe... right?
// Find the bug, explain it, fix it.
package main

import (
	"fmt"
	"sync"
)

type Inventory struct {
	mu    sync.Mutex
	stock map[string]int
}

func NewInventory() Inventory {
	return Inventory{stock: map[string]int{}}
}

func (inv Inventory) Add(item string, n int) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.stock[item] += n
}

func (inv Inventory) Count(item string) int {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.stock[item]
}

func main() {
	inv := NewInventory()
	var wg sync.WaitGroup

	for range 50 {
		wg.Go(func() {
			for range 100 {
				inv.Add("widget", 1)
			}
		})
	}
	wg.Wait()

	if got := inv.Count("widget"); got == 5000 {
		fmt.Println("PASS: widgets =", got)
	} else {
		fmt.Println("FAIL: widgets =", got, "want 5000")
	}
}
