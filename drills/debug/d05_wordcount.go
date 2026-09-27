//go:build ignore

// Drill d05: word count
//
// Count words across many documents, one goroutine per document.
// Find the bug, explain it, fix it.
package main

import (
	"fmt"
	"strings"
	"sync"
)

func main() {
	base := []string{
		"the quick brown fox jumps over the lazy dog",
		"the dog barks and the fox runs",
		"a lazy afternoon for a quick nap",
	}
	var docs []string
	for range 200 {
		docs = append(docs, base...)
	}

	counts := map[string]int{}
	var wg sync.WaitGroup

	for _, doc := range docs {
		wg.Go(func() {
			for _, w := range strings.Fields(doc) {
				counts[w]++
			}
		})
	}
	wg.Wait()

	// Per 3 base docs: "the" appears 4 times, "fox" 2, "a" 2.
	want := map[string]int{"the": 800, "fox": 400, "a": 400}
	ok := true
	for w, n := range want {
		if counts[w] != n {
			ok = false
			fmt.Printf("FAIL: counts[%q] = %d, want %d\n", w, counts[w], n)
		}
	}
	if ok {
		fmt.Println("PASS")
	}
}
