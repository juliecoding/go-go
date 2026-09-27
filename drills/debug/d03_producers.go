//go:build ignore

// Drill d03: producers
//
// Three producers each send 3 messages into a shared channel. main should
// receive all 9, then print PASS.
// Find the bug, explain it, fix it.
package main

import (
	"fmt"
	"sync"
	"time"
)

func produce(id int, out chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := 1; j <= 3; j++ {
		time.Sleep(time.Duration(id*10) * time.Millisecond)
		out <- fmt.Sprintf("producer %d, message %d", id, j)
	}
}

func main() {
	out := make(chan string)
	var wg sync.WaitGroup

	for id := 1; id <= 3; id++ {
		wg.Add(1)
		go produce(id, out, &wg)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	count := 0
	for msg := range out {
		fmt.Println(msg)
		count++
	}

	if count == 9 {
		fmt.Println("PASS: received 9 messages")
	} else {
		fmt.Println("FAIL: received", count, "messages, want 9")
	}
}
