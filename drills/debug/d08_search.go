//go:build ignore

// Drill d08: fastest replica
//
// search queries several replicas and returns whichever answers first.
// The results look right. The check at the end fails anyway.
// Find the bug, explain it, fix it.
package main

import (
	"fmt"
	"runtime"
	"time"
)

func search(query string, replicas []time.Duration) string {
	ch := make(chan string)
	for i, latency := range replicas {
		go func() {
			time.Sleep(latency)
			ch <- fmt.Sprintf("replica %d: results for %q", i, query)
		}()
	}
	return <-ch
}

func main() {
	before := runtime.NumGoroutine()

	replicas := []time.Duration{30 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}
	for range 20 {
		fmt.Println(search("golang", replicas))
	}

	time.Sleep(100 * time.Millisecond) // give every goroutine time to finish
	after := runtime.NumGoroutine()

	if leaked := after - before; leaked == 0 {
		fmt.Println("PASS: no goroutines left behind")
	} else {
		fmt.Println("FAIL:", leaked, "goroutines still running after search returned")
	}
}
