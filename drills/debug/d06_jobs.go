//go:build ignore

// Drill d06: jobs that can fail
//
// Run 10 jobs concurrently. Jobs 4 and 8 fail. main should wait for all jobs,
// then report exactly 2 errors.
// Find the bug, explain it, fix it.
package main

import (
	"fmt"
	"sync"
	"time"
)

func process(id int, wg *sync.WaitGroup, errs chan<- error) {
	if id%4 == 0 {
		errs <- fmt.Errorf("job %d failed", id)
		return
	}

	time.Sleep(10 * time.Millisecond)
	fmt.Println("job", id, "done")
	wg.Done()
}

func main() {
	errs := make(chan error, 10)
	var wg sync.WaitGroup

	for id := 1; id <= 10; id++ {
		wg.Add(1)
		go process(id, &wg, errs)
	}

	wg.Wait()
	close(errs)

	count := 0
	for err := range errs {
		fmt.Println("error:", err)
		count++
	}

	if count == 2 {
		fmt.Println("PASS: 2 errors")
	} else {
		fmt.Println("FAIL:", count, "errors, want 2")
	}
}
