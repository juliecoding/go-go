package simplepool

// Make a jobs channel.
// Start 3 workers. Each worker loops: take a job from the channel,
//     sleep 500ms, print "worker W squared N = N*N".
// main sends the numbers 1..10 into the jobs channel.
// main waits until all workers are finished, then prints "ALL DONE".

import (
	"fmt"
	"sync"
	"time"
)

func SimplePool() {
	var wg sync.WaitGroup

	jobs := make(chan int)
	results := make(chan int)

	for range 3 {
		wg.Go(func() {
			for j := range jobs {
				time.Sleep(time.Millisecond * 500)
				results <- j * j
			}
		})
	}

	go func() {
		for k := 1; k <= 10; k++ {
			jobs <- k
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	total := 0
	for i := range results {
		fmt.Printf("RESULT %d \n", i)
		total += i
	}

	fmt.Printf("TOTAL: %d \n", total)
	fmt.Println("ALL DONE")
}
