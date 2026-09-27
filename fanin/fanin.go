package fanin

import (
	"fmt"
	"sync"
	"time"
)

// Spawn 5 goroutines.
// Each goroutine sends a single number to a channel.
// Main goroutine receives all 5 numbers and prints them.
// Main waits until all 5 have been received, then exits.

// The parts
// 1 channel. The workers send on it.
// 1 WaitGroup. It tracks the 5 workers and nothing else.
// 5 worker goroutines. Each sends its number, then tells the WaitGroup it's finished. (wg.Go handles both the adding and the finished signal for you.)
// 1 closer goroutine. It's a plain go func() { ... }(), and it is not tracked by any WaitGroup. Its whole body is two lines: wait on the WaitGroup, then close the channel.
// FanIn itself ranges over the channel and prints.

func FanIn() {
	ch := make(chan int)
	var wg sync.WaitGroup

	for i := 1; i <= 5; i++ {
		wg.Go(func() {
			work(i, ch)
		})
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for val := range ch {
		fmt.Printf("Now I'm printing val: %d \n", val)
	}

	fmt.Println("ALL DONE")
}

func work(n int, c chan int) {
	time.Sleep(time.Second * 2)
	fmt.Printf("Putting value %d into channel \n", n)
	c <- n
}

func fanInWithoutWaitGroups() {
	c := make(chan int)
	for i := 1; i <= 5; i++ {
		go work(i, c)
	}

	for i := 1; i <= 5; i++ {
		val := <-c
		fmt.Printf("Now I'm printing val: %d \n", val)
	}

	fmt.Println("ALL DONE")
}
