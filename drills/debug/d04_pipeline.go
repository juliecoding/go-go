//go:build ignore

// Drill d04: pipeline
//
// gen emits numbers, square squares them, main sums the results.
// The sum of squares of 1..5 is 55.
// Find the bug, explain it, fix it.
package main

import "fmt"

func gen(nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		for _, n := range nums {
			out <- n
		}
		close(out)
	}()
	return out
}

func square(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		for n := range in {
			out <- n * n
		}
	}()
	return out
}

func main() {
	sum := 0
	for sq := range square(gen(1, 2, 3, 4, 5)) {
		fmt.Println("got", sq)
		sum += sq
	}

	if sum == 55 {
		fmt.Println("PASS: sum =", sum)
	} else {
		fmt.Println("FAIL: sum =", sum, "want 55")
	}
}
