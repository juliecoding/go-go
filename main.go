package main

import (
	"github.com/juliecoding/go-go/simplepool"
)

// Spawn 5 goroutines.
// Each goroutine sends a single number to a channel.
// Main goroutine receives all 5 numbers and prints them.
// Main waits until all 5 have been received, then exits.

func main() {
	simplepool.SimplePool()
}
