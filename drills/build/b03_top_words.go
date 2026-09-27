//go:build ignore

// Drill b03: top words (~25 min, no concurrency)
//
// Implement topWords(text, k):
//
//   - Split text on whitespace.
//   - Lowercase each word and trim leading/trailing punctuation: . , ! ? ; : " '
//     Skip anything that's empty after trimming.
//   - Return the k most frequent words, highest count first.
//     Ties are broken alphabetically.
//   - If there are fewer than k distinct words, return them all.
//
// Run: go run drills/build/b03_top_words.go
package main

import (
	"fmt"
	"slices"
)

type WordCount struct {
	Word  string
	Count int
}

func topWords(text string, k int) []WordCount {
	return nil
}

// ---- checks below: don't edit ----

var failures int

func check(name string, got, want []WordCount) {
	if slices.Equal(got, want) {
		fmt.Println("PASS", name)
		return
	}
	failures++
	fmt.Printf("FAIL %s\n  got:  %v\n  want: %v\n", name, got, want)
}

func main() {
	check("basic",
		topWords("the cat and the hat and the bat", 2),
		[]WordCount{{"the", 3}, {"and", 2}})

	check("ties are alphabetical",
		topWords("pear apple fig apple pear fig kiwi", 3),
		[]WordCount{{"apple", 2}, {"fig", 2}, {"pear", 2}})

	check("case and punctuation",
		topWords(`The CAT sat. "The cat!" the end?`, 2),
		[]WordCount{{"the", 3}, {"cat", 2}})

	check("k larger than distinct words",
		topWords("go go gopher", 10),
		[]WordCount{{"go", 2}, {"gopher", 1}})

	check("punctuation-only tokens skipped",
		topWords("wait ... what ?! wait", 5),
		[]WordCount{{"wait", 2}, {"what", 1}})

	if got := topWords("", 3); len(got) != 0 {
		failures++
		fmt.Println("FAIL empty text: got", got)
	} else {
		fmt.Println("PASS empty text")
	}

	if got := topWords("a b c", 0); len(got) != 0 {
		failures++
		fmt.Println("FAIL k = 0: got", got)
	} else {
		fmt.Println("PASS k = 0")
	}

	fmt.Printf("\n%d failure(s)\n", failures)
}
