package cos418_hw1_1

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
//
//	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
//
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuation and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	text, err := os.ReadFile(path)
	checkError(err)

	//lowercase conversion and punctuation removing
	text = []byte(strings.ToLower(string(text)))
	puncregex := regexp.MustCompile("[^0-9a-zA-Z']+")
	text = puncregex.ReplaceAll(text, []byte(" "))
	text = []byte(strings.ReplaceAll(string(text), "'", ""))

	// Spliting content into words
	words := strings.Fields(string(text))

	// Counting frequency of words
	freqwords := map[string]int{}
	for i := 0; i < len(words); i++ {
		word := words[i]
		if len(word) >= charThreshold {
			freqwords[word]++
		}
	}

	// Converting freqwords
	wordcounts := []WordCount{}
	for word, count := range freqwords {
		wordcounts = append(wordcounts, WordCount{Word: word, Count: count})
	}

	// Sorting wordCounts
	sortWordCounts(wordcounts)

	// Return the top K words
	if numWords > len(wordcounts) {
		numWords = len(wordcounts)
	}
	return wordcounts[:numWords]
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
