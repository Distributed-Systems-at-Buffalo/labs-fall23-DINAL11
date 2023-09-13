package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	sum := 0
	for num := range nums {
		sum += num
	}
	out <- sum
	// HINT: use for loop over `nums`
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	file, error := os.Open(fileName)
	if error != nil {
		checkError(error)
		return 0
	}
	defer file.Close()

	nums, error := readInts(file)
	if error != nil {
		checkError(error)
		return 0
	}

	var numsChannel chan int = make(chan int)
	var sumsChannel chan int = make(chan int)

	for i := 0; i < num; i++ {
		go sumWorker(numsChannel, sumsChannel)
	}

	for _, n := range nums {
		numsChannel <- n
	}
	close(numsChannel)

	// Collect sums from worker goroutines
	totalSum := 0
	for i := 0; i < num; i++ {
		sum := <-sumsChannel
		totalSum += sum
	}

	close(sumsChannel)
	return totalSum
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
