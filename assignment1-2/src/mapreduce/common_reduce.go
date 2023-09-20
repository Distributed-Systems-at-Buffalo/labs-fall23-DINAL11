package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.

	// Create a map to store intermediate key-value pairs
	intermediate := make(map[string][]string)

	// Read intermediate key-value pairs from map tasks
	for m := 0; m < nMap; m++ {
		intermediateFile := reduceName(jobName, m, reduceTaskNumber)

		// Open the intermediate file for reading
		file, err := os.Open(intermediateFile)
		if err != nil {
			checkError(err)
			continue
		}
		defer file.Close()

		// Create a JSON decoder for the intermediate file
		decoder := json.NewDecoder(file)

		// Decode and collect key-value pairs
		var kv KeyValue
		for {
			if err := decoder.Decode(&kv); err != nil {
				break // Reached end of file
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
	}

	// Create the output file for this reduce task
	outputFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	if err != nil {
		checkError(err)
		return
	}
	defer outputFile.Close()

	// Sort keys
	var keys []string
	for key := range intermediate {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Call the user-defined reduce function and write the result to the output file
	enc := json.NewEncoder(outputFile)
	for _, key := range keys {
		result := reduceF(key, intermediate[key])
		enc.Encode(KeyValue{key, result})
	}

}
