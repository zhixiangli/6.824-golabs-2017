package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	// read map output file & group key-value
	groupMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		interFilename := reduceName(jobName, i, reduceTaskNumber)
		interFile, err := os.Open(interFilename)
		if err != nil {
			log.Fatalf("open file %s err: %s", interFilename, err)
		}
		defer interFile.Close()
		jsonDec := json.NewDecoder(interFile)
		for jsonDec.More() {
			var entry KeyValue
			if err := jsonDec.Decode(&entry); err != nil {
				log.Fatalf("decode entry %v err: %s", entry, err)
			}
			if _, exits := groupMap[entry.Key]; !exits {
				groupMap[entry.Key] = make([]string, 0)
			}
			groupMap[entry.Key] = append(groupMap[entry.Key], entry.Value)
		}
	}

	// invoke reduceF & write to output file
	reducedFile, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("create file %s err: %s", outFile, err)
	}
	defer reducedFile.Close()
	jsonEnc := json.NewEncoder(reducedFile)
	for key, values := range groupMap {
		entry := KeyValue{
			Key:   key,
			Value: reduceF(key, values),
		}
		if err := jsonEnc.Encode(entry); err != nil {
			log.Fatalf("json encode %v err: %s", entry, err)
		}
	}

}
