package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

type KeyValueSlice []KeyValue

// for sorting by key.
func (a KeyValueSlice) Len() int           { return len(a) }
func (a KeyValueSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	printIfError(err)
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	replyMap := RequestMapTask()
	if replyMap.Filename == "nil" {
		fmt.Println("Filename is empty")
	}

	file, err := os.Open(replyMap.Filename)
	printIfError(err)

	content, err := io.ReadAll(file)
	printIfError(err)

	err = file.Close()
	printIfError(err)

	var mapResult KeyValueSlice = mapf(replyMap.Filename, string(content))
	sort.Sort(mapResult)

	intermediateFiles := make([]*os.File, reply.NReduce)
	jsonEncoders := make([]*json.Encoder, reply.NReduce)
	// create intermediate files and json encoders so that we can encode them into json docs
	for i := 0; i < replyMap.NReduce; i++ {

		intermediateFileName := "mr-" + strconv.Itoa(replyMap.MapTaskNumber) + "-" + strconv.Itoa(i)
		tmpFile, err := os.Create(intermediateFileName)
		printIfError(err)

		intermediateFiles[i] = tmpFile
		jsonEncoders[i] = json.NewEncoder(tmpFile)
	}

	//partition the output from map (using ihash) into intermediate files with json for further work
	for _, kv := range mapResult {
		err = jsonEncoders[ihash(kv.Key)%replyMap.NReduce].Encode(kv)
		printIfError(err)
	}

	/*
		outputName := "mr-out-" + strconv.Itoa(reply.MapTaskNumber)
		outputFile, _ := os.Create(outputName)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		err = outputFile.Close()
		if err != nil {
			fmt.Println(err)
			return
		}
	*/

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestMapTask() *TaskReply {
	//TODO: ta bort args, inte nu men senare när vi är säkra på att vi inte behöver den?
	args := GetTaskArgs{}
	args.X = 99

	reply := TaskReply{}

	ok := call("Coordinator.GrantMapTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Filename)
		return &reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

func RequestReduceTask() *TaskReply {

	args := GetTaskArgs{}
	args.X = 99

	reply := TaskReply{}

	ok := call("Coordinator.GrantReduceTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Filename)
		return &reply
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}

func SignalMapDone(intermediateFilePaths *[]string, mapTaskNumber int) bool {

	args := SignalMapDoneArgs{make([]IntermediateFile, len(*intermediateFilePaths))}
	for i, path := range *intermediateFilePaths {
		args.IntermediateFiles[i].Path = path
		args.IntermediateFiles[i].PartitionId = i
		args.IntermediateFiles[i].MapTaskNumber = mapTaskNumber
	}

	reply := TaskReply{}

	ok := call("Coordinator.MapDoneSignalled", &reply, &args)
	if ok {
		fmt.Println("Coordinator has been signalled")
		return true
	} else {
		fmt.Printf("call failed!\n")
		return false
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func logIfFatalError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func printIfError(err error) {
	if err != nil {
		fmt.Println(err)
		return
	}
}
