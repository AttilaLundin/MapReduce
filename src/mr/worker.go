package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

type KeyValueSlice []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	reply := RequestMapTask()
	if reply.Filename == "nil" {
		fmt.Println("Filename is empty")
	}

	file, err := os.Open(reply.Filename)
	printIfError(err)

	content, err := io.ReadAll(file)
	printIfError(err)

	err = file.Close()
	printIfError(err)

	var mapResult KeyValueSlice = mapf(reply.Filename, string(content))
	sort.Sort(mapResult)

	intermediateFiles := make([]*os.File, reply.NReduce)
	jsonEncoders := make([]*json.Encoder, reply.NReduce)
	// create intermediate files and json encoders so that we can encode them into json docs
	for i := 0; i < reply.NReduce; i++ {

		intermediateFileName := "mr-" + strconv.Itoa(reply.MapTaskNumber) + "-" + strconv.Itoa(i)
		tmpFile, err := os.Create(intermediateFileName)
		printIfError(err)

		intermediateFiles[i] = tmpFile
		jsonEncoders[i] = json.NewEncoder(tmpFile)
	}

	//partition the output from map (using ihash) into intermediate files for further work
	for _, kv := range mapResult {
		err = jsonEncoders[ihash(kv.Key)%reply.NReduce].Encode(kv)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestTask() string {
	args := GetTaskArgs{}
	args.X = 99

	reply := TaskReply{}

	ok := call("Coordinator.GrantTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Filename)
		return reply.Filename
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.Filename
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

func SignalMapDone() {

	return nil
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
