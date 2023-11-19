package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int
	files   []string
	done    bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GrantMapTask(args *GetTaskArgs, reply *TaskReply) error {
	// todo: grant any task. Pop from a stack of predefined text files?
	reply.Filename = "../main/pg-dorian_gray.txt"
	reply.MapTaskNumber = taskNr
	reply.NReduce = c.nReduce
	taskNr += 1
	return nil
}

func (c *Coordinator) GrantReduceTask(args *GetTaskArgs, reply *TaskReply) error {

	return nil
}

func (c *Coordinator) SignalMapDone(mTaskNumber int) error {

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

func (c *Coordinator) Done() bool {
	ret := false

	if c.done == true {
		ret = true
	}
	// question: what is the entire job?

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, done: false}

	// Your code here.

	c.server()
	return &c
}
