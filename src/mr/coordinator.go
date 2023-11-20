package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Status int64

type Coordinator struct {
	// Your definitions here.
	nReduce int
	files   []string
	done    bool
	tasks   map[string]Task
	status  Status
}

const (
	MAP    Status = 0
	REDUCE        = 1
	DONE          = 2
)

const (
	NOTSTARTED  Status = 0
	MAPPINGDONE        = 1
	REDUCEDONE         = 2
)

type Task struct {
	filename string
	status   Status
}

var taskNr = 0

func (c *Coordinator) updateTaskStatus(filename string, nextStatus Status) bool {
	task, ok := c.tasks[filename]
	if !ok {
		return false
	}
	task.status = nextStatus
	return true
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GrantTask(args *GetTaskArgs, reply *TaskReply) error {
	// todo: grant any task. Pop from a stack of predefined text files?
	switch args.Tasktype {
	case "MapTask":

		reply.Filename = "../main/pg-dorian_gray.txt"
		reply.MapTaskNumber = taskNr
		reply.NReduce = c.nReduce
		reply.ReduceTaskAvailable = false
		taskNr += 1

	case "ReduceTask":

		//TODO
	}

	return nil
}

func (c *Coordinator) MapDoneSignalled(args *SignalMapDoneArgs, reply *TaskReply) error {

	for _, intermediateFile := range args.IntermediateFiles {
		ok := c.updateTaskStatus(intermediateFile.filename, MAPPINGDONE)
		if !ok {
			//	TODO: handle error
		}
	}

	c.checkPhase(REDUCE)
	return nil
}
func (c *Coordinator) checkPhase(status Status) {
	for _, task := range c.tasks {
		if task.status != status {
			return
		}
	}
	if status == DONE {
		c.Done()
	}
	c.status = REDUCE
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
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		done:    false,
		tasks:   make(map[string]Task, len(files)),
		status:  MAP,
	}

	for i, file := range files {
		c.tasks[file] = Task{status: NOTSTARTED, filename: file}
	}

	c.server()
	return &c
}
