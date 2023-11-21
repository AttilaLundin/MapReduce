package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

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
	MAP_PHASE    Status = 0
	REDUCE_PHASE        = 1
	DONE                = 2
)

type Task struct {
	filename string
	status   Status
}

var mapTaskNr = 0
var rTaskNr = 0

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

	switch c.status {

	case MAP_PHASE:

		if mapTaskNr < len(c.files) {
			reply.Filename = c.files[mapTaskNr] //"../main/pg-dorian_gray.txt"
			reply.MapTaskNumber = mapTaskNr
			reply.NReduce = c.nReduce
			reply.Status = MAP_PHASE
			taskNr += 1
		} else {
			return errors.New("Map task not available")
		}

	case REDUCE_PHASE:

		if rTaskNr < c.nReduce {

			reply.Filename = c.files[rTaskNr] //"../main/pg-dorian_gray.txt"
			//reply.MapTaskNumber = mapTaskNr
			reply.NReduce = c.nReduce
			reply.Status = REDUCE_PHASE
			taskNr += 1
		} else {
			return errors.New("Reduce task not available")
		}

	case DONE:
		fmt.Println("MapReduce done")
		// TODO

	}

	return nil
}

func (c *Coordinator) MapDoneSignalled(args *SignalMapDoneArgs, reply *TaskReply) error {

	for _, intermediateFile := range args.IntermediateFiles {
		c.intermediateFiles[intermediateFile.ReduceTaskNumber] = append(c.intermediateFiles[intermediateFile.ReduceTaskNumber], intermediateFile)
		ok := c.updateTaskStatus(intermediateFile.filename, args.Status)
		if !ok {
			//	TODO: handle error
		}
	}
	c.checkPhase(args.Status)
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
	c.status = REDUCE_PHASE
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
		status:  MAP_PHASE,
	}

	for _, file := range files {
		c.tasks[file] = Task{status: MAP_PHASE, filename: file}
	}

	c.server()
	return &c
}
