package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	mapFIles     []string
	nMapTasks    int
	nReduceTasks int

	mapTaskFinshed    []bool
	mapTaskIssued     []time.Time
	reduceTaskFinshed []bool
	reduceTaskIssued  []time.Time

	isDone bool
}

func (c *Coordinator) CallTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NMapTaks = c.nMapTasks
	reply.NReduceTasks = c.nReduceTasks

	for {
		mapDone := true
		for i, done := range c.mapTaskFinshed {
			if !done {
				if c.mapTaskIssued[i].IsZero() || time.Since(c.mapTaskIssued[i]).Seconds() > 10 {
					reply.TaskType = Map
					reply.NumTask = i
					reply.MapFIle = c.mapFIles[i]
					c.mapTaskIssued[i] = time.Now()

					return nil
				} else {
					mapDone = false
				}
			}
		}
		if !mapDone {

		} else {
			break
		}
	}

	for {
		redDone := true
		for i, done := range c.reduceTaskFinshed {
			if !done {
				if c.reduceTaskIssued[i].IsZero() || time.Since(c.reduceTaskIssued[i]).Seconds() > 10 {
					reply.TaskType = Reduce
					reply.NumTask = i
					c.reduceTaskIssued[i] = time.Now()

					return nil
				} else {
					redDone = false
				}
			}
		}

		if !redDone {

		} else {
			break
		}
	}

	reply.TaskType = Done
	c.isDone = true

	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) DoneTask(args *DoneTaskRrgs, reply *DoneTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case Map:
		c.mapTaskFinshed[args.NumTask] = true
	case Reduce:
		c.reduceTaskFinshed[args.NumTask] = true
	default:
		log.Fatalf("bad finshed task? %d", args.NumTask)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	l, e := net.Listen("tcp", "127.0.0.1:8095")
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu = sync.Mutex{}

	c.mapFIles = files
	c.nMapTasks = len(files)
	c.nReduceTasks = nReduce

	c.mapTaskFinshed = make([]bool, c.nMapTasks)
	c.mapTaskIssued = make([]time.Time, c.nMapTasks)
	c.reduceTaskFinshed = make([]bool, nReduce)
	c.reduceTaskIssued = make([]time.Time, nReduce)

	c.isDone = false

	c.server()
	return &c
}
