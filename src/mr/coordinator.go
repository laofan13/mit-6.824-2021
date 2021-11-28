package mr

import (
	"container/list"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	StartTime     time.Time
	InputFIleName string
	MapIndex      int
	PartIndex     int
	NMap          int
	NReduce       int
}

type ReplyTask struct {
	State int //0 mapTask 1 reduceTask 2 waitTask 3 finshTask
	Ele   *list.Element
}

type Master struct {
	state            int //0 init 1 map_finshed 2 reduce_finshed
	nMap             int
	nReduce          int
	waitinglist      list.List //map人物列表
	RunningList      list.List //map任务运行队列
	mapFinshedNum    int
	reduceFinshedNum int

	lock  sync.Mutex
	files []string
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.state = 0
	m.nMap = len(files)
	m.nReduce = nReduce

	m.mapFinshedNum = 0
	m.reduceFinshedNum = 0
	m.files = files

	return &m
}

func (this *Master) distributeMap() {
	for i, v := range this.files {
		t := Task{}
		t.InputFIleName = v
		t.MapIndex = i
		t.NMap = this.nMap
		t.NReduce = this.nReduce

		this.waitinglist.PushBack(t)

	}
}

func (this *Master) distributeReduce() {
	for i := 0; i < this.nReduce; i++ {
		t := Task{}
		t.PartIndex = i
		t.NMap = this.nMap
		t.NReduce = this.nReduce
		this.waitinglist.PushBack(t)
	}
}

func (this *Master) CallTask() ReplyTask {
	reply := ReplyTask{}
	if this.state == 0 { //0 mapTask 1 reduceTask 2 waitTask 3 finshTask
		if this.waitinglist.Len() > 0 {
			//从任务队列中获取任务
			ele := this.waitinglist.Front()
			this.waitinglist.Remove(ele)
			task := (ele.Value).(Task)
			task.StartTime = time.Now()

			//加入到运行队列中
			reply.State = 0
			reply.Ele = this.RunningList.PushBack(task)

			fmt.Printf("Distributing map task on %vth file %v\n", task.MapIndex, task.InputFIleName)
		} else {
			reply.State = 2
		}
	} else if this.state == 1 {
		if this.waitinglist.Len() > 0 {
			//从任务队列中获取任务
			ele := this.waitinglist.Front()
			this.waitinglist.Remove(ele)
			task := (ele.Value).(Task)
			task.StartTime = time.Now()

			//加入到运行队列中
			reply.State = 1
			reply.Ele = this.RunningList.PushBack(task)

			fmt.Printf("Distributing reduce task on %vth file %v\n", task.PartIndex, task.InputFIleName)
		} else {
			fmt.Printf("All tasks are allocated, please wait\n")
			reply.State = 2
		}
	} else {
		reply.State = 3
	}
	return reply
}

func (this *Master) DoneTask(args *ReplyTask) {
	task := (args.Ele.Value).(Task)

	if task.StartTime.Unix()-time.Now().Unix() > 10 {
		if args.State == 0 {
			fmt.Println("th %v mapTask timeout, it will Reschedule", task.MapIndex)
		} else {
			fmt.Println("th %v reduceTask timeout, it will Reschedule", task.MapIndex)
		}
		this.RunningList.Remove(args.Ele)
		this.waitinglist.PushBack(task)
		return
	}

	this.RunningList.Remove(args.Ele)
	if args.State == 0 {
		this.mapFinshedNum++
		fmt.Printf("%vth map task finshed\n", task.MapIndex, task.InputFIleName)
		if this.mapFinshedNum == this.nMap {
			this.state = 1
			fmt.Printf("all map task finshed\n")
			this.distributeReduce()
		}
	} else if args.State == 1 {
		this.reduceFinshedNum++
		fmt.Printf("%vth map task finshed\n", task.PartIndex, task.InputFIleName)
		if this.reduceFinshedNum == this.nReduce {
			this.state = 2
			fmt.Printf("all reduce task finshed\n")
		}
	}

}

type Coordinator struct {
	// Your definitions here.
	master  *Master
	finshed bool
	lock    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

/*
	请求任务
*/
func (c *Coordinator) CallTask(args *ExampleArgs, reply *ReplyTask) error {
	c.master.lock.Lock()
	*reply = c.master.CallTask()
	c.master.lock.Unlock()
	return nil
}

/*
	完成任务
*/
func (c *Coordinator) DoneTask(args *ReplyTask, reply *ExampleReply) error {
	c.master.lock.Lock()
	c.master.DoneTask(args)
	c.master.lock.Unlock()
	if c.master.state == 2 {
		c.lock.Lock()
		c.finshed = true
		c.lock.Unlock()
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
	// ret := false

	// Your code here.

	c.lock.Lock()
	defer c.lock.Unlock()
	return c.finshed
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	gob.Register(Task{})
	c.master = MakeMaster(files, nReduce)
	c.master.distributeMap()
	// master := c.master;

	// master.distributeMap()
	// task := c.master.CallTask()
	// fmt.Println(task)
	// fmt.Println("%+v",master.RunningList);
	// master.DoneTask(&task)

	// fmt.Println(task.ele == master.RunningList.Front())
	// fmt.Println("%+v",master.RunningList);

	c.server()
	return &c
}
