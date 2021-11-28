package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "os"
import "time"
import "encoding/gob"
import "encoding/json"
import "io/ioutil"
import "path/filepath"
import "strconv"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	gob.Register(Task{})
	// // Your worker implementation here.
	for {
		reply := CallTask()
		switch reply.State {
			case 0:
				MapWorker(mapf,reply)
			case 1:
				ReduceWorker(reducef,reply)
			case 2:
				time.Sleep(time.Duration(time.Second * 5))
				break
			case 3:
				fmt.Println("Master all tasks complete. Nothing to do...")
				// exit worker process
				return
			default:
				panic("Invalid Task state received by worker")
		}
	}
}


/* 
	执行map任务
*/
func MapWorker(mapf func(string, string) []KeyValue,reply *ReplyTask) {
	task := (reply.Ele.Value).(Task)
	fileName := task.InputFIleName
	nReduce := task.NReduce
	fmt.Println("%vth map task run suceessfully",task.MapIndex)

	//获取键值对
	intermediate := []KeyValue{}
	file,err := os.Open(fileName);
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)

	//创建临时文件
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for outindex := 0; outindex < nReduce; outindex++ {
		//outname := outprefix + strconv.Itoa(outindex)
		//outFiles[outindex], _ = os.Create(outname)
		outFiles[outindex], _ = ioutil.TempFile("", "mr-tmp-*")
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}
	//编码
	for _, kv := range intermediate {
		outindex := ihash(kv.Key) % nReduce
		enc := fileEncs[outindex]
		enc.Encode(&kv)
		if err != nil {
			fmt.Println("编码错误", err.Error())
		   	panic("Json encode failed")
		}
	 }

	 //更名
	 outprefix := "mr-" + strconv.Itoa(task.MapIndex) + "-"
	 for outindex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outindex)
		oldpath := filepath.Join(file.Name())
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Rename(oldpath, outname)
		file.Close()
	 }

	 DoneTask(reply)
}


/* 
	执行reduce任务
*/
func ReduceWorker(reducef func(string, []string) string,reply *ReplyTask) {
	task := (reply.Ele.Value).(Task)
	fmt.Println("%vth reduce task run suceessfully",task.PartIndex)
	filePrefix := "mr-"
	fileSuffix := "-" + strconv.Itoa(task.PartIndex)

	//获取键值对
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap;i++ {
		fileName := filePrefix + strconv.Itoa(i) + fileSuffix
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
		  var kv KeyValue
		  if err := dec.Decode(&kv); err != nil {
			break
		  }
		  intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(task.PartIndex)
	ofile, _ := os.Create(oname)

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

	DoneTask(reply)
}

func CallTask() *ReplyTask{
	// declare an argument structure.
	args := ExampleArgs{}
	// declare a reply structure.
	reply := ReplyTask{}

	// send the RPC request, wait for the reply.
	call("Coordinator.CallTask", &args, &reply)

	return &reply;
}

func DoneTask(args* ReplyTask){
	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.DoneTask", args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
