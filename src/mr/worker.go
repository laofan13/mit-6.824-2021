package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

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

	// // Your worker implementation here.
	for {
		args := TaskArgs{}
		// declare a reply structure.
		reply := TaskReply{}

		// send the RPC request, wait for the reply.
		call("Coordinator.CallTask", &args, &reply)
		switch reply.TaskType {
		case Map:
			MapWorker(reply.NumTask, reply.MapFIle, reply.NReduceTasks, mapf)
		case Reduce:
			ReduceWorker(reply.NumTask, reply.NMapTaks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad taks type %d", reply.TaskType)
		}

		doneArgs := DoneTaskRrgs{
			TaskType: reply.TaskType,
			NumTask:  reply.NumTask,
		}
		doneReply := DoneTaskReply{}
		call("Coordinator.DoneTask", &doneArgs, &doneReply)

	}
}

/*
	执行map任务
*/
func MapWorker(numTask int, mapFIle string, nReduce int, mapf func(string, string) []KeyValue) {
	fmt.Println("%vth map task run suceessfully", numTask)

	//获取键值对
	file, err := os.Open(mapFIle)
	if err != nil {
		log.Fatalf("cannot open %v", mapFIle)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapFIle)
	}
	file.Close()
	kva := mapf(mapFIle, string(content))

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
	for _, kv := range kva {
		outindex := ihash(kv.Key) % nReduce
		enc := fileEncs[outindex]
		enc.Encode(&kv)
		if err != nil {
			fmt.Println("编码错误", err.Error())
			panic("Json encode failed")
		}
	}

	//更名
	outprefix := "mr-" + strconv.Itoa(numTask) + "-"
	for outindex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outindex)
		oldpath := filepath.Join(file.Name())
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Rename(oldpath, outname)
		file.Close()
	}
}

/*
	执行reduce任务
*/
func ReduceWorker(numTask int, nMap int, reducef func(string, []string) string) {
	fmt.Println("%vth reduce task run suceessfully", numTask)
	filePrefix := "mr-"
	fileSuffix := "-" + strconv.Itoa(numTask)

	//获取键值对
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
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

	oname := "mr-out-" + strconv.Itoa(numTask)
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
	// sockname := coordinatorSock()
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:8095")
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
