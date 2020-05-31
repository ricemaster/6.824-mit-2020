package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
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

type Task struct {
	TaskType int
	taskNo   int
	nReduce  int
	files    []string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// 1. request tasks
		args := TaskArgs{}
		reply := TaskReply{}
		conn := call("Master.Task", &args, &reply)
		fmt.Println("-->Request Task")
		if !conn {
			return
		}

		// 2. based on task type 0-map 1-reduce
		switch reply.TaskType {
		case 0:
			sleep(11, 0, reply.TaskNo, reply.Files)
			doMapTask(reply.TaskNo, reply.Files, reply.NReduce, mapf)
		case 1:
			sleep(11, 0, reply.TaskNo, reply.Files)
			doReduceTask(reply.TaskNo, reply.Files, reducef)
		default:
			return
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func sleep(second int, pencentage float32, taskNo int, files []string) {
	rand.Seed(time.Now().UnixNano())
	if i := rand.Intn(100); i < int(pencentage*100) {
		fmt.Printf("Crash! rand(%d<%d): TaskNo=%d, file=%v\n", i, int(pencentage*100), taskNo, files)
		os.Exit(1)
		// fmt.Printf("Sleep 11s rand(%d<%d): TaskNo=%d, file=%v\n", i, int(pencentage*100), taskNo, files)
		// time.Sleep(time.Second * time.Duration(second))
	}
}

//Map Task Process
func doMapTask(mapTaskNo int, files []string, nReduce int, mapf func(string, string) []KeyValue) {
	filenames := files
	if len(filenames) != 1 {
		fmt.Printf("invalid filenames: %d, %v\n", len(filenames), filenames)
		return
	}
	filename := filenames[0]
	if filename != "" {
		fmt.Printf("-->Worker @MapTask: File=%s, MapTaskNo=%d\n", filename, mapTaskNo)
		intermediate := []KeyValue{}

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)

		intermediateByReduceNo := make(map[int][]KeyValue)
		for _, item := range intermediate {
			reduceNo := ihash(item.Key) % nReduce
			intermediateByReduceNo[reduceNo] = append(intermediateByReduceNo[reduceNo], item)
		}

		for index, intermediate := range intermediateByReduceNo {
			tempFile, err := ioutil.TempFile(".", "tmp")
			if err != nil {
				fmt.Println("create temp file failed.")
			}
			enc := json.NewEncoder(tempFile)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Printf("encode error: %v\n", &kv)
				}
			}
			intermediateFileName := fmt.Sprintf("mr-%s-%s", strconv.Itoa(mapTaskNo), strconv.Itoa(index))
			os.Rename(tempFile.Name(), intermediateFileName)
			tempFile.Close()
			fmt.Printf("Finished intermediateFile: %s, No. %d\n", intermediateFileName, mapTaskNo)
		}
		noticeMapFinish(mapTaskNo, filename)
	}

}

func noticeMapFinish(mapTaskNo int, filename string) {
	args := MapFinishArgs{}
	args.TaskNo = mapTaskNo
	args.Filename = filename
	reply := MapFinishReply{}
	call("Master.MapFinish", &args, &reply)
	if reply.Noticed {
		fmt.Printf("-->MapTask Successful: File=%s, MapTaskNo=%d\n", filename, mapTaskNo)
		return
	}
	fmt.Printf("-->ERROR: No Feedback: File=%s, MapTaskNo=%d\n", filename, mapTaskNo)
}

//Reduce Task Process
func doReduceTask(reduceTaskNo int, filenames []string, reducef func(string, []string) string) {
	fmt.Printf("--> Reducing Files: Worker-%d, files: %v\n", reduceTaskNo, filenames)
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("Open file failed: %s\n", filename)
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
	oname := "mr-out-" + strconv.Itoa(reduceTaskNo)
	ofile, _ := os.Create(oname)

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
	// fmt.Printf("-->ReduceTask Finish: reduceTaskNo=%d, output=%s\n", reduceTaskNo, oname)
	noticeReduceFinish(reduceTaskNo)
}

func noticeReduceFinish(reduceTaskNo int) {
	args := ReduceFinishArgs{}
	args.TaskNo = reduceTaskNo
	reply := ReduceFinishReply{}
	call("Master.ReduceFinish", &args, &reply)
	if reply.Noticed {
		fmt.Printf("-->ReduceTask Successful: ReduceTaskNo=%d\n", reduceTaskNo)
		return
	}
	fmt.Printf("-->ERROR: No Feedback: ReduceTaskNo=%d\n", reduceTaskNo)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
