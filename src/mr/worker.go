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

var workerID string

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

func GetRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerID = GetRandomString(6)
	for {
		// 1. request tasks
		// log.Printf("Starting request a task: worker=%s\n", workerID)

		args := TaskArgs{}
		args.WorkerID = workerID
		reply := TaskReply{}

		if conn := call("Master.Task", &args, &reply); !conn {
			return
		}

		// 2. based on task type 0-map 1-reduce 2-waiting for last task finish or fail
		switch reply.TaskType {
		case 0:
			doMapTask(reply.TaskNo, reply.Files, reply.NReduce, mapf)
		case 1:
			doReduceTask(reply.TaskNo, reply.Files, reducef)
		case 2:
			time.Sleep(time.Second * 1)
		default:
			return
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//Map Task Process
func doMapTask(mapTaskNo int, files []string, nReduce int, mapf func(string, string) []KeyValue) {
	filenames := files
	if len(filenames) == 0 {
		return
	}
	if len(filenames) > 1 {
		log.Printf("invalid filenames: %d, %v\n", len(filenames), filenames)
		return
	}
	filename := filenames[0]
	if filename != "" {
		// log.Printf("Starting MapTask: File=%s, MapTaskNo=%d\n", filename, mapTaskNo)
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
			// log.Printf("ReduceNo=%d\n", reduceNo)
			intermediateByReduceNo[reduceNo] = append(intermediateByReduceNo[reduceNo], item)
		}

		intermediateFileNames := make([]string, 0)
		for index, intermediate := range intermediateByReduceNo {
			tempFile, err := ioutil.TempFile(".", "tmp")
			if err != nil {
				log.Println("create temp file failed.")
			}
			enc := json.NewEncoder(tempFile)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Printf("encode error: %v\n", &kv)
				}
			}
			intermediateFileName := fmt.Sprintf("mr-%s-%s", strconv.Itoa(mapTaskNo), strconv.Itoa(index))
			os.Rename(tempFile.Name(), intermediateFileName)
			tempFile.Close()
			intermediateFileNames = append(intermediateFileNames, intermediateFileName)
			// log.Printf("Finished intermediateFile: %s, No. %d\n", intermediateFileName, mapTaskNo)
		}
		noticeMapFinish(mapTaskNo, filename, intermediateFileNames)
		workerID = workerID + "-" + strconv.Itoa(mapTaskNo)
	}

}

func noticeMapFinish(mapTaskNo int, filename string, filenames []string) {
	args := MapFinishArgs{}
	args.TaskNo = mapTaskNo
	args.Filename = filename
	args.IntermediateFileNames = filenames
	reply := MapFinishReply{}
	call("Master.MapFinish", &args, &reply)
	if reply.Noticed {
		// log.Printf("Successful MapTask : File=%s, MapTaskNo=%d\n", filename, mapTaskNo)
		return
	}
	// log.Printf("ERROR: No Feedback: File=%s, MapTaskNo=%d\n", filename, mapTaskNo)
}

//Reduce Task Process
func doReduceTask(reduceTaskNo int, filenames []string, reducef func(string, []string) string) {
	// log.Printf("Starting ReduceTask: Worker-%d, files: %v\n", reduceTaskNo, filenames)
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Open file failed: %s\n", filename)
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

	tempFile, err := ioutil.TempFile(".", "tmp")
	if err != nil {
		log.Println("create temp file failed.")
	}

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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	oname := "mr-out-" + strconv.Itoa(reduceTaskNo)
	os.Rename(tempFile.Name(), oname)
	tempFile.Close()
	// log.Printf("ReduceTask Finish: reduceTaskNo=%d, output=%s\n", reduceTaskNo, oname)
	noticeReduceFinish(reduceTaskNo)
	workerID = workerID + "-" + strconv.Itoa(reduceTaskNo)
}

func noticeReduceFinish(reduceTaskNo int) {
	args := ReduceFinishArgs{}
	args.TaskNo = reduceTaskNo
	reply := ReduceFinishReply{}
	call("Master.ReduceFinish", &args, &reply)
	if reply.Noticed {
		// log.Printf("Successful ReduceTask : ReduceTaskNo=%d\n", reduceTaskNo)
		return
	}
	log.Printf("ERROR: No Feedback: ReduceTaskNo=%d\n", reduceTaskNo)
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
	log.Printf("reply.Y %v\n", reply.Y)
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

	// log.Println(err)
	return false
}
