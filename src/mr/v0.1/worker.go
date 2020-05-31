package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//KeyValue Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//Worker main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	doMapTask(mapf)
	doReduceTask(reducef)

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func doReduceTask(reducef func(string, []string) string) {
	reduceTask, index, err := callForReduce("Master.Reduce")
	if err != nil {
		fmt.Println("failed to call for reduce")
		return
	}
	intermediate := []KeyValue{}
	for _, line := range reduceTask.IntermediateItems {
		if len(line) != 0 {
			kv := strings.Split(line, " ")
			intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(index)
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
	callForReduceFinish("Master.ReduceNotice")
}

func callForReduce(rpcname string) (ReduceTask, int, error) {
	reduceArgs := ReduceArgs{}
	reduceReply := ReduceReply{ReduceTask{make([]string, 1000)}, 0}

	success := call(rpcname, &reduceArgs, &reduceReply)

	if !success {
		return ReduceTask{}, 0, errors.New("")
	}

	return reduceReply.IntermediateItems, reduceReply.Index, nil
}

func callForReduceFinish(rpcname string) {
	reduceFinishArgs := ReduceFinishArgs{}
	reduceFinishReply := ReduceFinishReply{}
	call(rpcname, &reduceFinishArgs, &reduceFinishReply)
}

func doMapTask(mapf func(string, string) []KeyValue) {
	rand.Seed(time.Now().UnixNano())
	amount := rand.Intn(3) + 1
	filenames, err := callForMap("Master.Map", amount)
	fmt.Printf("File: %s\n", filenames)
	if err != nil {
		fmt.Println("Failed to call for map")
		return
	}

	for _, filename := range filenames {
		if filename != "" {
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

			oname := "mr-intermediate-" + filename
			ofile, _ := os.Create(oname)
			for _, item := range intermediate {
				fmt.Fprintf(ofile, "%v %v\n", item.Key, item.Value)
			}
			ofile.Close()
			callForMapFinish("Master.MapNotice", oname)
		}
	}
}

func callForMap(rpcname string, amount int) ([]string, error) {
	mapArgs := MapArgs{amount}
	mapReply := MapReply{make([]string, 5)}
	if !call(rpcname, &mapArgs, &mapReply) {
		return nil, nil
	}
	return mapReply.File, nil
}

func callForMapFinish(rpcname, oname string) (bool, error) {
	mapFinishArgs := MapFinishArgs{oname}
	mapFinishReply := MapFinishReply{}
	call(rpcname, &mapFinishArgs, &mapFinishReply)
	return mapFinishReply.Done, nil
}

//CallExample example function to show how to make an RPC call to the master.
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
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := masterSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
