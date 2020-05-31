package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

//Master xxx
type Master struct {
	// Your definitions here.
	inputFiles        chan string
	intermediateFiles chan string
	intermidiateItem  []string
	totalInputFiles   int
	totalReduceTask   int
	reduceTasks       chan ReduceTask
	index             int
	mapFinished       bool
	done              chan bool
	reduceDone        int
}

type intermediateItem struct {
	word  string
	count int
}

//ReduceTask xxx
type ReduceTask struct {
	IntermediateItems []string
}

//Map Your code here -- RPC handlers for the worker to call.
func (m *Master) Map(args *MapArgs, reply *MapReply) error {
	if m.mapFinished {
		return errors.New("Map Finished")
	}
	fmt.Printf("--Working on Map: %d--\n", args.Amount)
	for i := 0; i < args.Amount; i++ {
		if len(m.inputFiles) == 0 {
			return nil
		}
		reply.File = append(reply.File, <-m.inputFiles)
	}
	return nil
}

//MapNotice xxx
func (m *Master) MapNotice(args *MapFinishArgs, reply *MapFinishReply) error {
	m.intermediateFiles <- args.File
	fmt.Printf("Map Finished: %s\n", args.File)
	return nil
}

//Reduce xxx
func (m *Master) Reduce(args *ReduceArgs, reply *ReduceReply) error {
	if m.index > m.totalReduceTask {
		return errors.New("Reduce finished")
	}
	if m.mapFinished {
		fmt.Printf("--Working on Reduce: %d--\n", m.index)
		reply.IntermediateItems = <-m.reduceTasks
		reply.Index = m.index
		m.index++
		return nil
	}
	return errors.New("Map not finished")
}

//ReduceNotice xxx
func (m *Master) ReduceNotice(args *ReduceFinishArgs, reply *ReduceFinishReply) error {
	m.reduceDone++
	if m.reduceDone == m.totalReduceTask {
		m.done <- true
	}
	return nil
}

//Example an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//Done main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = <-m.done

	return ret
}

//MakeMaster create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.inputFiles = make(chan string, 10)
	m.intermediateFiles = make(chan string, 10)
	m.intermidiateItem = make([]string, 1000)
	m.totalInputFiles = 0
	m.reduceTasks = make(chan ReduceTask, nReduce)
	m.index = 1
	m.mapFinished = false
	m.totalReduceTask = nReduce
	m.done = make(chan bool)
	m.reduceDone = 0

	for _, file := range files {
		m.inputFiles <- file
		m.totalInputFiles++
	}
	close(m.inputFiles)

	go func(m *Master) {
		total := 0
		for {
			if filename, ok := <-m.intermediateFiles; ok == true {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				for _, line := range strings.Split(string(content), "\n") {
					m.intermidiateItem = append(m.intermidiateItem, line)
				}
				total++
			}
			if total == m.totalInputFiles {
				totalItems := len(m.intermidiateItem)
				fmt.Printf("Total Items: %d\n", totalItems)
				everyTaskItems := totalItems / nReduce
				sort.Strings(m.intermidiateItem)

				for i := 0; i < nReduce; i++ {
					if i == 9 {
						m.reduceTasks <- ReduceTask{m.intermidiateItem[i*everyTaskItems:]}
					} else {
						m.reduceTasks <- ReduceTask{m.intermidiateItem[i*everyTaskItems : (i+1)*everyTaskItems]}
					}
				}
				m.mapFinished = true
				return
			}
		}
	}(&m)

	m.server()
	return &m
}
