package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var wg sync.WaitGroup

type Master struct {
	// Your definitions here.
	mux sync.Mutex

	done                   chan bool
	nextMapTaskNo          int
	totalInputFiles        int
	taskPhase              int // 0:map phase;  1:reduce phase
	inputFiles             chan string
	mapTasks               map[int]string // [WhickWorkerDidThisMapTask:Filename]
	mapFinish              map[string]int //[Filename:SuccessMapTaskNo]
	nReduce                int
	nextReduceTaskNo       int
	failedMapTaskNo        map[int]int
	reduceTasks            map[int][]string //[successfulMapTaskNo:intermediateFileList]
	inputIntermediateFiles chan []string
	reduceFinish           map[int]bool
	failedReduceFinish     []int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Task(args *TaskArgs, reply *TaskReply) error {
	m.mux.Lock()
	taskType := m.taskPhase
	m.mux.Unlock()
	switch taskType {
	case 0:
		reply.TaskType = taskType

		taskNo := m.nextMapTaskNo
		reply.TaskNo = taskNo

		filename := <-m.inputFiles
		reply.Files = []string{filename}

		reply.NReduce = m.nReduce

		// fmt.Printf("--> assigned MapTask: File=%s, MapTaskNo=%d\n", filename, taskNo)

		wg.Add(1)
		go checkMapTask(m, taskNo, filename)

		m.nextMapTaskNo++

		return nil
	case 1:
		reply.TaskType = taskType
		taskNo := m.nextReduceTaskNo
		reply.TaskNo = taskNo

		m.mux.Lock()
		fmt.Printf("-->Channel[inputIntermediateFiles] Size: %d\n", len(m.inputIntermediateFiles))
		filenames := <-m.inputIntermediateFiles
		m.nReduce--
		m.mux.Unlock()
		reply.Files = filenames

		wg.Add(1)
		go checkReduceTask(m, taskNo, filenames)

		m.nextReduceTaskNo++

		return nil

	default:
		return nil

	}

}

func (m *Master) MapFinish(args *MapFinishArgs, reply *MapFinishReply) error {
	reply.Noticed = true
	taskNo := args.TaskNo
	filename := args.Filename
	m.mux.Lock()
	_, failed := m.failedMapTaskNo[taskNo]
	m.mux.Unlock()
	if failed {
		return nil
	}
	m.mux.Lock()
	m.mapFinish[filename] = taskNo
	fmt.Printf("-->MapTask Successful[%d/%d]: %s by Worker-%d\n", len(m.mapFinish), m.totalInputFiles, args.Filename, args.TaskNo)

	if len(m.mapFinish) == m.totalInputFiles {
		// fmt.Printf("%v\n", m.mapFinish)
		m.taskPhase = 1
		for i := 0; i < m.nReduce; i++ {
			for _, successfulMapTaskNo := range m.mapFinish {
				filename := fmt.Sprintf("mr-%s-%s", strconv.Itoa(successfulMapTaskNo), strconv.Itoa(i))
				m.reduceTasks[i] = append(m.reduceTasks[i], filename)
			}
			m.inputIntermediateFiles <- m.reduceTasks[i]
		}
	}
	m.mux.Unlock()
	return nil
}

func (m *Master) ReduceFinish(args *ReduceFinishArgs, reply *ReduceFinishReply) error {
	m.mux.Lock()
	reply.Noticed = true
	m.reduceFinish[args.TaskNo] = true
	fmt.Printf("-->ReduceTask Successful[%d left]: Worker-%d\n", m.nReduce, args.TaskNo)
	if m.nReduce == 0 {
		m.done <- true
	}
	m.mux.Unlock()
	return nil
}

func checkMapTask(m *Master, taskNo int, filename string) {
	defer wg.Done()
	time.Sleep(time.Second * 10)
	m.mux.Lock()
	if _, done := m.mapFinish[filename]; !done {
		m.inputFiles <- filename
		m.failedMapTaskNo[taskNo] = 1
		fmt.Printf("--> MapTask Failed: Worker-%d, files: %v\n", taskNo, filename)
	}
	m.mux.Unlock()
}

func checkReduceTask(m *Master, taskNo int, filenames []string) {
	defer wg.Done()
	time.Sleep(time.Second * 10)
	m.mux.Lock()
	if _, done := m.reduceFinish[taskNo]; !done {
		m.inputIntermediateFiles <- filenames
		fmt.Printf("-->Channel[inputIntermediateFiles] Size: %d\n", len(m.inputIntermediateFiles))
		m.failedReduceFinish = append(m.failedReduceFinish, taskNo)
		m.nReduce++
		fmt.Printf("--> ReduceTask Failed: Worker-%d, files: %v\n", taskNo, filenames)
	}
	m.mux.Unlock()
}

//
// an example RPC handler.
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
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	wg.Wait()

	ret = <-m.done

	if len(m.failedReduceFinish) != 0 {
		fmt.Printf("%v\n", m.failedReduceFinish)
		for _, taskNo := range m.failedReduceFinish {
			filename := "mr-out-" + strconv.Itoa(taskNo)
			err := os.Remove(filename)
			if err != nil {
				fmt.Println("Delete file failed: ", filename)
			}
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nextMapTaskNo = 1
	m.nextReduceTaskNo = 1
	m.inputFiles = make(chan string, 100)
	m.mapTasks = make(map[int]string)
	m.mapFinish = make(map[string]int)
	m.failedMapTaskNo = make(map[int]int)
	m.reduceTasks = make(map[int][]string)
	m.inputIntermediateFiles = make(chan []string, nReduce)
	m.done = make(chan bool)
	m.reduceFinish = make(map[int]bool)
	m.taskPhase = 0
	m.nReduce = nReduce

	for _, file := range files {
		m.inputFiles <- file
		m.totalInputFiles++
	}

	m.server()
	// fmt.Println("-----Server Started------")
	return &m
}
