package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskArgs struct {
	WorkerID string
}

type TaskReply struct {
	TaskType int
	TaskNo   int
	Files    []string
	NReduce  int
}

type MapFinishArgs struct {
	TaskNo   int
	Filename string
	IntermediateFileNames []string
}

type MapFinishReply struct {
	Noticed bool
}

type ReduceFinishArgs struct {
	TaskNo int
}

type ReduceFinishReply struct {
	Noticed bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
