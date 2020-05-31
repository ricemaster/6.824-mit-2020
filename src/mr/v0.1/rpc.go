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

//ExampleArgs example to show how to declare the arguments
// and reply for an RPC.
//
type ExampleArgs struct {
	X int
}

//ExampleReply xxx
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//MapArgs xxx
type MapArgs struct {
	Amount int
}

//MapReply xxx
type MapReply struct {
	File []string
}

//MapFinishArgs xxx
type MapFinishArgs struct {
	File string
}

//MapFinishReply xxx
type MapFinishReply struct {
	Done bool
}

//ReduceArgs xxx
type ReduceArgs struct {
}

//ReduceReply xxx
type ReduceReply struct {
	IntermediateItems ReduceTask
	Index             int
}

//ReduceFinishArgs xxx
type ReduceFinishArgs struct {
}

//ReduceFinishReply xxx
type ReduceFinishReply struct {
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
