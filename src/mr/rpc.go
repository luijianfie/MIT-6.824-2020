package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.

const (
	MapTask      = 0
	ReduceTask   = 1
	TaskUnknown  = 3
	TaskWaitting = 0
	TaskRunning  = 1
	TaskDone     = 2
)

type TaskInfo struct {
	TaskNo    int
	State     int
	FileIndex int
	PartIndex int
	TaskType  int
	BeginTime time.Time
	FileName  string
	NReduce   int
	NFiles    int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
