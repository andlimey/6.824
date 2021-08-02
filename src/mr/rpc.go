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

type Task int

const (
	Nothing Task = iota
	Map
	Reduce
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
type RequestTaskArgs struct {
	WorkerID int
}

type ReplyTaskArgs struct {
	TaskType Task // 1 for map, 2 for reduce
	Filename string
	TaskNum  int
	NReduce  int
}

type NotifyMasterArgs struct {
	Ack      int // 1 for done, 0 for error
	TaskNum  int
	TaskType Task // 1 for map, 2 for reduce
	WorkerID int
}

type NotifyMasterReply struct {
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
