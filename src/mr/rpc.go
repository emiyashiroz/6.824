package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// get a job
type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	Success   bool   // 是否获取到了任务
	TaskType  string // map or reduce
	TaskID    int    // 任务ID
	FileName  string
	NReduce   int
	MapLength int
}

// report job finish
type ReportTaskArgs struct {
	WorkerID int
	TaskID   int
	TaskType string
}

type ReportTaskReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
