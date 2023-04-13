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

// CompleteArgs 任务完成通知参数
type CompleteArgs struct {
	TType  int // 任务类型 0: map; 1: reduce
	TaskId int // 任务id 用于命名mr-X-Y 和 mr-out-0
}

type GetTaskReply struct {
	TType       int    // 请求任务返回结果类型 0: 分配到map; 1: 分配到reduce; 2: 等待map执行完毕  3:等待reduce任务执行完毕 4:所有任务执行完毕
	TaskId      int    // 任务id 用于命名mr-X-Y 和 mr-out-0
	File        string // 任务文件名
	NReduce     int    // nReduce
	InputNumber int    // 输入文件数
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
