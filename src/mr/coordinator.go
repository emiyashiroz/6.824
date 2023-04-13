package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Coordinator master 定义
type Coordinator struct {
	// Your definitions here.
	Files              []string // 原始输入文件
	MediateFiles       []string // 中间输出文件 默认10
	Status             int      // 当前执行状态0: 在执行map; 1: 在执行reduce; 2: 执行完毕;
	FilesStatus        []int    // 表示map任务分配状态 0: 表示未分配; 1: 表示正在执行; 2: 表示执行完毕
	MediateFilesStatus []int    // 表示reduce任务分配状态 同上
	NReduce            int
}

// Your code here -- RPC handlers for the worker to call.

var lock sync.RWMutex

// Example
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetTask 获取任务
func (c *Coordinator) GetTask(args *ExampleArgs, reply *GetTaskReply) error {
	lock.Lock()
	defer lock.Unlock()
	if c.Status == 0 {
		for i, v := range c.FilesStatus {
			if v == 0 {
				reply.TType = 0
				reply.TaskId = i
				reply.File = c.Files[i]
				reply.NReduce = c.NReduce
				reply.InputNumber = len(c.Files)
				c.FilesStatus[i] = 1
				return nil
			}
		}
	} else if c.Status == 1 {
		for i, v := range c.MediateFilesStatus {
			if v == 0 {
				reply.TType = 1
				reply.TaskId = i
				// reply.File = c.MediateFiles[i]
				reply.NReduce = c.NReduce
				reply.InputNumber = len(c.Files)
				c.MediateFilesStatus[i] = 1
				return nil
			}
		}
	} else {
		reply.TType = 2
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteArgs, reply *ExampleReply) error {
	lock.Lock()
	defer lock.Unlock()
	if args.TType == 0 {
		c.FilesStatus[args.TaskId] = 2
		if check(c.FilesStatus) {
			c.Status = 1
		}
	} else {
		c.MediateFilesStatus[args.TaskId] = 2
		if check(c.MediateFilesStatus) {
			c.Status = 2
		}
	}
	return nil
}

func check(nums []int) bool {
	for i := 0; i < len(nums); i++ {
		if nums[i] != 2 {
			return false
		}
	}
	return true
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockName := coordinatorSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.Status == 2
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:              files,
		NReduce:            nReduce,
		FilesStatus:        make([]int, len(files)),
		MediateFilesStatus: make([]int, nReduce),
		Status:             0,
	}

	// Your code here.
	c.server()
	return &c
}
