package mr

import (
	"log"
	"sync"
	"time"
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
	reply.NReduce = c.NReduce
	reply.InputNumber = len(c.Files)
	if c.Status == 0 {
		for i, v := range c.FilesStatus {
			if v == 0 {
				reply.TType = 0
				reply.TaskId = i
				reply.File = c.Files[i]
				c.FilesStatus[i] = 1
				go c.TimeOutCheck(0, i)
				return nil
			}
		}
		reply.TType = 2
	} else if c.Status == 1 {
		for i, v := range c.MediateFilesStatus {
			if v == 0 {
				reply.TType = 1
				reply.TaskId = i
				c.MediateFilesStatus[i] = 1
				go c.TimeOutCheck(1, i)
				return nil
			}
		}
		reply.TType = 3
	} else {
		reply.TType = 4
	}
	// log.Printf("GetTask type=%d taskId=%d\n", reply.TType, reply.TaskId)
	return nil
}

// TimeOutCheck 任务超时检查, 超时: 需要重置任务
func (c *Coordinator) TimeOutCheck(tType, taskId int) {
	time.Sleep(time.Duration(10 * time.Second)) // 等待15s
	lock.Lock()
	defer lock.Unlock()
	if tType == 0 {
		if c.FilesStatus[taskId] != 2 {
			c.FilesStatus[taskId] = 0
		}
	} else {
		if c.MediateFilesStatus[taskId] != 2 {
			c.MediateFilesStatus[taskId] = 0
		}
	}
}

func (c *Coordinator) CompleteTask(args *CompleteArgs, reply *ExampleReply) error {
	lock.Lock()
	defer lock.Unlock()
	if args.TType == 0 {
		//if c.FilesStatus[args.TaskId] != 1 {
		//	return nil
		//}
		c.FilesStatus[args.TaskId] = 2
		if check(c.FilesStatus) {
			c.Status = 1
		}
	} else {
		//if c.MediateFilesStatus[args.TaskId] != 1 {
		//	return nil
		//}
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
