package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	taskQueue DeQueue[int]
	taskMap   map[int]*Task
	nReduce   int
	mapLength int
	wg        sync.WaitGroup
	wgMap     sync.WaitGroup
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task, get := c.taskQueue.LeftPoll()
	if !get {
		return nil
	}
	reply.Success = true
	reply.TaskID = task
	reply.TaskType = c.taskMap[task].Type
	reply.FileName = c.taskMap[task].FileName
	reply.NReduce = c.nReduce
	reply.MapLength = c.mapLength
	c.taskMap[task].workerID = args.WorkerID
	c.taskMap[task].invoke()
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	task, ok := c.taskMap[args.TaskID]
	if !ok {
		panic("error")
	}
	if task.workerID != args.WorkerID {
		return nil
	}
	task.done()
	reply.Success = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.wg.Wait()
	return true
}

func (c *Coordinator) done() {
	c.wg.Done()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.mapLength = len(files)
	c.taskMap = make(map[int]*Task)
	for i, file := range files {
		task := NewTask(TaskTypeMap, file, i, &c)
		c.taskQueue.RightPush(i)
		c.taskMap[i] = task
	}
	c.wgMap.Add(len(files))
	go func() {
		c.wgMap.Wait()
		for i := 0; i < nReduce; i++ {
			task := NewTask(TaskTypeReduce, "", i+len(files), &c)
			c.taskQueue.RightPush(i + len(files))
			c.taskMap[i+len(files)] = task
		}
	}()
	c.wg.Add(len(files) + nReduce)
	c.server()
	return &c
}
