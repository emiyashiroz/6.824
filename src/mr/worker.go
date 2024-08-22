package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Workman struct {
	WorkerID int
	mapF     func(string, string) []KeyValue
	reduceF  func(string, []string) string
}

func NewWorker(mapF func(string, string) []KeyValue, reduceF func(string, []string) string) *Workman {
	wm := Workman{
		WorkerID: os.Getpid(),
		mapF:     mapF,
		reduceF:  reduceF,
	}
	return &wm
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	wm := NewWorker(mapf, reducef)
	wm.loop()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func (w Workman) CallGetTask() (GetTaskReply, error) {
	args := GetTaskArgs{WorkerID: w.WorkerID}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply, nil
	}
	return GetTaskReply{}, errors.New("call rpc error")
}

func (w Workman) CallReportTask(taskID int, taskType string) (ReportTaskReply, error) {
	args := ReportTaskArgs{
		WorkerID: w.WorkerID,
		TaskID:   taskID,
		TaskType: taskType,
	}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		return reply, nil
	}
	return ReportTaskReply{}, errors.New("call rpc error")
}

func (w Workman) loop() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("recover")
		}
	}()
	for {
		gres, err := w.CallGetTask()
		if err != nil || !gres.Success {
			time.Sleep(time.Second)
			continue
		}
		err = w.handleTask(gres.TaskID, gres.NReduce, gres.MapLength, gres.TaskType, gres.FileName)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		_, _ = w.CallReportTask(gres.TaskID, gres.TaskType)
		time.Sleep(time.Second)
	}
}

func (w Workman) handleTask(taskID, nReduce, mapLength int, taskType, fileName string) error {
	if taskType == TaskTypeMap {
		file, err := os.Open(fileName)
		if err != nil {
			return err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}
		kva := w.mapF(fileName, string(content))
		interMediates := make([][]KeyValue, nReduce)
		for _, kv := range kva {
			hash := ihash(kv.Key) % nReduce
			interMediates[hash] = append(interMediates[hash], kv)
		}
		for i, interMediate := range interMediates {
			imfile, err := os.OpenFile(fmt.Sprintf("im-%d", mapLength+taskID*nReduce+i), os.O_RDWR|os.O_CREATE, 0777)
			if err != nil {
				return err
			}
			for _, kv := range interMediate {
				_, err = imfile.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
				if err != nil {
					return err
				}
			}
			imfile.Close()
		}
	} else if taskType == TaskTypeReduce {
		var intermediate []KeyValue
		for i := 0; i < mapLength; i++ {
			imfile, err := os.OpenFile(fmt.Sprintf("im-%d", taskID+i*nReduce), os.O_RDWR|os.O_CREATE, 0777)
			if err != nil {
				return err
			}
			defer os.Remove(fmt.Sprintf("im-%d", taskID+i*nReduce))
			defer imfile.Close()
			content, err := ioutil.ReadAll(imfile)
			if err != nil {
				return err
			}
			tokens := strings.Split(string(content), "\n")
			for _, token := range tokens {
				if len(token) == 0 {
					continue
				}
				xx := strings.Split(token, " ")
				intermediate = append(intermediate, KeyValue{
					Key:   xx[0],
					Value: xx[1],
				})
			}
		}
		sort.Sort(ByKey(intermediate))
		oname := fmt.Sprintf("mr-out-%d", taskID)
		ofile, err := os.Create(oname)
		if err != nil {
			return err
		}
		defer ofile.Close()
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := w.reduceF(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			_, err = fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			if err != nil {
				return err
			}
			i = j
		}
	} else {
		return errors.New("unsupported task type")
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
