package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use iHash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func iHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	for {
		// 获取任务
		args := ExampleArgs{}
		reply := GetTaskReply{}
		call("Coordinator.GetTask", &args, &reply)
		if reply.TType == 4 {
			break
		}
		if reply.TType == 2 || reply.TType == 3 {
			time.Sleep(1)
			continue
		}
		// 执行任务 map
		if reply.TType == 0 {
			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatalf("cannot open %v, taskId=%d, type=%d, err=%v", reply.File, reply.TaskId, reply.TType, err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()
			kva := mapF(reply.File, string(content))
			kvs := make([][]KeyValue, reply.NReduce)
			for _, kv := range kva {
				kvs[iHash(kv.Key)%reply.NReduce] = append(kvs[iHash(kv.Key)%reply.NReduce], kv)
			}
			// 生成nReduce个中间文件 命名 mr-taskId-0~9 (when nReduce=10)
			for i := 0; i < reply.NReduce; i++ {
				file, err = os.Create(fmt.Sprintf("mr-%d-%d", reply.TaskId, i))
				if err != nil {
					fmt.Println(fmt.Sprintf("mr-%d-%d", reply.TaskId, i), err)
				}
				enc := json.NewEncoder(file)
				for _, kv := range kvs[i] {
					_ = enc.Encode(&kv)
				}
				file.Close()
			}
		} else { // reduce
			n := reply.InputNumber
			Y := reply.TaskId
			kva := []KeyValue{}
			for i := 0; i < n; i++ {
				file, _ := os.Open(fmt.Sprintf("mr-%d-%d", i, Y))
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}
			// 排序
			sort.Sort(ByKey(kva))
			// reduceF
			outFile := fmt.Sprintf("mr-out-%d", Y)
			oFile, _ := os.Create(outFile)
			i, j := 0, 0
			for ; i < len(kva); i++ {
				j = i + 1
				for ; j < len(kva); j++ {
					if kva[i].Key != kva[j].Key {
						break
					}
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				res := reduceF(kva[i].Key, values)
				fmt.Fprintf(oFile, "%v %v\n", kva[i].Key, res)
				i = j - 1
			}
			oFile.Close()
		}
		// 结束通知
		completeArgs := CompleteArgs{
			TType:  reply.TType,
			TaskId: reply.TaskId,
		}
		completeReply := ExampleReply{}
		call("Coordinator.CompleteTask", &completeArgs, &completeReply)
	}
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	args := ExampleArgs{}
	args.X = 99
	reply := ExampleReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
