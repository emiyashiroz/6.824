package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Println("start worker")
	for {
		// 获取任务
		args := ExampleArgs{}
		reply := GetTaskReply{}
		call("Coordinator.GetTask", &args, &reply)
		if reply.TType == 2 {
			fmt.Println("任务已结束")
			break
		}
		// 执行任务 map

		if reply.TType == 0 {
			fmt.Println("doing task map")
			file, err := os.Open(reply.File)
			if err != nil {
				fmt.Printf(":cannot open %dth file %v\n", reply.TaskId, reply.File)
				log.Fatalf("cannot open %v", reply.File)

			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				fmt.Printf("cannot read %v\n", reply.File)
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()
			kva := mapf(reply.File, string(content))
			kvs := make([][]KeyValue, reply.NReduce)
			for _, kv := range kva {
				kvs[ihash(kv.Key)%reply.NReduce] = append(kvs[ihash(kv.Key)%reply.NReduce], kv)
			}
			// 生成nReduce个中间文件 命名 mr-taskId-0~9 (when nReduce=10)
			for i := 0; i < reply.NReduce; i++ {
				// fmt.Printf("produce mr-%d-%d\n", reply.TaskId, i)
				file, _ = os.Open(fmt.Sprintf("mr-%d-%d", reply.TaskId, i))
				enc := json.NewEncoder(file)
				for _, kv := range kvs[i] {
					_ = enc.Encode(&kv)
				}
				file.Close()
			}
		} else { // reduce
			// 汇总
			fmt.Println("doing task reduce")
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
			// reducef
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
					values = append(values, kva[i].Value)
				}
				res := reducef(kva[i].Key, values)
				fmt.Fprintf(oFile, "%v %v\n", kva[i].Key, res)
				i = j
			}
			oFile.Close()
		}
		// 结束通知
		completeArgs := CompleteArgs{
			TType:  reply.TType,
			TaskId: reply.TaskId,
		}
		completeReply := ExampleReply{}
		fmt.Println("complete task notity coordinator")
		call("Coordinator.CompleteTask", &completeArgs, &completeReply)
	}

	//
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// CallExample
// example function to show how to make an RPC call to the coordinator.
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
