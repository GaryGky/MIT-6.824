package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// mapf and reducef are defined by user
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		taskReply := GetTask()
		switch taskReply.TaskInfo.TaskType {
		case Map:
			fmt.Printf("Do Map Task")
			doMap(mapf, taskReply.TaskInfo)
		case Reduce:
			fmt.Printf("Do Reduce Task")
			doReduce(reducef, taskReply.TaskInfo)
		case Wait:
			fmt.Printf("Do Wait ")
			time.Sleep(time.Second * 1)
		case Done:
			fmt.Printf("All task Done")
			return
		}
	}
}

// mapf的输入是 (fileName, fileContent) 输出KV列表 (word -> 1)
func doMap(mapf func(string, string) []KeyValue, task Task) {
	// 从task中解析 filename

	// 根据filename 读取文件内容

	// 将文件名和文件内容放入 mapf中

	// 从mapf取回结果并且存储中间文件 mr-taskID-reduceID (json 格式)
}

// reducef inputs: 一个Key，这个Key对应的value list || return 这个Key出现的次数 = len(values
func doReduce(reducef func(string, []string) string, task Task) {

}

func GetTask() TaskReply {
	taskReq := TaskRequest{}
	taskReply := TaskReply{}
	call("Coordinator.HandleTaskRequest", &taskReq, &taskReply)
	fmt.Printf("reply: %v", taskReply)
	return taskReply
}

func FinishTask() TaskDoneReply {
	taskDoneReq := TaskDoneRequest{}
	taskDoneReply := TaskDoneReply{}
	call("Coordinator.Done", &taskDoneReq, &taskDoneReply)
	return taskDoneReply
}

func call(rpcname string, args interface{}, reply interface{}) bool {
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
