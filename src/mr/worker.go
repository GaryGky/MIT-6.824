package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
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

// use ihash(key) % NReduce to choose to reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function mapf and reducef are defined by user
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		taskReply := GetTask()
		var err error
		switch taskReply.TaskInfo.TaskType {
		case Map:
			fmt.Printf("Do Map Task")
			err = doMap(mapf, taskReply.TaskInfo, taskReply.nReduce)
		case Reduce:
			fmt.Printf("Do Reduce Task")
			err = doReduce(reducef, taskReply.TaskInfo)
		case Wait:
			fmt.Printf("Do Wait ")
			time.Sleep(time.Second * 1)
		case Done:
			fmt.Printf("All task Done")
			return
		}
		var taskStatus TaskStatus
		if err != nil {
			taskStatus = Fail
		} else {
			taskStatus = Success
		}
		taskDoneReq := TaskDoneRequest{
			TaskInfo:   taskReply.TaskInfo,
			TaskStatus: taskStatus,
		}
		FinishTask(taskDoneReq)
	}
}

// mapf的输入是 (fileName, fileContent) 输出KV列表 (word -> 1)
func doMap(mapf func(string, string) []KeyValue, task Task, nReduce int) error {
	// 从task中解析 filename
	fileName := task.FileName
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open: %v", fileName)
		return err
	}

	// 根据filename 读取文件内容
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
		return err
	}

	// 将文件名和文件内容放入 mapf中
	kva := mapf(fileName, string(content))

	// 从mapf取回结果并且存储中间文件 mr-taskID-reduceID (json 格式)
	intermediate := make(map[int][]KeyValue, 0)
	for i := 0; i < nReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}
	for _, kv := range kva {
		kvList, _ := intermediate[ihash(kv.Key)]
		kvList = append(kvList, kv)
	}

	for reduceID, kvs := range intermediate {
		wg := sync.WaitGroup{}
		wg.Add(1)
		outFileName := fmt.Sprintf("mr-%v-%v-tmp", task.TaskID, reduceID)
		outFile, _ := ioutil.TempFile("./", "tmp_")
		encoder := json.NewEncoder(outFile)
		for _, kv := range kvs {
			err := encoder.Encode(&kv)
			if err != nil {
				log.Fatalf("Json Encode Error: %v", kv)
				return err
			}
		}
		os.Rename(outFile.Name(), outFileName)
		outFile.Close()
	}
	return nil
}

// reducef inputs: 一个Key，这个Key对应的value list || return 这个Key出现的次数 = len(values
func doReduce(reducef func(string, []string) string, task Task) error {
	// Reduce 要拉取ReduceID的文件执行 reducef
	intermediate := make([]KeyValue, 0)

	// 1. 读取文件
	for _, taskID := range task.AllTaskID {
		fileName := fmt.Sprintf("mr-%v-%v-tmp", taskID, task.ReduceID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Cannot Open: %v", fileName)
			return err
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	outputFileName := fmt.Sprintf("mr-out-%v", task.ReduceID)
	outputFile, _ := ioutil.TempFile("./", "tmp_")
	// 2. 遍历每一个key 执行reducef 并写入临时文件
	for i := 0; i < len(intermediate); {
		j := i
		keyTmp := intermediate[i].Key
		values := make([]string, 0)
		for intermediate[j].Key == keyTmp {
			values = append(values, intermediate[j].Key)
			j++
		}
		result := reducef(keyTmp, values)
		fmt.Fprintf(outputFile, "%v %v \n", keyTmp, result)
		i = j
	}

	// 3. 把临时文件通过rename的方式变成最终的文件
	outputFile.Close()
	os.Rename(outputFile.Name(), outputFileName)
	return nil
}

func GetTask() TaskReply {
	taskReq := TaskRequest{}
	taskReply := TaskReply{}
	call("Coordinator.HandleTaskRequest", &taskReq, &taskReply)
	fmt.Printf("reply: %v", taskReply)
	return taskReply
}

func FinishTask(request TaskDoneRequest) TaskDoneReply {
	taskDoneReply := TaskDoneReply{}
	call("Coordinator.HandleTaskDone", &request, &taskDoneReply)
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
