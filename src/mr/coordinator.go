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

type TaskType int
type Phase int

const (
	UNKNOWN TaskType = 0
	Map     TaskType = 1
	Reduce  TaskType = 2
	Wait    TaskType = 3
	Done    TaskType = 4
)

const (
	Init        Phase = 0
	MapPhase    Phase = 1
	ReducePhase Phase = 2
)

type Task struct {
	TaskID   int64
	ReduceID int
	FileName string

	AllTaskID  []int64
	CreateTime time.Time
	TaskType   TaskType
}

// Coordinator Single Instance
type Coordinator struct {
	Lock         sync.Mutex
	FileNames    []string
	NReduce      int
	ProcessPhase Phase

	mapTaskReady    map[int]Task
	mapTaskProgress map[int]Task
}

var coordinator *Coordinator

func GetInstance(files []string) *Coordinator {
	if coordinator == nil {
		coordinator = &Coordinator{
			Lock:            sync.Mutex{},
			FileNames:       files,
			ProcessPhase:    Init,
			mapTaskReady:    make(map[int]Task),
			mapTaskProgress: make(map[int]Task),
		}
	}
	return coordinator
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := GetInstance(files)
	c.server()
	return c
}

func (c *Coordinator) HandleTaskRequest(req *TaskRequest, reply *TaskReply) error {
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}
