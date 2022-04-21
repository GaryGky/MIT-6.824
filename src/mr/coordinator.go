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
type TaskStatus int

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
	FinishPhase Phase = 3
)

const (
	Success TaskStatus = 1
	Fail    TaskStatus = 2
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
	AllTaskID    []int64

	mapTaskReady    map[int64]Task
	mapTaskProgress map[int64]Task

	reduceTaskReady  map[int64]Task
	reduceTaskFinish map[int64]Task
}

var coordinator *Coordinator

func GetInstance() *Coordinator {
	if coordinator == nil {
		coordinator = &Coordinator{
			Lock:             sync.Mutex{},
			ProcessPhase:     Init,
			mapTaskReady:     make(map[int64]Task),
			mapTaskProgress:  make(map[int64]Task),
			reduceTaskReady:  make(map[int64]Task),
			reduceTaskFinish: make(map[int64]Task),
		}
	}
	return coordinator
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := GetInstance()
	c.FileNames = files
	c.server()
	return c
}

func (c *Coordinator) HandleTaskRequest(req *TaskRequest, reply *TaskReply) error {
	reply.PhaseInfo = c.ProcessPhase
	reply.nReduce = c.NReduce
	var task Task
	switch c.ProcessPhase {
	case Init:
		task = Task{
			TaskType: Wait,
		}
	case ReducePhase:
		// 扫描任务 如果存在超时的任务 则分配给Worker
		for _, task := range c.reduceTaskReady {
			if time.Since(task.CreateTime) > time.Second*10 {
				reply.TaskInfo = Task{
					TaskID:     task.TaskID,
					ReduceID:   task.ReduceID,
					FileName:   task.FileName,
					AllTaskID:  c.AllTaskID,
					CreateTime: time.Now(),
					TaskType:   Reduce,
				}
				return nil
			}
		}

		// 如果任务已经全部分配出去了 返回wait
		if len(c.reduceTaskReady) == 0 {
			task = Task{
				TaskType: Wait,
			}
		}

		// 如果还有任务可以分配 则分配给Worker
		for _, task := range c.reduceTaskReady {
			reply.TaskInfo = Task{
				TaskID:     task.TaskID,
				ReduceID:   task.ReduceID,
				FileName:   task.FileName,
				AllTaskID:  c.AllTaskID,
				CreateTime: time.Now(),
				TaskType:   Reduce,
			}
			return nil
		}
	case MapPhase:
		for _, task := range c.mapTaskReady {
			if time.Since(task.CreateTime) > time.Second*10 {
				reply.TaskInfo = Task{
					TaskID:     task.TaskID,
					ReduceID:   task.ReduceID,
					FileName:   task.FileName,
					AllTaskID:  c.AllTaskID,
					CreateTime: time.Now(),
					TaskType:   Map,
				}
				return nil
			}
		}

		// 如果任务已经全部分配出去了 返回wait
		if len(c.mapTaskReady) == 0 {
			task = Task{
				TaskType: Wait,
			}
		}

		// 如果还有任务可以分配 则分配给Worker
		for _, task := range c.mapTaskReady {
			reply.TaskInfo = Task{
				TaskID:     task.TaskID,
				ReduceID:   task.ReduceID,
				FileName:   task.FileName,
				AllTaskID:  c.AllTaskID,
				CreateTime: time.Now(),
				TaskType:   Map,
			}
			return nil
		}
	case FinishPhase:
		task = Task{
			TaskType: Done,
		}
	}
	reply.TaskInfo = task
	return nil
}

func (c *Coordinator) HandleTaskDone(req *TaskDoneRequest, reply *TaskDoneReply) error {
	if req.TaskStatus == Fail {
		return nil
	} else {
		switch req.TaskInfo.TaskType {
		case Map:
			if _, ok := c.mapTaskReady[req.TaskInfo.TaskID]; ok {
				delete(c.mapTaskReady, req.TaskInfo.TaskID)
			}
		case Reduce:
			if _, ok := c.reduceTaskReady[req.TaskInfo.TaskID]; ok {
				delete(c.reduceTaskReady, req.TaskInfo.TaskID)
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// Done main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.ProcessPhase == FinishPhase
}
