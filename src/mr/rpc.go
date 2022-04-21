package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// from worker to master View TaskRequest as HeartBeat
type TaskRequest struct {
}

// from master to worker
type TaskReply struct {
	PhaseInfo Phase
	TaskInfo  Task
	nReduce   int
}

// from worker to master
type TaskDoneRequest struct {
	TaskInfo   Task
	TaskStatus TaskStatus
}

// from master to worker
type TaskDoneReply struct {
	PhaseInfo Phase
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
