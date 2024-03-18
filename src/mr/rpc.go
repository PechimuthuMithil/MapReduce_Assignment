package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	Map                 = 0
	Reduce              = 1
	SafelyExit          = -1
	MoreTasks           = 0
	WaitForMoreTasks    = 1
	TaskAssigned        = 2
	StaleTaskCompletion = 3
)

type AssignTaskArgs struct {
	WorkerId int
}

type AssignTaskReply struct {
	ReplyStatus         int
	TaskId              int
	TaskType            int
	MapWorkerNum        int
	MapFileName         string
	ReduceWorkerNum     int
	ReduceBuckets       int
	CompletedMapWorkers []int
}

type TaskCompleteArgs struct {
	TaskType        int
	MapFilename     string
	ReduceWorkerNum int
	TaskId          int
	TaskSucceeded   bool
}

type TaskCompleteReply struct {
	ReplyStatus int
}

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
