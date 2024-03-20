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

type TaskType string

const (
	MAP    TaskType = "MAP"
	REDUCE TaskType = "REDUCE"
)

type TaskRequest struct {
	WorkerID string
}

type TaskDoneNotification struct {
	WorkerID  string
	Filenames []string
	TaskID    int
	Type      TaskType
}

type TaskDoneAck struct {
	Ack bool
}

type TaskResponse struct {
	TaskID    int
	Type      TaskType
	Filenames []string
	NReduce   int
}

func CoordinatorSock() string {
	s := "/var/tmp/cs612-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
