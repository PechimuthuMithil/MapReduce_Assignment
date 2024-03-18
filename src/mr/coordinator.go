package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Unassigned = 0
	InProgress = 1
	Complete   = 2
)

type Coordinator struct {
	// Your definitions here.
	Map_tasks              map[string]*task_details
	Reduce_tasks           map[int]*task_details
	Remaining_map_tasks    int
	Remaining_reduce_tasks int
	CompletedWorkerList    []int
	Reduce_regions         int
	MuLock                 sync.Mutex
}

type task_details struct {
	id        int
	status    int
	workerId  int
	workerNum int
}

func (c *Coordinator) getUnassignedMapTask() (bool, string) {
	var unassigned = false
	var filename = ""
	var task *task_details

	for filename, task = range c.Map_tasks {
		if task.status == Unassigned {
			unassigned = true
			break
		}
	}

	return unassigned, filename
}

func (c *Coordinator) getUnassignedReduceTask() (bool, int) {
	var unassigned = false
	var reduceTaskNum = -1
	var task *task_details

	for reduceTaskNum, task = range c.Reduce_tasks {
		if task.status == Unassigned {
			unassigned = true
			break
		}
	}

	return unassigned, reduceTaskNum
}

func (c *Coordinator) WaitForMapWorker(fileName string) {
	time.Sleep(time.Second * 10)
	c.MuLock.Lock()
	defer c.MuLock.Unlock()
	task := c.Map_tasks[fileName]
	if task.status == InProgress {
		task.status = Unassigned
	}
}
func (c *Coordinator) WaitForReduceWorker(reduceNum int) {
	time.Sleep(time.Second * 10)
	c.MuLock.Lock()
	defer c.MuLock.Unlock()
	task := c.Reduce_tasks[reduceNum]
	if task.status == InProgress {
		task.status = Unassigned
	}
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.MuLock.Lock()
	defer c.MuLock.Unlock()
	if c.Remaining_map_tasks > 0 {
		unassignedTaskPresent, filename :=
			c.getUnassignedMapTask()

		// Ask if there are unassigned map tasks
		if unassignedTaskPresent {
			task := c.Map_tasks[filename]
			task.status = InProgress
			task.workerId = args.WorkerId
			task.workerNum = args.WorkerId

			reply.TaskId = task.id
			reply.TaskType = Map
			reply.MapFileName = filename
			reply.ReplyStatus = TaskAssigned
			reply.MapWorkerNum = task.workerNum
			reply.ReduceBuckets = c.Reduce_regions

			go c.WaitForMapWorker(filename)

		} else {
			reply.ReplyStatus = WaitForMoreTasks
		}

	} else if c.Remaining_reduce_tasks > 0 {

		unassignedTaskPresent, reduceWorkerNumber :=
			c.getUnassignedReduceTask()

		// all map tasks have completed.
		// begin reduce functions
		if unassignedTaskPresent {
			task := c.Reduce_tasks[reduceWorkerNumber]
			task.status = InProgress
			task.workerId = args.WorkerId
			task.workerNum = args.WorkerId

			reply.TaskId = task.id
			reply.TaskType = Reduce
			reply.ReduceWorkerNum = reduceWorkerNumber
			reply.ReplyStatus = TaskAssigned
			reply.ReduceBuckets = c.Reduce_regions
			reply.CompletedMapWorkers = c.CompletedWorkerList

			go c.WaitForReduceWorker(reduceWorkerNumber)
		} else {
			reply.ReplyStatus = WaitForMoreTasks
		}

	} else {
		reply.ReplyStatus = SafelyExit
	}
	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	var task *task_details
	c.MuLock.Lock()
	defer c.MuLock.Unlock()
	switch args.TaskType {
	case Map:
		task = c.Map_tasks[args.MapFilename]
	case Reduce:
		task = c.Reduce_tasks[args.ReduceWorkerNum]
	}
	if (task.id == args.TaskId) && (task.status == InProgress) {
		if !args.TaskSucceeded {
			task.status = Unassigned
			reply.ReplyStatus = MoreTasks

		} else {
			task.status = Complete
			switch args.TaskType {
			case Map:
				c.Remaining_map_tasks--
				c.CompletedWorkerList =
					append(c.CompletedWorkerList,
						task.workerNum)
			case Reduce:
				c.Remaining_reduce_tasks--
			}
			if (c.Remaining_map_tasks == 0) &&
				(c.Remaining_reduce_tasks == 0) {
				reply.ReplyStatus = SafelyExit
			} else {
				reply.ReplyStatus = MoreTasks
			}
		}
	} else {
		if task.id != args.TaskId {
			reply.ReplyStatus = StaleTaskCompletion
		} else if task.status == Unassigned {
			reply.ReplyStatus = StaleTaskCompletion
		} else {
			reply.ReplyStatus = MoreTasks
		}
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.MuLock.Lock()
	defer c.MuLock.Unlock()
	mapComplete := c.Remaining_map_tasks == 0
	reduceComplete := c.Remaining_reduce_tasks == 0
	ret = mapComplete && reduceComplete

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Map_tasks = make(map[string]*task_details)
	c.Reduce_tasks = make(map[int]*task_details)
	c.CompletedWorkerList = make([]int, 0, len(files))
	c.MuLock.Lock()

	for i, filename := range files {
		c.Map_tasks[filename] = &task_details{
			id:     i,
			status: Unassigned,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.Reduce_tasks[i] = &task_details{
			id:     i,
			status: Unassigned,
		}
	}
	c.Reduce_regions = nReduce
	c.Remaining_map_tasks = len(files)
	c.Remaining_reduce_tasks = nReduce
	c.MuLock.Unlock()

	c.server()
	return &c
}
