package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// Returns a unique 64-bit int WorkerId
func GetId() int {

	x := rand.NewSource(time.Now().UnixNano())
	y := rand.New(x)

	return int(y.Intn(time.Now().Nanosecond()))
}

type MapFunction func(string, string) []KeyValue

type ReduceFunction func(string, []string) string

type WorkerTaskDetails struct {
	Id int

	TaskType int

	MapFilename string

	MapWorkerNum int

	ReduceRegions int

	ReduceWorkerNum int

	MapWorkersList []int
}

type WorkerInfo struct {
	Id int

	MapF MapFunction

	ReduceF ReduceFunction

	Task *WorkerTaskDetails
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := WorkerInfo{}
	w.Id = GetId()
	w.MapF = mapf
	w.ReduceF = reducef
	w.Task = new(WorkerTaskDetails)

	isRunning := true
	for isRunning {
		status := w.CallRequestTask()
		var taskSucceeded bool = false
		if status == TaskAssigned {
			err := w.PerformTask()
			if err == nil {
				taskSucceeded = true
			}
			status = w.CallTaskComplete(taskSucceeded)
		}

		switch status {
		case MoreTasks:
			continue
		case WaitForMoreTasks:
			time.Sleep(time.Second)
		case StaleTaskCompletion:
			if w.Task.TaskType == Map {
				// delete map files
			} else {
				// delete reduce output file
			}
		case SafelyExit:
			isRunning = false
		}
	}
}

func (w *WorkerInfo) PerformTask() error {
	intermediate := []KeyValue{}
	task := w.Task
	if task.TaskType == Map {
		file, err := os.Open(task.MapFilename)
		if err != nil {
			return err
		}
		defer file.Close()

		content, err := io.ReadAll(file)
		if err != nil {
			return err
		}

		kva := w.MapF(task.MapFilename, string(content))
		intermediate = append(intermediate, kva...)

		// sort.Sort(ByKey(intermediate))

		reduce_regionkva := make([][]KeyValue, task.ReduceRegions)

		for _, item := range intermediate {

			partitionKey := ihash(item.Key) % task.ReduceRegions
			reduce_regionkva[partitionKey] =
				append(reduce_regionkva[partitionKey], item)
		}

		for i := 0; i < task.ReduceRegions; i++ {
			opFilename :=
				"mr-" + strconv.Itoa(task.MapWorkerNum) + "-" + strconv.Itoa(i)
			opFile, err := os.Create(opFilename)
			if err != nil {
				return err
			}
			for _, item := range reduce_regionkva[i] {
				fmt.Fprintf(opFile, "%v %v\n", item.Key, item.Value)
			}
			opFile.Close()
		}
	} else {
		keyTotal := []KeyValue{}
		for _, fileNo := range task.MapWorkersList {
			ipFileName :=
				"mr-" + strconv.Itoa(fileNo) + "-" + strconv.Itoa(task.ReduceWorkerNum)
			ipFile, err := os.Open(ipFileName)
			if err != nil {
				return err
			}
			sc := bufio.NewScanner(ipFile)
			sc.Split(bufio.ScanLines)

			for sc.Scan() {
				line := sc.Text()
				splitLine := strings.Split(line, " ")
				key := splitLine[0]
				count := splitLine[1]

				kv := KeyValue{key, count}
				keyTotal = append(keyTotal, kv)

			}

			ipFile.Close()
		}

		sort.Sort(ByKey(keyTotal))

		opFileName := "mr-out-" + strconv.Itoa(task.ReduceWorkerNum)
		opFile, err := os.Create(opFileName)

		if err != nil {
			// log.Printf("Cannot create filename %v for Reduce operation. Error: %v", opFileName, err)

			return err
		}

		i := 0

		for i < len(keyTotal) {

			j := i + 1

			for j < len(keyTotal) &&
				keyTotal[j].Key == keyTotal[i].Key {

				j++
			}

			values := []string{}

			for k := i; k < j; k++ {

				values = append(values, keyTotal[k].Value)
			}

			output := w.ReduceF(keyTotal[i].Key, values)

			fmt.Fprintf(opFile, "%v %v\n", keyTotal[i].Key, output)

			i = j
		}
		//		log.Printf ("\n\nClosing file: %s\n\n", opFileName)
		opFile.Close()
	} // end of reduce

	return nil
}

func (w *WorkerInfo) CallTaskComplete(taskSucceeded bool) int {

	// log.Printf(trace())

	args := TaskCompleteArgs{}

	reply := TaskCompleteReply{}

	args.MapFilename = w.Task.MapFilename
	args.TaskId = w.Task.Id
	args.TaskSucceeded = taskSucceeded
	args.TaskType = w.Task.TaskType
	args.ReduceWorkerNum = w.Task.ReduceWorkerNum

	ok := call("Coordinator.TaskComplete", &args, &reply)

	if ok {
		// log.Printf("Task completion informed succesfully\n")
		return reply.ReplyStatus
	} else {
		// log.Printf("Call failed. Exiting...!\n")
		return SafelyExit
	}
}

func (w *WorkerInfo) CallRequestTask() int {

	// log.Printf(trace())

	var taskStatus int

	// declare an argument structure.
	args := AssignTaskArgs{}

	// fill in the argument(s).
	args.WorkerId = w.Id

	// declare a reply structure.
	reply := AssignTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.AssignTask" tells the
	// receiving server that we'd like to call
	// the AssignTask() method of struct Coordinator.
	ok := call("Coordinator.AssignTask", &args, &reply)

	if ok {
		// perform the task assigned
		// log.Printf("Task Details %v\n", reply)
		task := w.Task
		task.Id = reply.TaskId
		task.TaskType = reply.TaskType

		switch {
		case task.TaskType == Map:
			task.MapFilename = reply.MapFileName
			task.ReduceRegions = reply.ReduceBuckets
			task.MapWorkerNum = reply.MapWorkerNum
		case task.TaskType == Reduce:
			task.ReduceWorkerNum = reply.ReduceWorkerNum
			task.MapWorkersList = reply.CompletedMapWorkers
		}

		taskStatus = reply.ReplyStatus

	} else {
		// log.Printf("Call failed. Exiting...!\n")
		taskStatus = SafelyExit

	}

	return taskStatus
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
