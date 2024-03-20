package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

var idList sync.Map
var workerID string

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func readFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return "", err
	}
	file.Close()
	return string(content), nil
}

func GetId() string {
	const charset = "1234567890"
	const length = 4
	for {
		seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		b := make([]byte, length)
		for i := range b {
			b[i] = charset[seededRand.Intn(len(charset))]
		}
		id := "W-" + string(b)
		_, loaded := idList.LoadOrStore(id, struct{}{})
		if !loaded {
			return id
		}
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	rand.Seed(time.Now().UnixNano())
	workerID = GetId()

	log.Printf("Initializing workerID: %s", workerID)

	for {
		task, err := GetTask()
		if err != nil {
			log.Println("Couldn't connect to Coordinator,quitting")
			break
		}
		if tasksAssigned(task) {
			if task.Type == MAP {
				doMap(mapf, task)
			} else if task.Type == REDUCE {
				doReduce(reducef, task)
			} else {
				log.Printf("Unknown task assigned to workerID %s\n", workerID)
			}
		}
		time.Sleep(2 * time.Second)
	}

}

func doMap(mapf func(string, string) []KeyValue, task *TaskResponse) {
	taskID := task.TaskID
	if len(task.Filenames) < 1 {
		log.Printf("No files provided to map worker. Invalid task: %v\n", task)
		return
	}

	intermediatekeyValues := []KeyValue{}
	LocalFileNames := []string{}
	encoders := make(map[int]*json.Encoder)

	for _, filename := range task.Filenames {
		content, err := readFile(filename)
		if err != nil {
			log.Fatalf("Couldn't read from %s\n", filename)
		}

		// Append key values from each file to intermediatekeyValues
		intermediatekeyValues = append(intermediatekeyValues, mapf(filename, content)...)
	}

	for i := 0; i < task.NReduce; i++ {
		tmpFile, err := ioutil.TempFile("", "map")
		if err != nil {
			log.Fatal("Couldn't create temp file\n")
		}
		LocalFileNames = append(LocalFileNames, tmpFile.Name())
		enc := json.NewEncoder(tmpFile)
		encoders[i] = enc
	}

	for _, kv := range intermediatekeyValues {
		err := encoders[ihash(kv.Key)%task.NReduce].Encode(&kv)
		if err != nil {
			log.Fatalf("Couldn't encode %v\n", &kv)
		}
	}

	for i := 0; i < task.NReduce; i++ {
		mapOutputFileName := fmt.Sprintf("mr-%d-%d.txt", taskID, i)
		os.Rename(LocalFileNames[i], mapOutputFileName)
		LocalFileNames[i] = mapOutputFileName
	}
	TaskDone(LocalFileNames, taskID, MAP)
}

func doReduce(reducef func(string, []string) string, task *TaskResponse) {
	filenames := task.Filenames
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		fileToReduce, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Couldn't open file %s to reduce\n", filename)
		}

		dec := json.NewDecoder(fileToReduce)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", task.TaskID)

	tmpFile, _ := ioutil.TempFile("", "pre-")

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpFile.Close()

	os.Rename(tmpFile.Name(), oname)

	TaskDone([]string{oname}, task.TaskID, REDUCE)
}

func GetTask() (*TaskResponse, error) {
	taskRequest := TaskRequest{}
	taskRequest.WorkerID = workerID
	taskResponse := TaskResponse{}
	if call("Coordinator.AssignTask", &taskRequest, &taskResponse) {
		return &taskResponse, nil
	} else {
		return nil, errors.New("Couldn't connect to Coordinator")
	}

}

func TaskDone(filenames []string, taskID int, taskType TaskType) {
	taskDoneNotification := TaskDoneNotification{}
	taskDoneNotification.WorkerID = workerID
	taskDoneNotification.Filenames = filenames
	taskDoneNotification.TaskID = taskID
	taskDoneNotification.Type = taskType

	taskDoneAck := TaskDoneAck{}

	if call("Coordinator.TaskDone", &taskDoneNotification, &taskDoneAck) {
		if !taskDoneAck.Ack {
			log.Panicf("Coordinator didnt ack")
		}
	} else {
		log.Panicf("Couldn't notify Coordinator")
	}

}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := CoordinatorSock()
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

func tasksAssigned(task *TaskResponse) bool {
	return len(task.Filenames) > 0
}
