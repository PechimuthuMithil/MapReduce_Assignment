package mr

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Coordinator struct
type Coordinator struct {
	taskfiles   []string
	nReduce     int
	mapTasks    map[int]*TaskInfo
	reduceTasks map[int]*TaskInfo
	mux         sync.Mutex
	wg          sync.WaitGroup
	done        bool
}

type TaskStatus string

const (
	IDLE        TaskStatus = "IDLE"
	IN_PROGRESS            = "IN_PROGRESS"
	FAILED                 = "FAILED"
	DONE                   = "DONE"
)

type TaskInfo struct {
	filenames  []string
	workerID   string
	status     TaskStatus
	assignedAt time.Time
}

var intRegex = regexp.MustCompile("[0-9]+")
var TIMEOUT = time.Second * 10

func (c *Coordinator) AssignTask(taskRequest *TaskRequest, taskResponse *TaskResponse) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	for taskID, taskInfo := range c.mapTasks {
		if taskInfo.status == IDLE || taskInfo.status == FAILED {
			taskInfo.workerID = taskRequest.WorkerID
			taskInfo.status = IN_PROGRESS
			taskInfo.assignedAt = time.Now()
			taskResponse.TaskID = taskID
			taskResponse.Filenames = taskInfo.filenames
			taskResponse.Type = MAP
			taskResponse.NReduce = c.nReduce
			log.Printf("MAP task assigned: taskId: %d to workerId: %s ", taskResponse.TaskID, taskInfo.workerID)
			return nil
		}
	}

	if c.mapDone() {
		for taskID, taskInfo := range c.reduceTasks {
			if taskInfo.status == IDLE || taskInfo.status == FAILED {
				taskInfo.workerID = taskRequest.WorkerID
				taskInfo.status = IN_PROGRESS
				taskInfo.assignedAt = time.Now()
				taskResponse.TaskID = taskID
				taskResponse.Filenames = taskInfo.filenames
				taskResponse.Type = REDUCE
				taskResponse.NReduce = c.nReduce
				log.Printf("REDUCE task assigned: taskId: %d to workerId: %s ", taskResponse.TaskID, taskInfo.workerID)
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(taskDoneNotification *TaskDoneNotification, taskDoneAck *TaskDoneAck) error {
	if taskDoneNotification.Type == MAP {
		log.Printf("Received MAP DONE notification from worker %s for taskID: %d", taskDoneNotification.WorkerID, taskDoneNotification.TaskID)
		taskInfo := c.mapTasks[taskDoneNotification.TaskID]

		c.mux.Lock()
		if taskInfo.status != DONE {
			taskInfo.status = DONE
			c.updateReduceTasks(taskDoneNotification.Filenames)
		} else {
			log.Printf("Received MAP done notification for a task: %v already marked done, ignoring it\n", taskDoneNotification)
		}
		taskDoneAck.Ack = true
		c.mux.Unlock()
	} else {
		taskInfo := c.reduceTasks[taskDoneNotification.TaskID]
		c.mux.Lock()
		taskInfo.status = DONE
		c.mux.Unlock()
		log.Printf("Marked REDUCE done for taskId %d by workerId %s \n", taskDoneNotification.TaskID, taskDoneNotification.WorkerID)
		taskDoneAck.Ack = true
	}
	return nil
}

func (c *Coordinator) updateReduceTasks(mapIntermediateFiles []string) {
	for _, mapIntermediateFile := range mapIntermediateFiles {
		reduceTaskID := getReduceTaskID(mapIntermediateFile)
		if reduceTask, ok := c.reduceTasks[reduceTaskID]; ok {
			reduceTask.filenames = append(reduceTask.filenames, mapIntermediateFile)
		} else {
			c.reduceTasks[reduceTaskID] = createReduceTask(mapIntermediateFile)
		}
	}
}

func (c *Coordinator) monitorInProgressTasks() {
	for {
		c.mux.Lock()
		if c.done {
			break
		}

		currentTime := time.Now()
		for taskID, taskInfo := range c.mapTasks {
			if taskInfo.status == IN_PROGRESS {
				if currentTime.Sub(taskInfo.assignedAt).Seconds() > TIMEOUT.Seconds() {
					log.Printf("Map task: {taskID: %d workerId: %s}, FAILED. Reassigning...\n", taskID, taskInfo.workerID)
					taskInfo.status = FAILED
				}
			}
		}

		for taskID, taskInfo := range c.reduceTasks {
			if taskInfo.status == IN_PROGRESS {
				if currentTime.Sub(taskInfo.assignedAt).Seconds() > TIMEOUT.Seconds() {
					log.Printf("Reduce task : {taskID: %d workerId: %s}, FAILED. Reassigning...\n", taskID, taskInfo.workerID)
					taskInfo.status = FAILED
				}
			}
		}
		c.mux.Unlock()
		time.Sleep(time.Second * 10)
	}
	c.mux.Unlock()
	c.wg.Done()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := CoordinatorSock()
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
	c.mux.Lock()
	allDone := c.mapDone() && c.reduceDone()
	if allDone {
		c.done = true
		c.mux.Unlock()
		c.wg.Wait()
		RemoveCopyFiles()
		return true
	} else {
		c.mux.Unlock()
		return false
	}
}

// RemoveCopyFiles removes all files in the parent directory that start with "Copy"
func RemoveCopyFiles() {
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "Chunk") {
			err := os.Remove(file.Name())
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func (c *Coordinator) mapDone() bool {
	for _, mapAssignment := range c.mapTasks {
		if mapAssignment.status != DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceDone() bool {
	for _, reduceAssignment := range c.reduceTasks {
		if reduceAssignment.status != DONE {
			return false
		}
	}
	return true
}

// MakeCoordinator create a Coordinator.
// main/mrCoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func splitToChunks(files []string, size int) ([]string, error) {
	if size == -1 {
		return files, nil
	}
	const MB = 1 << 20
	bufferSize := size * MB
	var chunkFiles []string

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		scanner.Split(bufio.ScanWords)

		var chunk strings.Builder
		chunk.Grow(bufferSize)

		chunkNumber := 1
		for scanner.Scan() {
			word := scanner.Text()
			if chunk.Len()+len(word)+1 > bufferSize && chunk.Len() > 0 {
				chunkFilename, err := writeChunkToFile(file, chunkNumber, chunk.String())
				if err != nil {
					return nil, err
				}
				chunkFiles = append(chunkFiles, chunkFilename)
				chunk.Reset()
				chunkNumber++
			}
			chunk.WriteString(word)
			chunk.WriteRune(' ')
		}

		if chunk.Len() > 0 {
			chunkFilename, err := writeChunkToFile(file, chunkNumber, chunk.String())
			if err != nil {
				return nil, err
			}
			chunkFiles = append(chunkFiles, chunkFilename)
		}

		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	return chunkFiles, nil
}

func writeChunkToFile(filename string, chunkNumber int, data string) (string, error) {
	chunkFilename := filename[:3] + "Chunk-" + strconv.Itoa(chunkNumber) + "-" + filename[3:]
	f, err := os.Create(chunkFilename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = f.WriteString(data)
	if err != nil {
		return "", err
	}
	return chunkFilename, nil
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.taskfiles = files
	c.nReduce = nReduce
	c.mapTasks = make(map[int]*TaskInfo)
	var chunkFiles []string

	chunkFiles, err := splitToChunks(files, -1) // -1 implies don't perform chunking.
	if err != nil {
		log.Fatalf("Failed to split files into chunks: %v", err)
	}

	for i, filename := range chunkFiles {
		mapTaskInfo := TaskInfo{}

		// Split the file into two
		// splitFiles, err := splitFileIntoTwo(filename)
		if err != nil {
			log.Fatalf("Failed to split file: %v", err)
		}

		// Assign the split files to the map task
		mapTaskInfo.filenames = []string{filename}
		mapTaskInfo.status = IDLE
		c.mapTasks[i+1] = &mapTaskInfo
	}

	c.reduceTasks = make(map[int]*TaskInfo)
	c.server()

	c.done = false

	c.wg.Add(1)
	go c.monitorInProgressTasks()
	return &c
}

func splitFileIntoTwo(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stats, statsErr := file.Stat()
	if statsErr != nil {
		return nil, statsErr
	}

	size := stats.Size()
	halfSize := int(size / 2)

	buffer := make([]byte, halfSize)
	_, err = file.Read(buffer)
	if err != nil {
		return nil, err
	}

	// Find the nearest word boundary
	i := len(buffer) - 1
	for i > 0 && buffer[i] != ' ' && buffer[i] != '\n' {
		i--
	}

	firstHalfFilename := filename[:3] + "Copy-1-" + filename[3:]
	firstHalfFile, err := os.Create(firstHalfFilename)
	if err != nil {
		return nil, err
	}
	defer firstHalfFile.Close()

	_, err = firstHalfFile.Write(buffer[:i])
	if err != nil {
		return nil, err
	}

	secondHalfFilename := filename[:3] + "Copy-2-" + filename[3:]
	secondHalfFile, err := os.Create(secondHalfFilename)
	if err != nil {
		return nil, err
	}
	defer secondHalfFile.Close()

	_, err = secondHalfFile.Write(buffer[i:])
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(secondHalfFile, file)
	if err != nil {
		return nil, err
	}

	return []string{firstHalfFilename, secondHalfFilename}, nil
}

func createReduceTask(filename string) *TaskInfo {
	taskInfo := TaskInfo{}
	taskInfo.status = IDLE
	taskInfo.filenames = []string{filename}
	return &taskInfo
}

func getReduceTaskID(filename string) int {
	matches := intRegex.FindAllString(filename, -1)
	if len(matches) == 2 {
		reduceTaskID, _ := strconv.Atoi(matches[1])
		return reduceTaskID
	}

	log.Panicf("couldn't parse reduce task id from filename %v\n", filename)
	return -1
}
