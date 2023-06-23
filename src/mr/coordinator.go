package mr

import (
	//"io/ioutil"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TaskState int

const (
	TaskIdle TaskState = iota
	TaskInProgress
	TaskCompleted
)

type TaskType int

const (
	TaskNone TaskType = iota
	TaskWait
	TaskMap // in case of no task
	TaskReduce
)

type Task struct {
	TaskType     TaskType
	FileName     string
	ReduceNumber int
	MapNumber    int
	MapIndex     int
	ReduceIndex  int
}

type TaskMetadata struct {
	startTime time.Time
	taskState TaskState
}

type WorkingPhase int

const (
	PhaseMap WorkingPhase = iota
	PhaseReduce
	PhaseCompleted
)

type Coordinator struct {
	// Your definitions here.
	mapTasks             sync.Map
	reduceTasks          sync.Map
	files                []string
	nReduce              int
	nMap                 int
	mutex                sync.Mutex
	phase                WorkingPhase
	completedMapTasks    atomic.Int32
	completedReduceTasks atomic.Int32
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

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
	return c.completedReduceTasks.Load() == int32(c.nReduce)
}

// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
func (c *Coordinator) GetTask(_ *EmptyArgs, reply *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.phase == PhaseMap {
		*reply = c.getAvailableMapTask()
		if reply.TaskType != TaskWait {
			log.Printf("GetTask: PhaseMap. Available task: %v\n", reply.MapIndex)
		}
	} else if c.phase == PhaseReduce {
		*reply = c.getAvailableReduceTask()
		if reply.TaskType != TaskNone {
			log.Printf("GetTask: PhaseReduce. Available task: %v\n", reply.ReduceIndex)
		}
	}

	return nil
}

func (c *Coordinator) MarkTaskCompleted(task *Task, _ *EmptyReply) error {
	switch task.TaskType {
	case TaskMap:
		taskMetadata, ok := c.mapTasks.Load(task.MapIndex)

		if ok && taskMetadata.(TaskMetadata).taskState == TaskCompleted {
			return nil
		}

		log.Printf("MarkTaskCompleted: Map task number %v\n", task.MapIndex)
		c.completedMapTasks.Add(1)
		c.mapTasks.Store(task.MapIndex, TaskMetadata{
			taskState: TaskCompleted,
		})
		if c.completedMapTasks.Load() == int32(c.nMap) {
			c.phase = PhaseReduce
		}
	case TaskReduce:
		taskMetadata, ok := c.reduceTasks.Load(task.ReduceIndex)
		if ok && taskMetadata.(TaskMetadata).taskState == TaskCompleted {
			return nil
		}

		log.Printf("MarkTaskCompleted: Reduce task number %v\n", task.ReduceIndex)
		c.completedReduceTasks.Add(1)
		c.reduceTasks.Store(task.ReduceIndex, TaskMetadata{
			taskState: TaskCompleted,
		})
		taskMetadata, ok = c.reduceTasks.Load(task.ReduceIndex)

		log.Printf("MarkTaskCompleted: Updated task %v\n", taskMetadata)
		if c.completedReduceTasks.Load() == int32(c.nReduce) {
			c.phase = PhaseCompleted
		}
	default:
		return nil
	}

	return nil
}

/*
 * Check if tasks are timed out, and if so, ignore the current worker and set the task status to idle
 */
func (c *Coordinator) monitorTasks() {
	for {
		time.Sleep(time.Second)

		c.mapTasks.Range(func(num, r interface{}) bool {
			taskMetadata := r.(TaskMetadata)
			if taskMetadata.taskState != TaskInProgress {
				return true
			}
			if time.Since(taskMetadata.startTime) > 15*time.Second {
				log.Printf("monitorTasks: Map task number %v\n", num)
				c.mapTasks.Store(num, TaskMetadata{
					taskState: TaskIdle,
				})
			}
			return true
		})
		c.reduceTasks.Range(func(num, r interface{}) bool {
			taskMetadata := r.(TaskMetadata)
			if taskMetadata.taskState != TaskInProgress {
				return true
			}
			if time.Since(taskMetadata.startTime) > 10*time.Second {
				log.Printf("monitorTasks: Reduce task number %v\n", num)
				c.reduceTasks.Store(num, TaskMetadata{
					taskState: TaskIdle,
				})
			}
			return true
		})
	}
}

func (c *Coordinator) getAvailableMapTask() (t Task) {
	c.mapTasks.Range(func(num, r interface{}) bool {
		task := r.(TaskMetadata)
		if task.taskState == TaskIdle {
			t.TaskType = TaskMap
			t.FileName = c.files[num.(int)]
			t.MapIndex = num.(int)
			t.ReduceNumber = c.nReduce
			return false
		}
		return true
	})

	if t.TaskType != TaskNone {
		c.mapTasks.Store(t.MapIndex, TaskMetadata{
			taskState: TaskInProgress,
			startTime: time.Now(),
		})
	} else {
		// inform the worker to wait instead of exit
		t.TaskType = TaskWait
	}

	return
}

func (c *Coordinator) getAvailableReduceTask() (t Task) {
	c.reduceTasks.Range(func(num, r interface{}) bool {
		task := r.(TaskMetadata)
		// log.Printf("Check task number %v - %v\n", num, task)
		if task.taskState == TaskIdle {
			t.TaskType = TaskReduce
			t.ReduceIndex = num.(int)
			t.MapNumber = c.nMap
			return false
		}
		return true
	})

	if t.TaskType != TaskNone {
		c.reduceTasks.Store(t.ReduceIndex, TaskMetadata{
			taskState: TaskInProgress,
			startTime: time.Now(),
		})
	}
	return
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	log.SetOutput(ioutil.Discard)
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		mutex:       sync.Mutex{},
		mapTasks:    sync.Map{},
		reduceTasks: sync.Map{},
	}

	// Your code here.
	c.populateMapTasks()
	c.populateReduceTasks()
	go c.monitorTasks()
	c.server()
	return &c
}

func (c *Coordinator) populateMapTasks() {
	for i := 0; i < len(c.files); i++ {
		c.mapTasks.Store(i, TaskMetadata{taskState: TaskIdle})
	}
}

func (c *Coordinator) populateReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks.Store(i, TaskMetadata{taskState: TaskIdle})
	}
}
