package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"sort"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetOutput(ioutil.Discard)

	cwd, _ := os.Getwd()
	// Your worker implementation here.
	log.Printf("Worker started. CWD: %v\n", cwd)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		if task, err := getTask(); err == nil {
      if task.TaskType == TaskNone {
        continue
      }
      switch task.TaskType {
      case TaskMap:
        doMap(task, mapf, cwd)
      case TaskReduce:
        doReduce(task, reducef, cwd)
      }
      if task.TaskType != TaskNone && task.TaskType != TaskWait {
        informTaskCompleted(&task)
      }
		}
	}
}

func informTaskCompleted(task *Task) error {

	// declare an argument structure.
	args := *task

	// declare a reply structure.
	reply := EmptyReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.MarkTaskCompleted", &args, &reply)
	if ok {
		// reply.Y should be 100.
		return nil
	} else {
		return fmt.Errorf("error occurred")
	}
}

func doMap(task Task, mapf func(string, string) []KeyValue, cwd string) {
	content, err := os.ReadFile(task.FileName)
	if err != nil {
		log.Panicf("cannot read file. Details: %v. Task %v", err, task)
	}

	kva := mapf(task.FileName, string(content))
	var intermediate [][]KeyValue
	for i := 0; i < task.ReduceNumber; i++ {
		intermediate = append(intermediate, []KeyValue{})
	}

	for _, kv := range kva {
		index := ihash(kv.Key) % task.ReduceNumber

		intermediate[index] = append(intermediate[index], kv)
	}

	writeJson(task.MapIndex, intermediate, cwd)
}

func writeJson(mapIndex int, intermediate [][]KeyValue, cwd string) {
	for i := 0; i < len(intermediate); i++ {
		ik := intermediate[i]
		sort.Slice(ik, func(i, j int) bool { return ik[i].Key < ik[j].Key })
		fileName := fmt.Sprintf("mr-%v-%v", mapIndex, i)
		filePath := path.Join(cwd, fileName)
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("cannot create file. Details: %v", err)
		}
		encoder := json.NewEncoder(file)
		encoder.Encode(ik)
	}
}

func doReduce(task Task, reducef func(string, []string) string, cwd string) {
	keyValues := make(map[string][]string)

	for i := 0; i < task.MapNumber; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", i, task.ReduceIndex)
		filePath := path.Join(cwd, fileName)
		loadJson(filePath, keyValues)
	}

	result := make([]KeyValue, 0)
	for k, v := range keyValues {
		acc := reducef(k, v)
		result = append(result, KeyValue{Key: k, Value: acc})
	}
	outFileName := fmt.Sprintf("mr-out-%v", task.ReduceIndex)

	writeOutFile(outFileName, result)
}

func writeOutFile(outFileName string, result []KeyValue) {
	cwd, _ := os.Getwd()
	file, err := os.CreateTemp(cwd, "mr-tmp-*")
	if err != nil {
		log.Printf("Error creating tmp file: %v\n", err)
	}
	tmpFileName := file.Name()
	for _, kv := range result {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
	err = os.Rename(tmpFileName, outFileName)
	if err != nil {
		log.Printf("Error: %v\n", err)
	}
}

func loadJson(fileName string, keyValues map[string][]string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open file. Details: %v", err)
	}
	decoder := json.NewDecoder(file)
	var intermediate []KeyValue
	if err := decoder.Decode(&intermediate); err != nil {
		log.Fatalf("cannot decode file. Details: %v", err)
	}
	for _, kvs := range intermediate {
		keyValues[kvs.Key] = append(keyValues[kvs.Key], kvs.Value)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func getTask() (Task, error) {

	// declare an argument structure.
	args := EmptyArgs{}

	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		return reply, nil
	} else {
		return Task{TaskType: TaskNone}, nil
	}
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

	log.Println(err)
	return false
}
