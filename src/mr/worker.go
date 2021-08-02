package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID := os.Getpid()

	log.Printf("Worker is %d", workerID)

	// Your worker implementation here.
	for true {
		reply := RequestTask(workerID)

		log.Printf("Reply from master is to do task %d; Task type is %d \n", reply.TaskNum, reply.TaskType)

		if reply.TaskType == Map {
			err := DoMapTask(mapf, reply.Filename, reply.TaskNum, reply.NReduce)

			if err != nil {
				log.Printf("WORKER: Notifying Master that map task contains ERROR. WorkerID: %d; Task Num: %d; File: %s \n", workerID, reply.TaskNum, reply.Filename)
				NotifyMaster(0, reply.TaskNum, Map, workerID)
			} else {
				log.Printf("WORKER: Notifying Master that map task is completed. WorkerID: %d; Task Num: %d; File: %s \n", workerID, reply.TaskNum, reply.Filename)
				NotifyMaster(1, reply.TaskNum, Map, workerID)
			}
		} else if reply.TaskType == Reduce {
			err := DoReduceTask(reducef, reply.TaskNum)

			if err != nil {
				log.Printf("WORKER: Notifying Master that reduce task contains ERROR. WorkerID: %d; Task Num: %d; \n", workerID, reply.TaskNum)
				NotifyMaster(0, reply.TaskNum, Reduce, workerID)
			} else {
				log.Printf("WORKER: Notifying Master that reduce task is completed. WorkerID: %d; Task Num: %d;\n", workerID, reply.TaskNum)
				NotifyMaster(1, reply.TaskNum, Reduce, workerID)
			}
		}
		time.Sleep(time.Second * 5)
	}
}

// TODO: Use ioutil.TempFile to name the file and then use os.Rename to atomically rename them.
// This is to ensure that nobody observes partially written files in the presence of crashes
func DoMapTask(mapf func(string, string) []KeyValue, filename string, mapTaskNum int, nReduce int) error {
	log.Printf("WORKER: Performing map task on %s; MapTaskNum: %d; \n", filename, mapTaskNum)

	// Open file and apply mapf on the contents.
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		return err;
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return err;
	}
	file.Close()
	kva := mapf(filename, string(content))

	// Write kva to intermediate files
	fileEncoders := make(map[string]*json.Encoder)

	for _, kv := range kva {
		mapTaskNum := strconv.Itoa(mapTaskNum)
		reduceTaskNum := strconv.Itoa(ihash(kv.Key) % nReduce)
		intermediate := "mr-" + mapTaskNum + "-" + reduceTaskNum

		if encoder, exists := fileEncoders[intermediate]; exists {
			encoder.Encode(&kv)
		} else {
			file, _ = os.Create(intermediate)
			enc := json.NewEncoder(file)
			enc.Encode(&kv)
			fileEncoders[intermediate] = enc
		}
	}
	return nil
}

func DoReduceTask(reducef func(string, []string) string, reduceTaskNum int) error {
	log.Printf("WORKER: Performing reduce task. ReduceTaskNum: %d \n", reduceTaskNum)

	intermediate := []KeyValue{}

	pattern := "mr-*-" + strconv.Itoa(reduceTaskNum)
	matches, err := filepath.Glob(pattern)

	if err != nil {
		log.Printf("Cannot find match %v", err)
		return err
	}

	for _, filename := range matches {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reduceTaskNum)
	ofile, _ := os.Create(oname)

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return nil
}

func RequestTask(id int) ReplyTaskArgs {
	args := RequestTaskArgs{
		WorkerID: id,
	}

	reply := ReplyTaskArgs{}

	call("Master.Request", &args, &reply)

	return reply
}


func NotifyMaster(ack int, taskNum int, taskType Task, id int) {
	args := NotifyMasterArgs{
		Ack: ack,
		TaskNum: taskNum,
		TaskType: taskType,
		WorkerID: id,
	}

	reply := NotifyMasterReply{}

	call("Master.Notify", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
