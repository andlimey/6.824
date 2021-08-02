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

type TaskStatus int

const (
	NotStarted TaskStatus = iota
	InProgress
	Completed
)

type MapTask struct {
	status   TaskStatus
	filename string
	startedAt time.Time
	workerID int
}

type ReduceTask struct {
	status TaskStatus
	startedAt time.Time
	workerID int
}

type Master struct {
	// Your definitions here.
	mapTasks    map[int]*MapTask
	reduceTasks map[int]*ReduceTask

	allMapTasksCompleted    bool
	allReduceTasksCompleted bool

	nMap    int
	nReduce int
	timeout time.Duration 

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Request(args *RequestTaskArgs, reply *ReplyTaskArgs) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.allMapTasksCompleted { // Give reduce task
		log.Println("Map tasks completed. Assigning reduce tasks")
		return m.AssignReduceTask(args, reply)
	} else { // Give map task
		log.Println("Assigning map tasks")
		return m.AssignMapTask(args, reply)
	}
}

func (m *Master) AssignMapTask(req *RequestTaskArgs, reply *ReplyTaskArgs) error {
	for k, v := range m.mapTasks {
		// Check for tasks that aren't assigned yet.
		if v.status == NotStarted {
			reply.TaskType = Map
			reply.NReduce = m.nReduce
			reply.Filename = v.filename
			reply.TaskNum = k
			v.status = InProgress
			v.startedAt = time.Now()
			v.workerID = req.WorkerID
			log.Printf("Assigning new map task to worker ID %d; Task Num is %d; Filename is %s", req.WorkerID, reply.TaskNum, reply.Filename)
			break;
		}

		// If all tasks are in-progress already, check if there are in-progress tasks that have exceeded some time.
		if v.status == InProgress {
			timeTaken := time.Now().Sub(v.startedAt)

			// Reassign this task	
			if timeTaken > m.timeout {
				reply.TaskType = Map
				reply.NReduce = m.nReduce
				reply.Filename = v.filename
				reply.TaskNum = k
				v.workerID = req.WorkerID
				v.startedAt = time.Now()
				log.Printf("Assigning in-progress map task to worker ID %d; Task Num is %d; Filename is %s", req.WorkerID, reply.TaskNum, reply.Filename)
				break;
			}
		}
	}

	return nil
}

func (m *Master) AssignReduceTask(req *RequestTaskArgs, reply *ReplyTaskArgs) error {
	for k, v := range m.reduceTasks {
		// Check for tasks that aren't assigned yet.
		if v.status == NotStarted {
			reply.TaskType = Reduce
			reply.NReduce = m.nReduce
			reply.TaskNum = k
			v.status = InProgress
			v.startedAt = time.Now()
			v.workerID = req.WorkerID
			log.Printf("Assigning new reduce task to worker ID %d; Task Num is %d", req.WorkerID, reply.TaskNum)
			break;
		}

		// If all tasks are in-progress already, check if there are in-progress tasks that have exceeded some time.
		if v.status == InProgress {
			timeTaken := time.Now().Sub(v.startedAt)

			// Reassign this task	
			if timeTaken > m.timeout {
				reply.TaskType = Reduce
				reply.NReduce = m.nReduce
				reply.TaskNum = k
				v.startedAt = time.Now()
				v.workerID = req.WorkerID
				log.Printf("Assigning in-progress reduce task to worker ID %d; Task Num is %d", req.WorkerID, reply.TaskNum)
				break;
			}
		}
	}

	return nil
}

func (m *Master) Notify(args *NotifyMasterArgs, reply *NotifyMasterReply) error {
	log.Printf("Retrieved notification from worker ID: %d; Task is %d; Task Num is %d", args.WorkerID, args.TaskType, args.TaskNum)

	m.mu.Lock()
	defer m.mu.Unlock()

	if args.TaskType == Map {
		// Checks if the worker assigned is the same as the one notifying master
		if m.mapTasks[args.TaskNum].workerID == args.WorkerID {
			if args.Ack == 1 {
				m.mapTasks[args.TaskNum].status = Completed
				log.Printf("Worker %d completed the map task successfully!", args.WorkerID)
			} else if args.Ack == 0 {
				log.Printf("Worker %d wasn't able to complete the map task", args.WorkerID)
				m.mapTasks[args.TaskNum].status = NotStarted
			}
		} else {
			log.Printf("Worker ID %d isn't the same as the worker ID that Master assigned the map task to %d", args.WorkerID, m.mapTasks[args.TaskNum].workerID)
		}

		m.allMapTasksCompleted = true
		for _, v := range m.mapTasks {
			if v.status != Completed {
				m.allMapTasksCompleted = false
				log.Printf("Map tasks not yet completed.")
				break;
			}
		}

		if m.allMapTasksCompleted {
			log.Printf("All map tasks completed")
		} 
	} else if args.TaskType == Reduce {
		// Checks if the worker assigned is the same as the one notifying master
		if m.reduceTasks[args.TaskNum].workerID == args.WorkerID {
			if args.Ack == 1 {
				log.Printf("Worker %d completed the reduce task successfully!", args.WorkerID)
				m.reduceTasks[args.TaskNum].status = Completed
			} else if args.Ack == 0 {
				log.Printf("Worker %d wasn't able to complete the reduce task", args.WorkerID)
				m.reduceTasks[args.TaskNum].status = NotStarted
			}
		} else {
			log.Printf("Worker ID %d isn't the same as the worker ID that Master assigned the reduce task to %d", args.WorkerID, m.mapTasks[args.TaskNum].workerID)
		}

		m.allReduceTasksCompleted = true
		for _, v := range m.reduceTasks {
			if v.status != Completed {
				m.allReduceTasksCompleted = false
				log.Printf("Reduce tasks not yet completed.")
				break;
			}
		}	

		if m.allReduceTasksCompleted {
			log.Printf("All reduce tasks completed")
		}
	}

	return nil;
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.allMapTasksCompleted && m.allReduceTasksCompleted
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		mapTasks:    make(map[int]*MapTask),
		reduceTasks: make(map[int]*ReduceTask),

		allMapTasksCompleted:    false,
		allReduceTasksCompleted: false,

		nMap:    len(files),
		nReduce: nReduce,

		timeout: time.Second * 10,
	}

	for k, v := range files {
		mapTask := &MapTask{
			status:   NotStarted,
			filename: v,
		}
		m.mapTasks[k+1] = mapTask
	}

	for i := 0; i < nReduce; i++ {
		reduceTask := &ReduceTask{
			status: NotStarted,
		}
		m.reduceTasks[i] = reduceTask
	}

	m.server()
	return &m
}
