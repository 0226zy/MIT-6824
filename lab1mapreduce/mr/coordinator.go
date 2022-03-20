package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	files                []string // input files
	nReduce              int
	mapTaskInReady       map[int]MRTask
	mapTaskInProgress    map[int]MRTask
	reduceTaskInReady    map[int]MRTask
	reduceTaskInProgress map[int]MRTask
	mapTasksMutex        *sync.Mutex
	reduceTasksMutex     *sync.Mutex
	ticker               *time.Ticker
}

type MRTask struct {
	FilePath      string
	TaskType      string
	TaskID        int
	DeadLine      time.Time
	MapTaskNum    int
	ReduceTaskNum int
	TaskStatis    Statis
}

type Statis struct {
	BeginTime time.Time
	EndTime   time.Time
	Cost      int64
	KeyNum    int
}

func (s Statis) String() string {
	return fmt.Sprintf("handle KeyNum:%d cost:%d second", s.KeyNum, s.Cost)
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
	log.Printf("[Coordinator] Begin...\n")
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	if c.mapDone() && c.reduceDone() {
		c.ticker.Stop()
		ret = true
		log.Println("[Coordinator] End...")
	}
	return ret
}

func (c *Coordinator) ReportTask(req *Req, reply *Reply) (err error) {
	task := req.Task
	reply.Task = task
	switch task.TaskType {
	case "Map":
		c.mapTasksMutex.Lock()
		defer c.mapTasksMutex.Unlock()
		delete(c.mapTaskInProgress, task.TaskID)
		log.Printf("[Coordinator] map task[%d] done,statis %v\n", task.TaskID, task.TaskStatis)
	case "Reduce":
		c.reduceTasksMutex.Lock()
		defer c.reduceTasksMutex.Unlock()
		delete(c.reduceTaskInProgress, task.TaskID)
		log.Printf("[Coordinator] reduce task[%d] done,statis %v\n", task.TaskID, task.TaskStatis)
	default:
		log.Println("report unkown task type ")
	}
	return nil
}

// GetTask 分配任务
func (c *Coordinator) GetTask(req *Req, reply *Reply) (err error) {
	c.checkTask()

	err = nil
	task := MRTask{}
	task, err = c.getMapTask()
	if err == nil {
		reply.Task = task
		return
	}
	err = nil

	// map 已经分配完
	// 检查 map 任务是否处理完
	if false == c.mapDone() {
		task.TaskType = "Wait"
		reply.Task = task
		return
	}

	// 分配 reduce 任务
	task, err = c.getReduceTask()
	if err == nil {
		reply.Task = task
		return
	}

	err = nil
	if false == c.reduceDone() {
		task.TaskType = "Wait"
		reply.Task = task
		return
	}
	// 所有的任务都分配完成
	task.TaskType = "Finish"
	reply.Task = task
	return
}

func (c *Coordinator) mapDone() bool {
	ret := false
	c.mapTasksMutex.Lock()
	defer c.mapTasksMutex.Unlock()
	if 0 == len(c.mapTaskInReady) && 0 == len(c.mapTaskInProgress) {
		ret = true
	}
	return ret
}

func (c *Coordinator) reduceDone() bool {
	ret := false
	c.reduceTasksMutex.Lock()
	defer c.reduceTasksMutex.Unlock()
	if 0 == len(c.reduceTaskInReady) && 0 == len(c.reduceTaskInProgress) {
		ret = true
	}
	return ret
}

func (c *Coordinator) getMapTask() (MRTask, error) {
	c.mapTasksMutex.Lock()
	defer c.mapTasksMutex.Unlock()
	if 0 == len(c.mapTaskInReady) {
		return MRTask{}, errors.New("map task queue empty")
	}

	for _, task := range c.mapTaskInReady {
		delete(c.mapTaskInReady, task.TaskID)
		task.DeadLine = time.Now().Add(20 * time.Second)
		task.TaskStatis = Statis{
			BeginTime: time.Now(),
			EndTime:   time.Now(),
			Cost:      0,
		}
		c.mapTaskInProgress[task.TaskID] = task
		return task, nil
	}
	return MRTask{}, nil
}

func (c *Coordinator) getReduceTask() (MRTask, error) {
	c.reduceTasksMutex.Lock()
	defer c.reduceTasksMutex.Unlock()
	if 0 == len(c.reduceTaskInReady) {
		return MRTask{}, errors.New("map task queue empty")
	}

	for _, task := range c.reduceTaskInReady {
		delete(c.reduceTaskInReady, task.TaskID)
		task.DeadLine = time.Now().Add(20 * time.Second)
		task.TaskStatis = Statis{
			BeginTime: time.Now(),
			EndTime:   time.Now(),
			Cost:      0,
		}
		c.reduceTaskInProgress[task.TaskID] = task
		return task, nil
	}
	return MRTask{}, nil
}

func (c *Coordinator) checkTask() {

	mapTimeout := func() {
		c.mapTasksMutex.Lock()
		defer c.mapTasksMutex.Unlock()
		for _, task := range c.mapTaskInProgress {
			if task.DeadLine.Before(time.Now()) {
				log.Printf("[Coordinator] map task %d timeout\n", task.TaskID)
				c.mapTaskInReady[task.TaskID] = task
				delete(c.mapTaskInProgress, task.TaskID)
			}
		}
	}

	reduceTimeout := func() {
		c.reduceTasksMutex.Lock()
		defer c.reduceTasksMutex.Unlock()
		for _, task := range c.reduceTaskInProgress {
			if task.DeadLine.Before(time.Now()) {
				log.Printf("[Coordinator] reduce task %d timeout\n", task.TaskID)
				delete(c.reduceTaskInProgress, task.TaskID)
				c.reduceTaskInReady[task.TaskID] = task
			}
		}
	}
	mapTimeout()
	reduceTimeout()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.mapTaskInReady = make(map[int]MRTask)
	c.reduceTaskInReady = make(map[int]MRTask)
	c.mapTaskInProgress = make(map[int]MRTask)
	c.reduceTaskInProgress = make(map[int]MRTask)
	c.mapTasksMutex = &sync.Mutex{}
	c.reduceTasksMutex = &sync.Mutex{}

	for i, file := range files {
		task := MRTask{
			FilePath:      file,
			TaskID:        i,
			DeadLine:      time.Now(),
			TaskType:      "Map",
			MapTaskNum:    len(files),
			ReduceTaskNum: nReduce,
			TaskStatis:    Statis{},
		}
		c.mapTaskInReady[task.TaskID] = task
	}

	for i := 0; i < nReduce; i++ {
		task := MRTask{
			FilePath:      "",
			TaskID:        i,
			TaskType:      "Reduce",
			DeadLine:      time.Now(),
			MapTaskNum:    len(files),
			ReduceTaskNum: nReduce,
			TaskStatis:    Statis{},
		}
		c.reduceTaskInReady[task.TaskID] = task
	}

	c.ticker = time.NewTicker(40 * time.Second)
	go func(ticker *time.Ticker) {
		for {
			<-ticker.C
			c.checkTask()
		}
	}(c.ticker)

	c.server()
	return &c
}
