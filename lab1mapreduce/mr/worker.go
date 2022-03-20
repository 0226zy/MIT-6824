package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
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

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string
type Work struct {
	mapf    MapFunc
	reducef ReduceFunc
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *Work) process() {
	for {
		task := w.getTask()
		if nil == task {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch task.TaskType {
		case "Map":
			w.doMapTask(task)
		case "Reduce":
			w.doReduceTask(task)
		case "Wait":
			time.Sleep(100 * time.Millisecond)
			continue
		case "Finish":
			log.Println("[Work] reviced cmd:Finish,all task done, exit")
			return
		default:
			log.Fatalf("[Work] reviced unkown cmd:%s, exit\n", task.TaskType)
		}
	}
}

func (w *Work) getTask() *MRTask {
	req := Req{}
	reply := Reply{}
	if ok := call("Coordinator.GetTask", &req, &reply); !ok {
		return nil
	}
	return &reply.Task
}

func (w *Work) reportTask(task *MRTask) {
	req := Req{Task: *task}
	reply := Reply{}
	retryCnt := 3
	for i := 0; i < retryCnt; i++ {
		if ok := call("Coordinator.ReportTask", &req, &reply); ok {
			break
		}
	}
}

func (w *Work) doMapTask(task *MRTask) {
	// 检查任务是否已经处理完
	intermediates := make([][]KeyValue, task.ReduceTaskNum)
	for i := 0; i < len(intermediates); i++ {
		intermediates[i] = []KeyValue{}
	}
	file, err := os.Open(task.FilePath)
	if err != nil {
		log.Printf("[Work] open %s fiailed,err %v\n", task.FilePath, err)
		return
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("[Work] ReadAll from %s fiailed,err %v\n", task.FilePath, err)
		return
	}

	rets := w.mapf(task.FilePath, string(content))
	KeyNum := len(rets)
	for _, kv := range rets {
		idx := ihash(kv.Key) % task.ReduceTaskNum
		intermediates[idx] = append(intermediates[idx], kv)
	}
	for i := 0; i < task.ReduceTaskNum; i++ {

		if 0 == len(intermediates[i]) {
			continue
		}
		w.emitMapTask(task.TaskID, i, intermediates[i])
	}
	task.TaskStatis.KeyNum = KeyNum
	task.TaskStatis.EndTime = time.Now()
	task.TaskStatis.Cost = task.TaskStatis.EndTime.Unix() - task.TaskStatis.BeginTime.Unix()
	w.reportTask(task)
}

func (w *Work) doReduceTask(task *MRTask) {
	// 检查任务是否已经处理完
	intermediates := make([]KeyValue, 0)
	for i := 0; i < task.MapTaskNum; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		kvs := w.readMapFile(filename)
		intermediates = append(intermediates, kvs...)
	}
	sort.Sort(ByKey(intermediates))

	outputKvs := make([]KeyValue, 0)
	for i := 0; i < len(intermediates); {
		j := i + 1
		for j < len(intermediates) && intermediates[i].Key == intermediates[j].Key {
			j++
		}

		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := w.reducef(intermediates[i].Key, values)
		outputKvs = append(outputKvs, KeyValue{intermediates[i].Key, output})
		i = j
	}
	w.emitReduceTask(task.TaskID, outputKvs)
	task.TaskStatis.KeyNum = len(intermediates)
	task.TaskStatis.EndTime = time.Now()
	task.TaskStatis.Cost = task.TaskStatis.EndTime.Unix() - task.TaskStatis.BeginTime.Unix()
	w.reportTask(task)
}

func (w *Work) readMapFile(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("[Work] reduce task open %s failed,err:%v\n", filename, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	ret := make([]KeyValue, 0)
	for {
		var kv KeyValue
		if err := decoder.Decode(&kv); err != nil {
			break
		}
		ret = append(ret, kv)
	}
	return ret
}

func (w *Work) emitMapTask(taskID, reduceID int, kvs []KeyValue) {
	filename := fmt.Sprintf("mr-%d-%d", taskID, reduceID)
	tmpFile, err := ioutil.TempFile("./", "tmp_"+filename+"_")
	if err != nil {
		log.Printf("[Work] map task[%d] emit reduce[%d] create TempFile  failed,err:%v\n",
			taskID, reduceID, err)
	}

	encoder := json.NewEncoder(tmpFile)
	for _, kv := range kvs {
		if err := encoder.Encode(&kv); err != nil {
			log.Printf("[Work] map task[%d] emit reduce[%d] json encode:k-%s v-%s failed,err:%v\n",
				taskID, reduceID, kv.Key, kv.Value, err)
		}
	}
	tmpFile.Close()
	os.Rename(tmpFile.Name(), filename)
}

func (w *Work) emitReduceTask(reduceID int, contents []KeyValue) {
	filename := fmt.Sprintf("mr-out-%d", reduceID)
	tmpFile, err := ioutil.TempFile("./", "tmp_"+filename+"_")
	if err != nil {
		log.Printf("[Work] reduce task[%d] emit,cerate TempFile failed,err:%v\n", reduceID, err)
	}
	for _, kv := range contents {
		fmt.Fprintf(tmpFile, "%v %v\n", kv.Key, kv.Value)
	}
	tmpFile.Close()
	os.Rename(tmpFile.Name(), filename)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf MapFunc,
	reducef ReduceFunc) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// callExample()
	work := &Work{
		mapf:    mapf,
		reducef: reducef,
	}
	fmt.Println("[Worker] Begin ...")
	work.process()
	fmt.Println("[Worker] End ...")
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func callExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("[Work] dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
