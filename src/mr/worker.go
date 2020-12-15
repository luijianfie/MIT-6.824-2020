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

type ByKey []KeyValue

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

	loop := 1

	for {

		if loop == 1 {

			task := RequestTaskFromServer()
			//fmt.Printf("[Info]Info recieve from master:%v\n", task)
			switch task.State {
			case TaskUnknown:
				fmt.Printf("Can't connect master. Probably task is done.\n")
				loop = 0
				break
			case TaskWaitting:
				switch task.TaskType {
				case MapTask:
					fmt.Printf("[Map]Recieve maptask from master,TaskNo:%d,FileIndex:%d\n", task.TaskNo, task.FileIndex)
					task.BeginTime = time.Now()
					MapTaskProcess(mapf, *task)
					break
				case ReduceTask:
					fmt.Printf("[Reduce]Recieve reducetask from master,TaskNo:%d,PartIndex:%d\n", task.TaskNo, task.PartIndex)
					task.BeginTime = time.Now()
					ReduceTaskProcess(reducef, *task)
					break
				}
				break
			case TaskRunning:
				fmt.Printf("[Info]No available task in waitting list.Sleep 5 seconds.\n")
				time.Sleep(time.Duration(5) * time.Second)
				break
			case TaskDone:
				fmt.Printf("[Info]Task is finished.\n")
				loop = 0
				break
			default:
				panic("[Info]Unexpected result.\n")
			}
		} else {
			break
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func ResponseToServer(task TaskInfo) {
	args := task
	reply := ExampleReply{}
	// fmt.Printf("Task %v is finished. Send result to master.\n",task)
	call("Master.TaskResponseFromWorker", &args, &reply)
}

func RequestTaskFromServer() *TaskInfo {
	args := ExampleArgs{}

	reply := TaskInfo{}

	call("Master.TaskRequestFromWorker", &args, &reply)
	// fmt.Printf("Task %v\n",reply)
	return &reply
}

func MapTaskProcess(mapf func(string, string) []KeyValue, task TaskInfo) {
	fmt.Printf("[Map]task begins, FileName:%s,taskNo:%d\n", task.FileName, task.TaskNo)
	intermediate := []KeyValue{}

	file, err := os.Open(task.FileName)
	if err != nil {
		fmt.Printf("can't open %s", task.FileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("can't read %s", task.FileName)
	}

	file.Close()
	keys := mapf(task.FileName, string(content))
	intermediate = append(intermediate, keys...)

	nReduce := task.NReduce
	filesList := make([]*os.File, nReduce)
	fileEncodeList := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		filesList[i], err = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		if err != nil {
			panic("Failed to create tmp file")
		}
		fileEncodeList[i] = json.NewEncoder(filesList[i])
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce
		err := fileEncodeList[index].Encode(&kv)
		if err != nil {
			fmt.Printf("File %v key %v Value %v Error:%v\n", task.FileName, kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}

	filePrefix := "mr-tmp/mr-" + strconv.Itoa(task.FileIndex) + "-"

	for index, file := range filesList {
		realName := filePrefix + strconv.Itoa(index)
		tmpName := filepath.Join(file.Name())
		os.Rename(tmpName, realName)
		file.Close()
	}

	// fmt.Printf("Map task finished. Send response to master.\n")
	fmt.Printf("[Map]TaskNo:%d fileIndex:%d is finished.\n", task.TaskNo, task.FileIndex)
	ResponseToServer(task)

}

func ReduceTaskProcess(reducef func(string, []string) string, task TaskInfo) {
	fmt.Printf("[Reduce]task begins, partIndex:%d,taskNo:%d\n", task.PartIndex, task.TaskNo)

	FileName := "mr-out-" + strconv.Itoa(task.PartIndex)
	filePrefix := "mr-tmp/mr-"
	fileSubfix := "-" + strconv.Itoa(task.PartIndex)

	intermediate := []KeyValue{}

	for i := 0; i < task.NFiles; i++ {
		file, err := os.Open(filePrefix + strconv.Itoa(i) + fileSubfix)
		if err != nil {
			fmt.Printf("can't find %s", file.Name())
			continue
		}
		fileDecoder := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := fileDecoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	tmpFile, err := ioutil.TempFile("mr-tmp", "mr-tmp-*")
	if err != nil {
		panic("Fail to create tmp file")
	}

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
	os.Rename(filepath.Join(tmpFile.Name()), FileName)
	tmpFile.Close()

	// fmt.Printf("Task is done.Send response to master.")
	fmt.Printf("[Reduce]TaskNo:%d PartIndex:%d is finished.\n", task.TaskNo, task.PartIndex)
	ResponseToServer(task)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	fmt.Println(err)
	return false
}
