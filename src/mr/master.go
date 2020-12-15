package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var taskCount int

type Master struct {
	// Your definitions here.
	BeginTime time.Time
	files     []string
	NReduce   int

	mapWaittingChan    chan TaskInfo
	reduceWaittingChan chan TaskInfo

	mapRunningTaskList    list.List
	reduceRunningTaskList list.List

	mutex  sync.Mutex
	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (m *Master) TaskRequestFromWorker(args *ExampleArgs, reply *TaskInfo) error {

	if len(m.mapWaittingChan) != 0 {
		task := <-m.mapWaittingChan
		task.BeginTime = time.Now()
		m.mapRunningTaskList.PushBack(task)
		fmt.Printf("[Master-Map]Get request from worker. Sending Map Task:%d,FileIndex:%d\n", task.TaskNo, task.FileIndex)
		*reply = task
		return nil

	} else if len(m.reduceWaittingChan) != 0 && m.mapRunningTaskList.Len() == 0 {
		task := <-m.reduceWaittingChan
		task.BeginTime = time.Now()
		m.reduceRunningTaskList.PushBack(task)
		fmt.Printf("[Master-Reduce]Get request from worker. Send Reduce Task:%d,PartIndex:%d\n", task.TaskNo, task.PartIndex)
		*reply = task
		return nil
	} else if m.mapRunningTaskList.Len() > 0 || m.reduceRunningTaskList.Len() > 0 {
		reply.State = TaskRunning
		if m.mapRunningTaskList.Len() > 0 {
			fmt.Printf("[Master-Info]First task in map waitting list %v\n", m.mapRunningTaskList.Front().Value.(TaskInfo))
		}
		if m.reduceRunningTaskList.Len() > 0 {
			fmt.Printf("[Master-Info]First task in reduce waitting list %v\n", m.reduceRunningTaskList.Front().Value.(TaskInfo))
		}
		return nil
	}

	reply.State = TaskDone
	m.isDone = true
	return nil
}

func (m *Master) TaskResponseFromWorker(args *TaskInfo, reply *ExampleReply) error {

	switch args.TaskType {
	case MapTask:
		m.mutex.Lock()
		defer m.mutex.Unlock()

		for begin := m.mapRunningTaskList.Front(); begin != nil; begin = begin.Next() {
			if begin.Value.(TaskInfo).TaskNo == args.TaskNo {
				m.mapRunningTaskList.Remove(begin)
				fmt.Printf("[Master-Map]TaskNo:%d FileIndex:%d is finished.\n", args.TaskNo, args.FileIndex)
				break
			}

		}
		break
	case ReduceTask:
		m.mutex.Lock()
		defer m.mutex.Unlock()

		for begin := m.reduceRunningTaskList.Front(); begin != nil; begin = begin.Next() {
			if begin.Value.(TaskInfo).TaskNo == args.TaskNo {
				m.reduceRunningTaskList.Remove(begin)
				fmt.Printf("[Master-Reduce]TaskNo:%d PartIndex:%d is finished.\n", args.TaskNo, args.PartIndex)
				break
			}
		}
		break
	default:
		break
	}
	return nil
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	return m.isDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.

func (m *Master) timeOutCheck(period int, timeOutSeconds int) {
	for {
		m.mutex.Lock()
		if m.mapRunningTaskList.Len() != 0 {
			for begin := m.mapRunningTaskList.Front(); begin != nil; {
				mapTask := begin.Value.(TaskInfo)
				if time.Now().Sub(mapTask.BeginTime) > time.Duration(timeOutSeconds)*time.Second {
					m.mapWaittingChan <- mapTask
					next := begin.Next()
					m.mapRunningTaskList.Remove(begin)
					begin = next
				} else {
					begin = begin.Next()
				}
			}
		}
		if m.reduceRunningTaskList.Len() != 0 {
			for begin := m.reduceRunningTaskList.Front(); begin != nil; {
				reduceTask := begin.Value.(TaskInfo)
				if time.Now().Sub(reduceTask.BeginTime) > time.Duration(timeOutSeconds)*time.Second {
					m.reduceWaittingChan <- reduceTask
					next := begin.Next()
					m.reduceRunningTaskList.Remove(begin)
					begin = next
				} else {
					begin = begin.Next()
				}
			}
		}
		m.mutex.Unlock()
		time.Sleep(time.Duration(period) * time.Second)
	}

}

func MakeMaster(files []string, NReduce int) *Master {
	mapTaskChan := make(chan TaskInfo, 100)
	for index, name := range files {
		newTask := TaskInfo{
			TaskNo:    taskCount,
			State:     TaskWaitting,
			FileIndex: index,
			TaskType:  MapTask,
			FileName:  name,
			NReduce:   NReduce,
			NFiles:    len(files),
		}

		taskCount++
		fmt.Printf("Load map task into mapChannel.Task:%v\n", newTask)
		mapTaskChan <- newTask
	}

	reduceTaskChan := make(chan TaskInfo, 100)
	for index := 0; index < NReduce; index++ {
		newTask := TaskInfo{
			TaskNo:    taskCount,
			State:     TaskWaitting,
			PartIndex: index,
			TaskType:  ReduceTask,
			NReduce:   NReduce,
			NFiles:    len(files),
		}
		taskCount++
		reduceTaskChan <- newTask
	}

	m := Master{
		BeginTime:          time.Now(),
		files:              files,
		NReduce:            NReduce,
		mapWaittingChan:    mapTaskChan,
		reduceWaittingChan: reduceTaskChan,
		isDone:             false,
	}

	//环境初始化
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Println("fail to create dir mr-tmp.")
		}
	}

	//进程监控是否存在任务过期的情况
	//2秒检查周期，5秒超时阈值
	go m.timeOutCheck(2, 5)

	m.server()

	return &m

}
