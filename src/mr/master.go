package mr

import (
	//"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

type TaskState int

const (
	MapState    TaskState = 0
	ReduceState TaskState = 1
	StopState   TaskState = 2
	WaitState   TaskState = 3
)

type Task struct {
	process bool
	finsh   bool
}



type Master struct {
	// Your definitions here.
	FileNames []string // filenames
	Phase TaskState // master status
	NMap  int // Map number
	NReduce int // too

	MapTask []Task
	ReduceTask  []Task

	TaskRetryNum []int
	ReduceDone bool
	MapDone bool
	Mutex sync.Mutex
}



// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) TimeOut() {
    for{
		m.Mutex.Lock()
		if(m.MapDone && m.ReduceDone) {
			m.Mutex.Unlock()
			break
		}
		if(!m.MapDone) {
			for i := 0; i < m.NMap; i++ {
				if(!m.MapTask[i].finsh) {
					m.MapTask[i].process = false
				}
			}
		}

		if(!m.MapDone) {
			for i := 0; i < m.NReduce; i++ {
				if(!m.ReduceTask[i].finsh) {
					m.ReduceTask[i].process = false
				}
			}
		}
		m.Mutex.Unlock()
		time.Sleep(2000 * time.Millisecond)

	}

}

func (m *Master) CreateWorkTask(reply *ExampleReply, argwork *Work) error {
	//log.Printf("work create: argwork:%+v", argwork)
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if !m.MapDone {
		// log.Printf("%+v",argwork)
		// log.Printf("master value : %+v", m)
		for  i := 0; i < len(m.FileNames); i++ {

			// log.Printf("retrynum %d ,i:%d",m.TaskRetryNum, i)
			if !m.MapTask[i].finsh && !m.MapTask[i].process  {
				m.TaskRetryNum[i]++
				argwork.FileName = m.FileNames[i]
				argwork.MapNum = m.NMap
				argwork.ReduceNum = m.NReduce
				argwork.State = MapState
				argwork.MapId = i
				argwork.RetryNum = m.TaskRetryNum[i]
				m.MapTask[i].process = true
				//log.Printf("%+v",argwork)
				return nil
			}
		}
		m.Phase = WaitState
		return nil
	}
	if m.ReduceDone == false {
		//log.Printf("start reduce work")
		for i := 0; i < m.NReduce; i++ {
            if !m.ReduceTask[i].finsh && !m.ReduceTask[i].process {
			m.TaskRetryNum[i]++
			argwork.ReduceId = i
			argwork.State = ReduceState
			argwork.MapNum = m.NMap
			argwork.RetryNum = m.TaskRetryNum[i]
			return nil
			}
			}
		m.Phase = WaitState
		return nil
		}
	//log.Printf("m state: %v", m.Phase)
	if m.Phase == StopState {
       argwork.State = StopState
	}
	return nil

	}

func (m *Master) ReportWorkTask(args *ReportWorkArgs, reply *ExampleReply) error{
	//fmt.Println("work report")
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	//log.Printf("report args: %+v", args)
	//log.Printf("args.RetryNum %+v, m.TaskRetryNum[args.MapId] %+v args.IsSuccess %+v, m.Phase %+v", args.RetryNum, m.TaskRetryNum[args.MapId], args.IsSuccess,m.Phase)

	if  args.IsSuccess {
		if !m.MapDone {
			if args.RetryNum == m.TaskRetryNum[args.MapId] {
				m.MapTask[args.MapId].process = false
				m.MapTask[args.MapId].finsh = true
			}
		}else {
			if args.RetryNum == m.TaskRetryNum[args.ReduceId] {
				m.ReduceTask[args.ReduceId].process = false
				m.ReduceTask[args.ReduceId].finsh = true
			}
		}
	}else {
		if !m.MapDone {
			if m.MapTask[args.MapId].finsh == false {
				m.MapTask[args.MapId].process = false
			}
		}else {
			if m.ReduceTask[args.ReduceId].finsh == false {
				m.ReduceTask[args.ReduceId].process = false
			}
		}
	}
	//log.Printf("mapid %v, task state %v", args.MapId,m.MapTask[args.MapId].finsh)
	 MapIsDone := true
	for i := 0; i < m.NMap; i++ {
	if m.MapTask[i].finsh == true {
		continue
	}else {
		MapIsDone = false
	}
	}
	if(MapIsDone ) {
		//fmt.Println("map is done")
		m.MapDone = true
		m.Phase = ReduceState
	}
	ReduceIsDone := true
	for i := 0; i < m.NReduce; i++ {
		if m.ReduceTask[i].finsh == true{
			continue
		}else {
			ReduceIsDone = false
		}
	}
	if(ReduceIsDone) {
		//fmt.Println("reduce is done")
		m.ReduceDone = true
		m.Phase = StopState

	}
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
	ret := false

	// Your code here.
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if m.MapDone && m.ReduceDone {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		FileNames: files,
		NMap : len(files),
		NReduce : nReduce,
		MapTask : make([]Task, len(files)),
		ReduceTask : make([]Task, nReduce),

		TaskRetryNum : make([]int, nReduce ),
		MapDone: false,
		ReduceDone: false,
	}

	// Your code here.
	go m.TimeOut()


	m.server()
	return &m
}
