package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type Work struct {
	FileName string
	ReduceNum int
	MapNum int
    State TaskState
	RetryNum int

	MapId int
	ReduceId int


	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	workargs := Work{
		mapf: mapf,
		reducef: reducef,
	}
	reply := ExampleReply{}
	for true {
		call("Master.CreateWorkTask",&reply, &workargs)

			if workargs.State == MapState {
				log.Printf("%+v",workargs)
				workargs.doMap()
			} else if workargs.State == ReduceState {
				workargs.doReduce()
		    } else if workargs.State == WaitState {
				time.Sleep(30 * time.Second)
		    } else if workargs.State == StopState {
			    break
			}

	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}




func (work *Work) doMap()  {
	mapf := work.mapf
	filename := work.FileName
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	//将 kva 转化为 reduce 的 入参
	meietedata := make([][]KeyValue, work.ReduceNum)

	for _, kv := range kva {
	idx := ihash(kv.Key) % work.ReduceNum
	meietedata[idx] = append(meietedata[idx], kv)
	}

	for i := 0; i < work.ReduceNum; i++ {
		oname := fmt.Sprintf("mr-%d-%d", work.MapId,i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range meietedata{
		err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("cannot write to file %v by json", filename)
			}
		}
	}
	args := ReportWorkArgs{
		issuccess: true,
		MapId: work.MapId,
		RetryNum: work.RetryNum,
	}
	reply := ExampleReply{}
	call("Master.ReportWorkTask", &args, &reply)


}

func (work *Work) doReduce()  {
	reducef := work.reducef
	inputvalues := make(map[string][]string)
		for i := 0; i < work.MapNum; i++ {
			name := fmt.Sprintf("mr-%d-%d", i, work.ReduceId)
			file, _ := os.Create(name)
			dec := json.NewDecoder(file)
			kvs := make([]KeyValue,0)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvs = append(kvs, kv)
			}
			for _ , kv := range kvs {
				_,ok := inputvalues[kv.Key]
				if !ok {
					inputvalues[kv.Key] = make([]string, 0)
				}
				inputvalues[kv.Key] = append(inputvalues[kv.Key], kv.Value)
			}
		}
		//result := make([]string, 0)
	oname := fmt.Sprintf("mr-out-%d", work.ReduceId)
	ofile, _ := os.Create(oname)
	for inputkey ,  inputvalue:= range inputvalues{
		//result = append(result, "%v %v\n", inputkey, reducef(inputkey,inputvalue))
		fmt.Fprintf(ofile, "%v %v\n", inputkey, reducef(inputkey,inputvalue))
	}

	args := ReportWorkArgs{
		issuccess: true,
		ReduceId: work.ReduceId,
		RetryNum: work.RetryNum,
	}
	reply := ExampleReply{}
	call("Master.ReportWorkTask", &args, &reply)

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
