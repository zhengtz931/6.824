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
import "strings"


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
		//log.Printf("state: %v", workargs.State)
		call("Master.CreateWorkTask",&reply, &workargs)
		// fmt.Println("get master return %+v", workargs)

			if workargs.State == MapState {
				//log.Printf("start map metod")
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
	issuccess := true


	file, err := os.Open(work.FileName)
	if err != nil {
		issuccess = false
		log.Printf("open file %v err:%+v", work.FileName, err)
		
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		issuccess = false
		log.Printf("read file err:%+v", err)
	}
	file.Close()

	kvs := work.mapf(work.FileName , string(content))
	intermediate := make([][]KeyValue, work.ReduceNum, work.ReduceNum)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % work.ReduceNum
		intermediate[idx] = append(intermediate[idx], kv)
	}
	for idx := 0; idx < work.ReduceNum; idx++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", work.MapId, idx)
		file, err = os.Create(intermediateFileName)
		data, _ := json.Marshal(intermediate[idx])
		_, err = file.Write(data)
		file.Close()
	}

	// content, err := ioutil.ReadAll(file)
	// if err != nil {
	// 	log.Fatalf("cannot read %v", filename)
	// }
	// file.Close()
	// kva := mapf(filename, string(content))
	// //将 kva 转化为 reduce 的 入参
	// meietedata := make([][]KeyValue, work.ReduceNum)

	// for _, kv := range kva {
	// idx := ihash(kv.Key) % work.ReduceNum
	// meietedata[idx] = append(meietedata[idx], kv)
	// }

	// for i := 0; i < work.ReduceNum; i++ {
	// 	oname := fmt.Sprintf("mr-%d-%d", work.MapId,i)
	// 	ofile, _ := os.Create(oname)
	// 	enc := json.NewEncoder(ofile)
	// 	for _, kv := range meietedata{
	// 	err := enc.Encode(kv)
	// 		if err != nil {
	// 			log.Fatalf("cannot write to file %v by json", filename)
	// 		}
	// 	}
	// }
	args := ReportWorkArgs{
		IsSuccess: issuccess,
		MapId: work.MapId,
		RetryNum: work.RetryNum,
	}
	reply := ExampleReply{}
	//fmt.Println("start report master call")
	call("Master.ReportWorkTask", &args, &reply)


}

func (work *Work) doReduce()  {
	kvsReduce := make(map[string][]string)
	for idx := 0; idx < work.MapNum; idx++ {
		filename := fmt.Sprintf("mr-%d-%d", idx, work.ReduceId)
		file, _ := os.Open(filename)
		content, _ := ioutil.ReadAll(file)
		file.Close()
		kvs := make([]KeyValue, 0)
		err := json.Unmarshal(content, &kvs)
		if err != nil {
			log.Printf("json err: %+v", err)
			break
		}
		for _, kv := range kvs {
			_, ok := kvsReduce[kv.Key]
			if !ok {
				kvsReduce[kv.Key] = make([]string, 0)
			}
			kvsReduce[kv.Key] = append(kvsReduce[kv.Key], kv.Value)
		}
	}
	ReduceResult := make([]string, 0)
	for key, val := range kvsReduce {
		ReduceResult = append(ReduceResult, fmt.Sprintf("%v %v\n", key, work.reducef(key, val)))
	}
	outFileName := fmt.Sprintf("mr-out-%d", work.ReduceId)
	err := ioutil.WriteFile(outFileName, []byte(strings.Join(ReduceResult, "")), 0644)
	if err != nil {
		log.Printf("write to file err: %+v", err)
	}


	// reducef := work.reducef
	// inputvalues := make(map[string][]string)
	// 	for i := 0; i < work.MapNum; i++ {
	// 		name := fmt.Sprintf("mr-%d-%d", i, work.ReduceId)
	// 		//log.Printf("input file name:%v", name)
	// 		file, _ := os.Open(name)
	// 		//log.Printf("input file :%v", file)
	// 		dec := json.NewDecoder(file)
	// 		//log.Printf("dev decoder :%v", dec)
	// 		kvs := make([][]KeyValue,0)
	// 		for {
	// 			var kv[] KeyValue
	// 			if err := dec.Decode(&kv); err != nil {
	// 				log.Printf("decode err: %+v", err)
	// 				break
	// 			}
	// 			//log.Printf("print kv:%+v", kv)
	// 			kvs = append(kvs, kv)
	// 		}
	// 		log.Printf("start write")
	// 		for _ , kv := range kvs {
	// 			for _, kx := range kv {
	// 			 _,ok := inputvalues[kx.Key]
	// 			if !ok {
	// 				inputvalues[kx.Key] = make([]string, 0)
	// 			}
	// 			//log.Printf("kx value is:%+v", kx.Value)
	// 			inputvalues[kx.Key] = append(inputvalues[kx.Key], kx.Value)
	// 			}
	// 		}
	// 	}
	// 	//result := make([]string, 0)
	// oname := fmt.Sprintf("mr-out-%d", work.ReduceId)
	// ofile, _ := os.Create(oname)
	// for inputkey ,  inputvalue:= range inputvalues{
	// 	//result = append(result, "%v %v\n", inputkey, reducef(inputkey,inputvalue))
	// 	//log.Printf("input to file: %v %v\n", inputkey, reducef(inputkey,inputvalue))
	// 	fmt.Fprintf(ofile, "%v %v\n", inputkey, reducef(inputkey,inputvalue))
	// }

	args := ReportWorkArgs{
		IsSuccess: true,
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
