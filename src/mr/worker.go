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
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// map工作,需要返回中间文件
func mapwork(mapf func(string, string) []KeyValue, reply WorkerReply) (filenames []string) {
	//应写入mr-X-Y中间文件中
	intermediate := []KeyValue{}
	for _, filename := range reply.Task.Filename {
		//打开文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		//读取文件内容
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		//关闭文件
		file.Close()

		//将键值对写入中间文件mr-X-Y
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	// fmt.Printf("完成的map任务：%v\n",reply.Task)
	mod := reply.NReduce
	for i := 0; i < mod; i++ {
		//mr-X-Y ，X为map任务id Y为reduce对应的mod数
		//创建文件
		filename := "mr-"
		filename += strconv.Itoa(reply.Task.Tid)
		filename += "-"
		filename += strconv.Itoa(i)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Println("无法创建中间文件:", err)
		}
		enc := json.NewEncoder(file)
		defer file.Close()
		//写入内容
		for _, kv := range intermediate {
			if ihash(kv.Key)%mod == i {
				if err := enc.Encode(&kv); err != nil {
					fmt.Println("写入中间文件错误:", err)
				}
			}
		}
		filenames = append(filenames, filename)
	}
	// fmt.Printf("写入完成长度:%v",len(filenames))
	return filenames
}

// reduce工作
func reducework(reducef func(string, []string) string, reply WorkerReply) {
	//将答案写入到mr-out-X中
	//读取中间文件 
	kva := []KeyValue{}
	for _, filename := range reply.Task.Filename {
		//打开文件
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("打开中间文件错误")
		}
		dec := json.NewDecoder(file)
		//循环读取json数据
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	//中间文件为mr-X-Y
	//将kva写入mr-out-Y文件中
	//看文件名的最后那个Y即可知道要写入哪个out
	str := strings.Split(reply.Task.Filename[0], "-")
	new_filename := "mr-out-"
	new_filename += str[2]
	// 以追加模式打开文件或创建文件（如果文件不存在）
	sort.Sort(ByKey(kva))
	//创建临时文件存放数据
	dir,_:=os.Getwd()
	file, err := os.CreateTemp(dir,"mr-out-tmpfile")
	if err != nil {
		fmt.Println("创建临时文件错误")
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.实现追加写
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}
	//将临时文件重命名
	file.Close()
	os.Rename(dir+file.Name(), dir+new_filename)
	// fmt.Printf("reduce中的task:%v\n",reply.Task)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	loop := true
	args := WorkerArgs{}
	for loop {
		// time.Sleep(time.Second)
		reply, ok := CallForTask(args)
		// fmt.Printf("收到的任务：%v\n",reply.Task)
		//如果没有收到任务，则sleep 1s,注意这里要清空掉args
		if !ok { 
			args=WorkerArgs{}
			time.Sleep(time.Second)
			continue
		}
		// 停止任务进行
		if reply.Done {
			loop = false
		} else if reply.Task.Task_type == maptype { //map任务
			// fmt.Println(reply.Task.Tid)
			args.Filenames = mapwork(mapf, reply)
			args.Task = reply.Task
			// fmt.Printf("完成的map任务：%v\n",reply.Task)
		} else { //reduce 任务
			// fmt.Println(reply.Task)
			reducework(reducef, reply)
			args.Task = reply.Task
			// fmt.Printf("完成的reduce任务：%v\n",reply.Task)
		}
	}
}

// worker请求一个任务,传入上一个完成的任务
func CallForTask(args WorkerArgs) (WorkerReply, bool) {
	reply := WorkerReply{}
	// fmt.Println(args.filenames)
	//发送完成的任务
	// fmt.Printf("发送的完成任务：%v\n",args.Task) 
	call("Master.SentTask", &args, &reply)
	if reply.Done {
		return reply, true
	} else if reply.Task != nil {
		// fmt.Printf("获得一个请求，请求id为%v \n ", reply.Task.Tid)
		// fmt.Println(reply.Task)
		return reply, true
	} else {
		// fmt.Print("没有收到任务。。。")
		return reply, false
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//"unix" 表示网络类型，指示使用 UNIX 域套接字进行通信。而 sockname 则是 UNIX 域套接字的地址。
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
