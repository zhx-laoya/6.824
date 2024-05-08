package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	// "fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// worker的请求
type WorkerArgs struct {
	Task      *Task    //worker完成的任务
	Filenames []string //如果完成的为map任务则要返回给中间文件给master
}

type WorkerReply struct {
	Task    *Task //从master请求到的新任务
	Done    bool  //任务是否已经全部完成
	NReduce int   //ihash用
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
