package mr

import (
	// "errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 任务类型
const (
	maptype    = 0
	reducetype = 1
)

const (
	free_state = 0 //空闲状态
	busy_state = 1 //处理状态
	fin_state  = 2 //完成状态
)

type Task struct {
	Tid        int       //任务标识符
	Filename   []string  //这个任务的要处理的文件的文件位置
	Task_type  int       //任务类型，有map类型以及reduce类型,0为maptype，1为reducetype
	Task_state int       //0 1 2
	start_time time.Time //任务被分配时的时间
}

type Master struct {
	current_task_id int
	// Your definitions here.
	Mu         sync.Mutex    //互斥访问，多个worker互斥访问master
	Midfile    []string      //所有的中间文件
	NReduce    int           //reduce任务个数
	Mapcnt     int           //还有多少个map任务没完成
	Mapdone    bool          //map任务是否完成
	Reducecnt  int           //还有多少个reduce任务没完成
	MapChan    chan *Task    //map任务管道
	ReduceChan chan *Task    //reduce任务管道
	TaskMap    map[int]*Task //管理任务，后续如果有超时的则cut掉它
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 生成reduce任务
func (m *Master) produce_reduce() {
	for i := 0; i < m.NReduce; i++ {
		filenames := []string{}
		for _, filename := range m.Midfile {
			if strings.HasPrefix(filename, "mr-") && strings.HasSuffix(filename, strconv.Itoa(i)) {
				filenames = append(filenames, filename)
			}
		}
		t := Task{
			Tid:        m.current_task_id + 1,
			Filename:   filenames,
			Task_type:  reducetype,
			Task_state: free_state,
			start_time: time.Now(),
		}
		// fmt.Printf("生成任务长度：%v\n",len(t.Filename))
		m.current_task_id++
		m.TaskMap[m.current_task_id] = &t
		m.ReduceChan <- &t
	}
}
func (m *Master) SentTask(args *WorkerArgs, reply *WorkerReply) error {
	//args里的task为已经完成的任务
	//注意要在这里加锁，否则可能会出现多个id进来并发错误
	m.Mu.Lock()
	// fmt.Printf("完成的任务：%v\n",args.Task)
	if args.Task != nil &&  m.TaskMap[args.Task.Tid].Task_state == busy_state { //修改该任务状态为完成
		m.TaskMap[args.Task.Tid].Task_state = fin_state
		//如果是map任务完成
		// fmt.Printf("收到的完成任务：%v 当前还未完成的mapcnt：%v .... \n ",args.Task,m.Mapcnt )
		if args.Task.Task_type == maptype {
			m.Mapcnt--
			m.Midfile = append(m.Midfile, args.Filenames...)
		} else { //完成reduce任务
			m.Reducecnt--
		}
	}
	// fmt.Println(m.Mapcnt)
	//所有map任务处理完了,生成reduce任务
	if m.Mapcnt == 0 && !m.Mapdone {
		m.Mapdone = true
		m.produce_reduce()
	}
	//回复
	//hash信息
	reply.NReduce = m.NReduce
	if m.Mapcnt == 0 && m.Reducecnt == 0 {
		reply.Done = true
		m.Mu.Unlock()
		// fmt.Println("所有任务已完成")
		return nil
	}
	//还有map任务没完成
	// fmt.Println(m.Mapcnt)
	if m.Mapcnt != 0 {
		// 存在map任务
		if len(m.MapChan) > 0 {
			task := <-m.MapChan
			task.start_time = time.Now()
			task.Task_state = busy_state
			reply.Task = task
			m.Mu.Unlock()
			// fmt.Println(task)
			return nil
		}
		m.Mu.Unlock()
		// fmt.Println("没有空闲map任务")
		return nil
	}
	//还有未发送的reduce任务
	if len(m.ReduceChan) > 0 {
		task := <-m.ReduceChan
		task.start_time = time.Now()
		task.Task_state = busy_state
		reply.Task = task
		m.Mu.Unlock()
		// fmt.Printf("剩余的map任务：%v,剩余的reduce任务：%v 当前任务发送任务id：%v\n",m.Mapcnt,m.Reducecnt,task.Tid)
		return nil
	} else { //返回一个空任务
		reply.Task = nil
		m.Mu.Unlock()
		return nil
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := (m.Reducecnt == 0 && m.Mapcnt == 0)
	// Your code here.
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		current_task_id: 0,
		Mu:              sync.Mutex{},
		NReduce:         nReduce,
		Mapcnt:          len(files), //还有多少个map任务没有完成，只有为0才能进入reduce阶段
		Reducecnt:       nReduce,    //还有多少个reduce任务没有完成
		MapChan:         make(chan *Task, len(files)),
		ReduceChan:      make(chan *Task, nReduce),
		TaskMap:         make(map[int]*Task),
		Midfile:         []string{},
		Mapdone:         false,
	}
	//将所有输入文件转化为任务,即发布map任务
	for _, filename := range files {
		// fmt.Println(filename)
		t := Task{
			Tid:        m.current_task_id + 1,
			Filename:   []string{filename},
			Task_type:  maptype,
			Task_state: free_state,
			start_time: time.Now(),
		}
		m.current_task_id++
		m.TaskMap[m.current_task_id] = &t
		m.MapChan <- &t
	}
	// Your code here.
	m.server()

	go m.CrashCheck()
	return &m
}

// 每隔5秒钟check一次
func (m *Master) CrashCheck() {
	for {
		time.Sleep(time.Second * 1)
		//查询所有的map中哪些busy的事务
		m.Mu.Lock()
		for _, task := range m.TaskMap {
			if task.Task_state == busy_state {
				duration := time.Since(task.start_time)
				if duration > time.Second*10 { //超过10s则换将其状态置为free
					task.Task_state=free_state 
					if task.Task_type==maptype {
						// fmt.Println("mapchan")
						// fmt.Printf("crash map任务id:%v\n",task.Tid)
						m.MapChan<-task 
					}else {
						m.ReduceChan<-task
						// fmt.Println("reducechan")
					}
				}
			}
		}
		m.Mu.Unlock()
	}
}
