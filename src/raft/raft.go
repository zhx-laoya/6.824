package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// ApplyMsg, but set CommandValid to false for these other uses.
// 当每个 Raft 对等节点意识到连续的日志条目已经被提交时，
// 对等节点应通过传递给 Make() 的 applyCh 向服务（或测试程序）发送 ApplyMsg。
// 将 CommandValid 设置为 true，以指示 ApplyMsg 包含一个新提交的日志条目。

// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// 在实验三中，您可能希望在 applyCh 上发送其他类型的消息（例如快照）；
// 在这种情况下，您可以向 ApplyMsg 添加字段，但对于这些其他用途，请将 CommandValid 设置为 false。

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	follower_type  = 0
	candidate_type = 1
	leader_type    = 2
)

// A Go object implementing a single Raft peer.
// 存的是单个服务器
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers 全局的所有服务器包括自己
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[] //自己服务器所在下标， peers[me]
	dead      int32               // set by Kill()
	// applyCh 是一个通道，测试程序或服务希望 Raft 通过该通道发送 ApplyMsg 消息。
	StatusType int //当前服务器的身份
	Applych    chan ApplyMsg
	//lab2A
	CurrentTerm  int       //接收到的最大的一个任期
	VotedFor     int       //当前的投票信息 ，如果为len(peers)则为未投票，
	ElectionTime time.Time //选举超时时间
	votechan     chan bool //投票通道
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
// 返回当前服务器的状态
func (rf *Raft) GetState() (int, bool) {
	//这里没加锁导致-race出问题了
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.CurrentTerm
	isleader = (rf.StatusType == leader_type)
	// fmt.Printf("我的id：%v，我的身份：%v\n 我的任期：%v",rf.me,rf.StatusType,rf.CurrentTerm)
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!字段名必须以大写字母开头!
// 由candidate发送
type RequestVoteArgs struct {
	//2A 用于candidate发送投票请求
	Term        int //candidate的当前任期
	CandidateId int //发送者的id
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 由其他服务器回复
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 恢复者的term信息
	VoteGranted bool //true代表赞同票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//我的reset函数自带加锁，所以不能在这里加锁！！！！！否则会卡死在reset函数那里
	// Your code here (2A, 2B).
	//已知candidate的信息存储在args中
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentterm := rf.CurrentTerm
	reply.Term = rf.CurrentTerm
	statustype := rf.StatusType
	servers := len(rf.peers)
	votedfor := rf.VotedFor
	// flag := false
	// if args.Term > currentterm { //任期较大
	// flag = true
	// fmt.Println("error吗........................................")
	// }
	if args.Term < currentterm { //过期rpc 不管,但也要返回我的任期给candidate知道
		reply.VoteGranted = false
		return
	}
	// 任期一样,只考虑follower投票问题
	if args.Term == currentterm {
		if statustype == follower_type { //只有follower才会投票
			if votedfor == servers || votedfor == args.CandidateId { //未投票或者本来就投给它
				// fmt.Printf("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh 我的id：%v，我的votedfor：%v\n", rf.me, rf.VotedFor)
				reply.VoteGranted = true
				rf.VotedFor = args.CandidateId
				rf.reseteletiontime()
			}
		}
		return
	}
	//它的term大我就一定要投给它
	reply.VoteGranted = true
	// if flag {
	// 	fmt.Println("error................................................................REPLY SUCCESS!!!!")
	// }
	rf.VotedFor = args.CandidateId
	//更新其任期，变为follower，重置超时时间，返回任期
	rf.changeto(follower_type)
	rf.reseteletiontime()
	rf.CurrentTerm = args.Term
	// fmt.Printf("收到request信息了！！！，我的id：%v，发送方任期：%v,我的任期：%v 收到信息后我的身份：%v\n", rf.me, args.Term, currentterm, rf.StatusType)

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[]. server 是目标服务器在 rf.peers[] 中的索引。
// expects RPC arguments in args. 期望将 RPC 参数放入 args 中。
// fills in *reply with RPC reply, so caller should 使用 RPC 回复填充 *reply，因此调用方应传递 &reply
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// labrpc 包模拟了一个有丢失的网络，在该网络中，服务器可能无法访问，请求和回复可能会丢失。
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// Call() 发送请求并等待回复。如果在超时间隔内收到回复，Call() 将返回 true；
// 否则，Call() 将返回 false。因此，Call() 可能要等一段时间才会返回。
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
// 返回 false 可能是由于服务器宕机、无法访问的活动服务器、丢失的请求或丢失的回复引起的。
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
//	Call() 保证会返回（可能会有延迟），除非服务器端的处理函数没有返回。
//	因此，在 Call() 周围没有必要实现自己的超时机制
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 由candidate发起
// 注意这里发给对面的时候对面可能会死锁了！！！！！！！！！！！
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntirs RPC
// 由leader发起
type AppendEntriesArgs struct {
	Term     int //leader term
	LeaderId int
}
type AppendEntriesReply struct {
	Term    int  //返回当前服务器的currentterm
	Success bool //true表明服务正常
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentterm := rf.CurrentTerm
	reply.Term = currentterm
	reply.Success = true
	// fmt.Printf("我是：%d，我收到leader消息了！我的任期:%v,leader任期：%v\n",rf.me,rf.CurrentTerm,args.Term)
	// fmt.Printf("发送者任期：%v，接收者任期：%v\n",args.Term,reply.Term)
	// if(rf.StatusType==leader_type){
	// 	fmt.Println("error....................................")
	// }
	if args.Term < currentterm { //leader发送的任期比它已知任期小 ，则表明该服务器拒绝该append请求
		reply.Success = false
		return
	}
	//rule servers-allservers-2 投完票后重置为follower，并	且重置超时时间
	//只可能我是candidate或者follower,那么接收请求即将我转变为follower,注意votefor不会变化
	rf.changeto(follower_type)
	rf.CurrentTerm = args.Term
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// atomic 包提供了原子操作的函数，用于在多个goroutine之间进行原子操作。
// 这些原子操作可以保证操作的原子性，即在执行期间不会被中断。
// atomic 包的函数使用底层的原子指令来确保操作的原子性，而不需要使用互斥锁

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// 在每个测试之后，测试程序不会停止 Raft 创建的 goroutine，
// 但它会调用 Kill() 方法。您的代码可以使用 killed() 来检查是否已调用 Kill()。
// 使用 atomic 可以避免使用锁。

// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
// 问题在于长时间运行的 goroutine 会使用内存，并可能占用 CPU 时间，
// 这可能导致后续的测试失败并生成令人困惑的调试输出。
// 任何具有长时间运行循环的 goroutine 应调用 killed() 来检查是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 服务或测试程序希望创建一个 Raft 服务器。所有 Raft 服务器（包括此服务器）的端口都在 peers[] 中，
// 此服务器的端口是 peers[me]。所有服务器的 peers[] 数组顺序相同。
// persister 是用于该服务器保存其持久状态的位置，并且在有时还保存最近保存的状态。
// applyCh 是一个通道，测试程序或服务希望 Raft 通过该通道发送 ApplyMsg 消息。
// Make() 必须快速返回，因此它应为任何长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.StatusType = follower_type
	rf.CurrentTerm = 0 //初始化任期为0
	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = len(rf.peers)
	rf.reseteletiontime()
	rf.votechan = make(chan bool, 5)
	// fmt.Printf("我的id是：%v，我的超时时间是%v\n",rf.me,rf.ElectionTime)
	go rf.Ticker()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

// 更新重置超时时间
func (rf *Raft) reseteletiontime() {
	randtime := rand.Intn(300) + 500
	rf.ElectionTime = time.Now().Add(time.Duration(randtime) * time.Millisecond)
}

// candidate要做的事
func (rf *Raft) candidate_do() {
	count := 1
	//给自己投票
	rf.mu.Lock()
	rf.VotedFor = rf.me
	me := rf.me
	servers := len(rf.peers)
	rf.mu.Unlock()

	//发送rpc要使用go协程很大的原因在于发送失败的超时时间很长，同时发送给其他人也会阻塞
	//开启协程来发送信息
	//不开启协程可能出现这种情况（前面只是发现这个问题但不知道为什么，后面调试时发现导致这个的原因是rpc发送失败需要的时间很多，导致消息被堆积在后面了）
	//发送者的身份：1 ，1向2发送了一次请求,发送任期：3，接受任期：6，结果为false
	//然而此时发送者的任期已经不为3了

	//用来确保协程完成
	var wg sync.WaitGroup
	//用来确保count被正确修改
	var countLock sync.Mutex
	for i := 0; i < servers; i++ {
		rf.mu.Lock()
		statustype:=rf.StatusType 
		rf.mu.Unlock() 
		if i != me && statustype == candidate_type {
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				rf.mu.Lock() 
				args := RequestVoteArgs{}
				args.CandidateId = me
				args.Term = rf.CurrentTerm
				reply := RequestVoteReply{}
				rf.mu.Unlock() 
				// fmt.Printf("Innum:%d  ----   %d ------发送给------%d 时间：%v\n",num,rf.me,server,time.Now())
				ok := rf.sendRequestVote(server, &args, &reply)
				// fmt.Printf("--------------------------------OUTnum %v \n", num)
				//注意发送成功的时间忽略不计，但是发送失败的时间在600-2000ms之间，这会导致waitgroup阻塞
				//所以检测票数是否大于等于一半时不要等所有协程结束后统一计算，而要一达到要求就进行状态的转换
				if !ok { //发送失败 
					return
				}

				rf.mu.Lock() 
				if rf.StatusType != candidate_type { //有可能发送完不是candidate了,不加的话可能会导致同个任期出现多个leader
					rf.mu.Unlock() 
					return
				}
				rf.mu.Unlock() 
				// fmt.Printf("成功的Outnum:%d ----  %d发送给%d成功 经过多少时间：%v\n",num,rf.me,server,time.Since(curtime).Milliseconds())
				// fmt.Printf("发送者的身份：%v ，%v向%v发送了一次请求,发送任期：%v，接受任期：%v，结果为%v\n", rf.StatusType, rf.me, server, args.Term, reply.Term, reply.VoteGranted)
				// fmt.Printf("成功的Outnum:%d ---发送者的身份：%v ，%v向%v发送了一次请求,发送任期：%v，接受任期：%v，结果为%v\n", num,rf.StatusType, rf.me, server, args.Term, reply.Term, reply.VoteGranted)

				//收到reply的任期较大，那我也得变为follower
				rf.mu.Lock() 
				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.changeto(follower_type)
					rf.mu.Unlock() 
					return
				}
				if reply.VoteGranted {
					countLock.Lock()
					count++
					countLock.Unlock()
					if count > servers/2 { //获得超过一半的投票,变为leader
						rf.changeto(leader_type)
						// fmt.Printf("我的id：%v,我的任期:%v，我的身份:%v，收到几张投票：%v\n", rf.me, rf.CurrentTerm, rf.StatusType, count)
						rf.mu.Unlock() 
						return
					}
				}
				rf.mu.Unlock() 
			}(i)
		}
	}

	//保护goroutine的生命周期
	wg.Wait() // 等待所有 goroutine 完成
}

func (rf *Raft) leader_do() {
	// fmt.Printf("我成为leader了，我的任期：%v，我的id：%v\n",rf.CurrentTerm,rf.me)
	rf.mu.Lock()
	me := rf.me
	servers := len(rf.peers)
	rf.mu.Unlock()

	var wg sync.WaitGroup

	for i := 0; i < servers; i++ {
		rf.mu.Lock()
		statustype:=rf.StatusType
		rf.mu.Unlock() 
		if i != me && statustype==leader_type {
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				rf.mu.Lock() 
				if rf.StatusType != leader_type {
					rf.mu.Unlock() 
					return
				}
				args := AppendEntriesArgs{}
				args.LeaderId = me
				args.Term = rf.CurrentTerm 
				rf.mu.Unlock() 

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				// fmt.Printf("%v向%v发送了一次heartbeat请求,发送任期：%v，接受任期：%v，结果为%v\n", rf.me, server, rf.CurrentTerm, reply.Term, reply.Success)
				if !ok {
					return
				}
				rf.mu.Lock() 
				if reply.Term > rf.CurrentTerm { //其他节点的任期较大
					rf.changeto(follower_type)
				}
				rf.mu.Unlock() 
			}(i)
		}
	}
	wg.Wait() // 等待所有 goroutine 完成

}
func (rf *Raft) Ticker() {
	for !rf.killed() {
		//超时选举检查,follower和candidate超时都重新开始选举
		rf.mu.Lock()
		if time.Now().After(rf.ElectionTime) && rf.StatusType != leader_type { //则它成为candidate
			rf.changeto(candidate_type)
			go rf.candidate_do() 
			// fmt.Printf("我成为candidate了，我的id是：%v，我的任期是：%v，我的超时时间是：%v\n", rf.me, rf.CurrentTerm, rf.ElectionTime)
		}
		//不是在这里发起投票请求的！！！！！，candidate一个任期只会发送一次投票请求
		// if Statustype == candidate_type { //为候选者则发送投票信息

		//在这里测试心跳机制
		if rf.StatusType == leader_type { //向其他端口发送append信息,lab1中为心跳信息
			go rf.leader_do()
		}
		rf.mu.Unlock()

		//50ms检测一次是否超时
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) changeto(changetype int) {
	// fmt.Printf("change:-------------id：%v ，变化：%v %v \n",rf.me,rf.StatusType,changetype)
	if changetype == candidate_type {
		rf.CurrentTerm++
		rf.reseteletiontime()
		rf.VotedFor = len(rf.peers)
		rf.StatusType = candidate_type
	} else if changetype == leader_type {
		rf.VotedFor = len(rf.peers)
		rf.StatusType = leader_type
	} else {
		//如果不为follower才将投票信息清空
		if rf.StatusType != follower_type {
			rf.VotedFor = len(rf.peers)
		}
		rf.reseteletiontime()
		rf.StatusType = follower_type
	}
}
