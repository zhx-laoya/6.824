package raft

//
// support for Raft and kvraft to save persistent 
// Raft state (log &c) and k/v server snapshots. 用于保存 Raft 状态（日志等）和 k/v 服务器的快照
//
// we will use the original persister.go to test your code for grading. 
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}
//更新raft的状态
func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
}
//读取raft的状态
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

//获取raft状态长度
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
