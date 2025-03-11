package raft

import (
	"bytes"
	"course/labgob"
	"fmt"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistLocked() {
	// Your code here (PartC).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)

	// 将 RaftLog 除了snapshot 的部分进行序列化
	rf.log.persist(e)
	// 将 RaftLog 的 snapshot存入 raftstate 进行序列化
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot)

	LOG(rf.me, rf.currentTerm, DPersist, "Persist: %v", rf.persistString())
}

func (rf *Raft) persistString() string {
	return fmt.Sprintf("T%d, VotedFor: %d, Log: [0, %d)", rf.currentTerm, rf.votedFor, rf.log.size())
}

// restore previously persisted state.
// 一定要在所有字段初始化完成后，再调用该函数。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int

	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currentTerm error: %v", err)
		return
	}
	rf.currentTerm = currentTerm

	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor error: %v", err)
		return
	}
	rf.votedFor = votedFor

	// 将 RaftLog 除了snapshot 的部分进行反序列化
	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error: %v", err)
		return
	}
	// 通过 persister 读取 snapshot 进行反序列化
	rf.log.snapshot = rf.persister.ReadSnapshot()

	// 重要！！
	// 基于测试框架的实现，所有 Snapshot 都是已提交的日志，因此宕机重启读取snapshot后需要比较更新 commitIndex和lastApplied
	if rf.log.snapLastIdx > rf.commitIndex {
		rf.commitIndex = rf.log.snapLastIdx
		rf.lastApplied = rf.log.snapLastIdx
	}
	LOG(rf.me, rf.currentTerm, DPersist, "Read from persist: %v", rf.persistString())
}
