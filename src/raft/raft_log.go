package raft

import (
	"course/labgob"
	"fmt"
)

type RaftLog struct {
	snapLastIdx  int // 全局下标
	snapLastTerm int

	// 快照范围: 全局日志 (0, snapLastIdx]
	snapshot []byte
	// 日志范围: 全局日志(snapLastIdx, snapLastIdx+len(tailLog)]
	// 实现时添加dummy node
	tailLog []LogEntry
}

func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}

	// dummy节点避免边界溢出，有效logIndex下标从1开始
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)

	return rl
}

// 以下序列化/反序列化函数需要在rf.mu加锁情况下调用

// 机器宕机重启后需要反序列化
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIdx = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	// TODO: snapshot反序列化

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	// TODO: snapshot序列化
	e.Encode(rl.tailLog)
}

// RaftLog的长度
// 实际上是 (rl.snapLastIdx + 1) + len(rl.tailLog) - 1个dummy
func (rl *RaftLog) size() int {
	return rl.snapLastIdx + len(rl.tailLog)
}

// 还原tailLog的真实下标（即snapshot截断后下标）
func (rl *RaftLog) idx(logicIdx int) int {
	if logicIdx < rl.snapLastIdx || logicIdx >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIdx, rl.snapLastIdx+1, rl.size()-1))
	}
	return logicIdx - rl.snapLastIdx
}

func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

func (rl *RaftLog) firstForLocked(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIdx // 转换为全局下标
		} else if entry.Term > term {
			break
		}
	}
	return invalidIndex
}

// index是全局日志下标
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index) // 先计算idx，因为计算tailLog下标依赖于下方的snapLastIdx

	rl.snapLastIdx = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	// 日志截断（深拷贝）
	newLog := make([]LogEntry, 0, len(rl.tailLog)-idx)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

func (rl *RaftLog) appendFrom(logicPrevIdx int, entries []LogEntry) {
	idx := rl.idx(logicPrevIdx)
	rl.tailLog = append(rl.tailLog[:idx+1], entries...)
}

func (rl *RaftLog) last() (index, term int) {
	index = rl.size() - 1
	term = rl.at(index).Term
	return
}

func (rl *RaftLog) tail(startIdx int) []LogEntry {
	// 重要！！
	// 允许返回nil。不能过滤给rl.idx判断越界后panic
	if startIdx >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(startIdx):]
}

// debug时使用的方法
func (rl *RaftLog) String() string {
	var terms string
	prevIdx := rl.snapLastIdx
	prevTerm := rl.snapLastTerm
	for idx, entry := range rl.tailLog {
		if entry.Term != prevTerm {
			terms += fmt.Sprintf("[%d, %d]T%d ", prevIdx, idx+rl.snapLastIdx-1, prevTerm)
			prevTerm = entry.Term
			prevIdx = idx + rl.snapLastIdx
		}
		if idx == len(rl.tailLog)-1 {
			terms += fmt.Sprintf("[%d, %d]T%d", prevIdx, idx+rl.snapLastIdx, prevTerm)
		}
	}
	return terms
}
