package raft

import (
	"fmt"
	"sort"
	"time"
)

const (
	invalidTerm  int = 0
	invalidIndex int = 0
)

type LogEntry struct {
	CommandValid bool        // 是否应该应用日志
	Command      interface{} // 操作日志，任意结构体
	Term         int         // 日志所在任期
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// 匹配点试探参数，term和index唯一确定一条日志
	PrevLogIndex int
	PrevLogTerm  int
	// 待追加日志条目
	Entries []LogEntry

	// 更新peer的commitIndex
	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, prev=[%d]T%d, logs=(%d, %d], CommitIdx=%d", args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Success=%v, ConflictTerm=[%d]T%d", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 接收方的回调函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, entries appended, args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false

	// 重要！！
	// 收到AppendEntries RPC时，无论Follower接受还是拒绝日志，只要认可对方是Leader就要重置选举时钟
	// 否则，如果Leader和Follower匹配日志所花时间特别长，Follower一直不重置选举时钟，就有可能错误地触发超时选举，变为新任期的Candidate
	defer func() {
		rf.resetElectionTimeoutLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower log=%v", args.LeaderId, rf.logString())
		}
	}()

	// 对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject log, higher term, T%d > T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term) // 将其他peers都变为Follower
	}

	// 若Follower日志过短，则匹配失败，将ConflictIndex设为Follower的日志长度
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = invalidTerm

		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower's log too short, Len: %d < PrevLog: %d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}

	// Follower日志在PrevLogIndex处的term不等于给定term，则匹配失败，更新ConflictIndex和ConflictTerm
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// ConflictTerm设置为Follower日志在PrevLogIndex处的term
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		// ConflictIndex设置为ConflictTerm的第一条日志
		reply.ConflictIndex = rf.firstLogForLocked(reply.ConflictTerm)

		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower's log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 匹配成功，将参数中的Entries添加到本地
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 实现LeaderCommit的应用功能
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index: %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal() // Signal唤醒阻塞在Wait()上的goroutine
	}
}

// 心跳循环、日志同步，生命周期是一个term
func (rf *Raft) replicationTicker(term int) {
	for rf.killed() == false {
		ok := rf.startReplication(term)
		if !ok {
			break // 心跳循环生命周期结束
		}

		time.Sleep(replicationInterval)
	}
}

// 返回Leader是否成功发起一轮心跳，只在给定 term 内有效
func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(args *AppendEntriesArgs, peer int) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply) // BUG1: peer 参数带入成了 rf.me

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, lost or error", peer)
			return
		}

		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AppendEntries, reply=%v", peer, reply.String())

		// 对齐term，让出领导权
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 重要！！
		// 检查上下文，检查自己仍然是给定 term 的 Leader
		if rf.isContextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context lost, T%d:%s->T%d:%s", peer, term, Leader, rf.currentTerm, rf.role)
			return
		}

		// 处理RPC响应参数

		// 若匹配失败，探测更小的nextIndex：回滚。
		// 【难点】每回滚一次需要来回两次RPC。而replicationInterval是固定的，RPC次数过多，同步该peer日志的间隔过大，导致tester中日志同步超时
		if !reply.Success {
			prevNext := rf.nextIndex[peer]
			// ConflictTerm为空，说明Follower日志太短，
			if reply.ConflictTerm == invalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstTermIndex := rf.firstLogForLocked(reply.ConflictTerm)
				if firstTermIndex != invalidIndex {
					rf.nextIndex[peer] = firstTermIndex
				} else {
					// Leader日志中不存在ConflictTerm期间的日志
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}

			// 避免多次回退时，较早的回退RPC较晚到达Leader，使得nextIndex[peer]反而增大
			if rf.nextIndex[peer] > prevNext {
				rf.nextIndex[peer] = prevNext
			}

			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, try next Prev=[%d]T%d", peer, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1].Term)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.logString())
			return
		}

		// 匹配成功，更新match/next index
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 更新commitIndex：所有peer匹配点的众数（超过半数的数-排序中位数）
		majorityMatched := rf.getMajorityIndexLocked()

		// Figure 8: Leader不能提交非当前任期的日志
		if majorityMatched > rf.commitIndex && rf.log[majorityMatched].Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index: %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal() // Signal后释放mu
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 重要！！
	if rf.isContextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// 重要！！
			rf.matchIndex[rf.me] = len(rf.log) - 1 // Leader自己当前的匹配下标
			rf.nextIndex[rf.me] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term

		// 发起RPC
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:],
			LeaderCommit: rf.commitIndex,
		}

		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, append entries, args=%v", peer, args.String())
		go replicateToPeer(args, peer)
	}

	return true
}

func (rf *Raft) getMajorityIndexLocked() int {
	tmpIndices := make([]int, len(rf.matchIndex))
	copy(tmpIndices, rf.matchIndex)
	sort.Ints(tmpIndices)
	LOG(rf.me, rf.currentTerm, DDebug, "Majority index after sort: %v[%d]=%d", tmpIndices, len(tmpIndices)/2, tmpIndices[len(tmpIndices)/2])
	return tmpIndices[len(tmpIndices)/2]
}

func (rf *Raft) logString() string {
	var terms string
	prevIdx := 0
	prevTerm := 0
	for idx, entry := range rf.log {
		if entry.Term != prevTerm {
			terms += fmt.Sprintf("[%d, %d]T%d ", prevIdx, idx-1, prevTerm)
			prevTerm = entry.Term
			prevIdx = idx
		}
		if idx == len(rf.log)-1 {
			terms += fmt.Sprintf("[%d, %d]T%d", prevIdx, idx, prevTerm)
		}
	}
	return terms
}
