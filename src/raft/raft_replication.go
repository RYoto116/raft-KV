package raft

import "time"

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
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 接收方的回调函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// 对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject log, higher term, T%d > T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term) // 将其他peers都变为Follower
	}

	// 若PrevLogIndex不匹配，返回失败
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower's log too short, Len: %d < PrevLog: %d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}

	// 匹配：本地日志中给定index的term是否等于给定term
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower's log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 匹配成功，将参数中的Entries添加到本地
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// TODO: 实现LeaderCommit的应用功能

	rf.resetElectionTimeoutLocked()
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

// 返回Leader是否成功发起一轮心跳（上下文不变）
func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(args *AppendEntriesArgs, peer int) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply) // BUG1: peer 参数带入成了 rf.me

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Append entries to S%d, lost or error", peer)
			return
		}

		// 对齐term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 处理RPC响应
		// 若匹配失败，探测更小的nextIndex
		if !reply.Success {
			// TODO
			idx := args.PrevLogIndex
			term := args.PrevLogTerm
			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}

			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Not match with S%d in %d, try next=%d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		// 匹配成功，更新match/next index
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// TODO: 更新commitIndex
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		}
		go replicateToPeer(args, peer)
	}

	return true
}
