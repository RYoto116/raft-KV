package raft

import "time"

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject log, higher term, T%d > T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	rf.resetElectionTimeoutLocked()
	LOG(rf.me, rf.currentTerm, DLog, "<- S%d, Log entries appended", args.LeaderId)
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
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isContextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		// 发起RPC
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		go replicateToPeer(args, peer)
	}

	return true
}
