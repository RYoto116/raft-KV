package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// 重置选举时钟
func (rf *Raft) resetElectionTimeoutLocked() {
	rf.electionStart = time.Now()
	// 随机超时时间
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionDuration = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// 选举超时检查
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionDuration
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// 非Leader节点选举超时，需要转换为Candidate
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, last=[%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted=%v", reply.Term, reply.VoteGranted)
}

// Raft 算法是强 Leader 算法，因此会要求 Leader 一定要包含所有已经提交日志。
// 进行选举时，需要确保只有具有比大多数 Peer 更新日志的候选人才能当选 Leader。

// 比较Candidate与Follower的最后一个日志条目新旧
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	lastIndex, lastTerm := rf.log.last()
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)

	// Term 大者新
	if candidateTerm != lastTerm {
		return lastTerm > candidateTerm
	}

	// Term一样，index大者新
	return lastIndex > candidateIndex
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
// 要票请求接收方逻辑
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Vote Asked, args=%v", args.CandidateId, args.String())

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject votes, higher term, T%d > T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 检查选票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject votes, already voted for S%d", args.CandidateId, rf.votedFor)
		return
	}

	// 检查候选者日志是否更全更新
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject votes, Candidate less up-to-date", args.CandidateId)
		return
	}

	rf.votedFor = args.CandidateId
	rf.persistLocked()

	reply.VoteGranted = true

	// 重要！别忘了
	rf.resetElectionTimeoutLocked()
	LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateId)
}

// Candidate要票：向其他peer发送RPC请求选票
func (rf *Raft) startElection(term int) {
	votes := 0
	// 重要：RPC请求需要在临界区之外
	askVoteFromPeer := func(args *RequestVoteArgs, peer int) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		// 处理请求响应
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d, lost or error", peer)
			return
		}

		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Askvote reply=%v", peer, reply.String())

		// 对齐Candidate当前term和选票响应term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 重要！！
		// 检查上下文
		if rf.isContextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply for S%d", peer)
			return
		}

		// 统计选票
		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				// 票数超过一半，变为Leader，发起心跳和日志同步
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isContextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, abort RequestVote", rf.role)
		return
	}

	lastIdx, lastTerm := rf.log.last()
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
		} else {
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Ask vote, args=%d", peer, args.String())
			go askVoteFromPeer(args, peer)
		}
	}
}
