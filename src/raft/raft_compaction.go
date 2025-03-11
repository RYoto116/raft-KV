package raft

import "fmt"

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

// 服务器不再需要参数中包含的snapshot，Raft需要截断这些日志并存储下来
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, last=[%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

// 只需要对齐Term
type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d,", reply.Term)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Recv Snashot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject snapshot, higher term, T%d > T%d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term) // 将其他peers（包含Candidate）都变为Follower
	}

	// 检查本地是否包含Leader发送来的snapshot
	if rf.log.snapLastIdx >= args.LastIncludedIndex {
		LOG(rf.me, rf.commitIndex, DSnap, "<- S%d, Reject Snapshot, Already installed: %d>%d", args.LeaderId, rf.log.snapLastIdx, args.LastIncludedIndex)
		return
	}

	rf.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot) // 在内存中同步snapshot
	rf.persistLocked()                                                                   // 在persist中同步snapshot
	rf.snapPending = true
	rf.applyCond.Signal() // 唤醒application线程，在应用层同步snapshot
}

// Leader向peer同步Snapshot
func (rf *Raft) installToPeer(term int, args *InstallSnapshotArgs, peer int) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, lost or error", peer)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}

	if rf.isContextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context lost, T%d:%s->T%d:%s", peer, term, Leader, rf.currentTerm, rf.role)
		return
	}

	// 更新match/next index，同时避免乱序更新
	if rf.nextIndex[peer] < args.LastIncludedIndex {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	// 重要！！
	// 所有 Snapshot 都是已提交的日志，因此为peer同步Snapshot之后不需要更新 commitIndex
}
