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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

// 选举超时的上下界
const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	replicationInterval time.Duration = 200 * time.Millisecond // 需要小于选举超时的下界，从而抑制有异心的peer进行选举
)

// 收到Leader同步日志/给其他Candidiate投票后需要重置选举时钟
// 重置raft实例的选举时钟起始点和选举超时间隔
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	// 随机选择超时间隔
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// 判断当前选举是否超时
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
// Raft全局变量访问前需要加锁
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int // 当前任期
	votedFor    int // 在当期任期投票给谁，-1表示未投票

	// 选举周期控制字段
	electionStart   time.Time     // 选举时钟起始点
	electionTimeout time.Duration // 选举超时间隔，随机
}

// 状态机函数
// 参数term表示成为该term中的follower
func (rf *Raft) becomeFollowerLocked(term int) { // 持有锁时才能调用
	// 检查状态机当前状态是否允许变为Follower
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%d", term)
		return
	}
	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%d->T%d", rf.role, rf.currentTerm, term)
	rf.role = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1 // 进入新任期，需要重新投票
	}
	rf.currentTerm = term
}

func (rf *Raft) becomeCandidateLocked() {
	// 检查状态机当前状态是否允许变为Candidate
	// Leader不需要变成candidate
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DVote, "Leader can't become Candidate")
		return
	}
	// 进入新任期，变更状态，给自己投票
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeaderLocked() {
	// 检查状态机当前状态是否允许变为Leader
	// 只有Candidate可以变为Leader
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int // 候选者当前任期
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int // 带回被要票者的任期
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 对齐任期
	// 高任期的peer可以无视低任期peer的所有请求
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	// 变成Follower之后votedFor变为-1，可以投票
	// 检查是否在本任期投过票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Already voted to S%d", args.CandidateId, rf.votedFor)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote granted", args.CandidateId)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

	// Your code here (PartB).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 检测自身是否仍为发送RPC请求之前的角色，是否处于自认为的任期
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.role == role && rf.currentTerm == term)
}

// 心跳请求参数
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

// 心跳返回参数
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// 日志同步回调函数
// 只要Leader的任期不小于自己，就对其进行认可，变为Follower并重置选举时钟，承诺一段时间内不发起选举
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对齐任期
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	} else {
		rf.becomeFollowerLocked(args.Term)
	}

	// 重置选举时钟
	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 返回值表示是否成功发起了一轮心跳；如果上下文不发生变化则心跳成功
func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		// 处理RPC返回值
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		// 对齐任期
		// 对方的任期比自己高，自己退回Follower
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查上下文一致性
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Lost Leader[T%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		// 构造RPC请求参数，向其他peer发送RPC请求
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		go replicateToPeer(peer, args)
	}
	return true
}

// 只在给定的任期内进行日志同步
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		// 向其他peer发送RPC请求
		ok := rf.startReplication(term)
		if !ok {
			break
		}

		time.Sleep(replicationInterval)
	}
}

// 超时成为Candidate之后，针对所有Peer（除自己外）发起一次要票过程
// 发起选举只在term这个任期有效
func (rf *Raft) startElection(term int) {
	votes := 0                                                 // 统计票数
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) { // 对一个peer发送RPC并处理返回值
		reply := &RequestVoteReply{}
		// Candidate发送RPC请求调用RequestVote函数，线程安全，无需上锁
		ok := rf.sendRequestVote(peer, args, reply)

		// 处理RPC返回值
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from S%d, Lost or Error", peer)
			return
		}

		// 对齐任期，只有在相同任期下状态机才能推进
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// 检查上下文
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVote for S%d", peer)
			return
		}

		// 投票计数
		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
			}
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 检查上下文是否一致
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, abort RequestVote", rf.role)
		return
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}

		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}

		// 在临界区内，需要异步要票
		go askVoteFromPeer(peer, args)
	}

}

// 选举循环，进行超时检查和角色检查
func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// 非Leader且选举超时，变为Candidate状态
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.electionTicker()

	return rf
}
