package shardctrler

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"sync"
	"sync/atomic"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead           int32
	lastApplied    int
	stateMachine   *CtrlerStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo

	configs []Config // indexed by config num
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

func (sc *ShardCtrler) isRequestDuplicate(clientId, seqId int64) bool {
	info, ok := sc.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

// 向集群添加一个Group，需要处理负载均衡问题
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isRequestDuplicate(args.ClientId, args.SeqId) {
		opReply := sc.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	var opReply OpReply
	sc.command(Op{
		Servers:  args.Servers,
		OpType:   OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &opReply)

	reply.Err = opReply.Err
}

// 将若干个group从集群中删除
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isRequestDuplicate(args.ClientId, args.SeqId) {
		opReply := sc.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	var opReply OpReply
	sc.command(Op{
		GIDs:     args.GIDs,
		OpType:   OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isRequestDuplicate(args.ClientId, args.SeqId) {
		opReply := sc.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	var opReply OpReply
	sc.command(Op{
		Shard:    args.Shard,
		GID:      args.GID,
		OpType:   OpMove,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}, &opReply)

	reply.Err = opReply.Err
	// reply.WrongLeader = opReply.WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		Num:    args.Num,
		OpType: OpQuery,
	}, &opReply)

	reply.Config = opReply.ControllerConfig
	reply.Err = opReply.Err
}

// 操作通用方法
func (sc *ShardCtrler) command(args Op, reply *OpReply) {
	index, _, isLeader := sc.rf.Start(args)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	// 重要！！不能直接访问notifyChans[index]
	notifyCh := sc.getNotifyChannel(index)
	sc.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Err = result.Err
		reply.ControllerConfig = result.ControllerConfig
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeNotifyChannel(index)
		sc.mu.Unlock()
	}()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{} // 默认配置

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.lastApplied = 0

	sc.stateMachine = NewCtrlerStateMachine() // TODO

	sc.notifyChans = make(map[int]chan *OpReply)
	sc.duplicateTable = make(map[int64]LastOperationInfo)

	go sc.applyTask()

	return sc
}

func (sc *ShardCtrler) applyTask() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				sc.mu.Lock()

				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}

				sc.lastApplied = message.CommandIndex

				op := message.Command.(Op)

				var opReply *OpReply
				if op.OpType != OpQuery && sc.isRequestDuplicate(op.ClientId, op.SeqId) {
					opReply = sc.duplicateTable[op.ClientId].Reply
				} else {
					opReply = sc.applyToStateMachine(op)
					if op.OpType != OpQuery {
						sc.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				if _, isLeader := sc.rf.GetState(); isLeader {
					sc.getNotifyChannel(message.CommandIndex) <- opReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyToStateMachine(op Op) *OpReply {
	var reply OpReply

	switch op.OpType {
	case OpJoin:
		reply.Err = sc.stateMachine.Join(op.Servers)
	case OpLeave:
		reply.Err = sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		reply.Err = sc.stateMachine.Move(op.Shard, op.GID)
	case OpQuery:
		reply.ControllerConfig, reply.Err = sc.stateMachine.Query(op.Num)
	}

	return &reply
}

func (sc *ShardCtrler) getNotifyChannel(index int) chan *OpReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpReply, 1) // 重要！！缓冲，否则过不了Historical queries test
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeNotifyChannel(index int) {
	delete(sc.notifyChans, index)
}
