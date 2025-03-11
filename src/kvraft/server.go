package kvraft

import (
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied  int
	stateMachine *MemoryKVStateMachine
	notifyChans  map[int]chan *OpReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 调用raft模块，将请求存储到raft日志中进行同步

	index, _, isLeader := kv.rf.Start(Op{
		Key:    args.Key,
		OpType: OpGet,
	})

	// 如果当前KVServer不是Leader，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// TODO: 等待Get结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	// 异步删除reply channel
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	index, _, isLeader := kv.rf.Start(Op{
		Key:    args.Key,
		Value:  args.Value,
		OpType: getOperationType(args.Op),
	})

	// 如果当前KVServer不是Leader，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// TODO: 等待Put/Append结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	// 异步删除reply channel
	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKVMachine()

	go kv.applyTask()
	return kv
}

// kv处理raft模块applyCh中的任务
func (kv *KVServer) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				// 忽略已经处理过的消息
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				kv.lastApplied = message.CommandIndex

				// 取出用户的操作信息
				op := message.Command.(Op)

				// 将操作应用到状态机中
				opReply := kv.applyToStateMachine(op)

				// 重要！！
				// 由 Leader 将reply发送回对应的server
				// 并发场景下，如果由其他节点发送opReply可能出现数据不一致，影响线性一致性
				if _, isLeader := kv.rf.GetState(); isLeader {
					// 构造reply channel，发送opReply
					kv.getNotifyChannel(message.CommandIndex) <- opReply
				}

				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyToStateMachine(op Op) *OpReply {
	var reply OpReply

	switch op.OpType {
	case OpGet:
		reply.Value, reply.Err = kv.stateMachine.Get(op.Key)
	case OpPut:
		reply.Err = kv.stateMachine.Put(op.Key, op.Value)
	case OpAppend:
		reply.Err = kv.stateMachine.Append(op.Key, op.Value)
	}

	return &reply
}

func (kv *KVServer) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1) // 创建一个容量为 1 的有缓冲区的通道
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}
