package shardkv

import (
	"bytes"
	"course/labgob"
	"course/labrpc"
	"course/raft"
	"course/shardctrler"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32
	lastApplied int

	shards         map[int]*MemoryKVStateMachine // shard -> 状态机
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo

	prevConfig    shardctrler.Config // 配置变更时shard迁移需要获取之前配置中shard所属的gid
	currentConfig shardctrler.Config
	mck           *shardctrler.Clerk
}

func (kv *ShardKV) isRequestDuplicate(clientId, seqId int64) bool {
	info, ok := kv.duplicateTable[clientId]
	return ok && seqId <= info.SeqId
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 判断key是否属于当前server所属group负责的shard
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 调用raft模块，将操作请求Op存储到applyCh中，通过raft进行同步
	index, _, isLeader := kv.rf.Start(RaftCommand{
		CmdType: ClientOperation,
		Data: Op{
			Key:    args.Key,
			OpType: OpGet,
		},
	})

	// 如果当前KVServer不是Leader，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待Get结果
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 判断key是否属于当前server所属group负责的shard
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.isRequestDuplicate(args.ClientId, args.SeqId) {
		opReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = opReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(RaftCommand{
		CmdType: ClientOperation,
		Data: Op{
			Key:      args.Key,
			Value:    args.Value,
			OpType:   getOperationType(args.Op),
			ClientId: args.ClientId,
			SeqId:    args.SeqId,
		},
	})

	// 如果当前KVServer不是Leader，直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// 等待Put/Append结果
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(RaftCommand{})
	labgob.Register(ShardOperationArgs{})
	labgob.Register(ShardOperationReply{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Use something like this to talk to the shardctrler:
	// 初始化shardCtrler客户端，以获取最新配置
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.dead = 0
	kv.lastApplied = 0

	kv.shards = make(map[int]*MemoryKVStateMachine)
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)

	// 初始化默认配置
	kv.prevConfig = shardctrler.DefaultConfig()
	// 需要后台线程不断从shardCtrler获取集群的最新配置
	kv.currentConfig = shardctrler.DefaultConfig()

	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask() // 取出日志中的RaftCommand

	// 后台获取最新配置
	go kv.fetchConfigTask() // 调用kv.ConfigCommand

	go kv.shardMigrationTask()

	go kv.shardGCTask()

	return kv
}

func (kv *ShardKV) applyToStateMachine(op Op) *OpReply {
	var reply OpReply
	shardID := key2shard(op.Key)

	switch op.OpType {
	case OpGet:
		reply.Value, reply.Err = kv.shards[shardID].Get(op.Key)
	case OpPut:
		reply.Err = kv.shards[shardID].Put(op.Key, op.Value)
	case OpAppend:
		reply.Err = kv.shards[shardID].Append(op.Key, op.Value)
	}

	return &reply
}

func (kv *ShardKV) getNotifyChannel(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1) // 创建一个容量为 1 的有缓冲区的通道
	}
	return kv.notifyChans[index]
}

func (kv *ShardKV) removeNotifyChannel(index int) {
	delete(kv.notifyChans, index)
}

// 对statemachine中的数据以及去重表进行snapshot持久化
func (kv *ShardKV) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(kv.shards)
	e.Encode(kv.duplicateTable)
	e.Encode(kv.prevConfig)
	e.Encode(kv.currentConfig)

	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *ShardKV) restoreFromSnapshot(snaphot []byte) {
	if len(snaphot) == 0 {
		// 重要！！没有Snapshot时，需要初始化shard信息
		for i := 0; i < shardctrler.NShards; i++ {
			if _, ok := kv.shards[i]; !ok {
				kv.shards[i] = NewMemoryKVMachine()
			}
		}
		return
	}

	buf := bytes.NewBuffer(snaphot)
	d := labgob.NewDecoder(buf)
	var shards map[int]*MemoryKVStateMachine
	var duplicateTable map[int64]LastOperationInfo
	var prevConfig shardctrler.Config
	var currentConfig shardctrler.Config

	if d.Decode(&shards) != nil || d.Decode(&duplicateTable) != nil || d.Decode(&prevConfig) != nil || d.Decode(&currentConfig) != nil {
		panic("failed to restore from snashot")
	}
	kv.shards = shards
	kv.duplicateTable = duplicateTable
	kv.prevConfig = prevConfig
	kv.currentConfig = currentConfig
}

// 判断key是否属于当前server所属group负责的shard
// 根据shard状态判断能否提供服务。如果是 GC 或者 Normal 状态，均可以继续提供服务
func (kv *ShardKV) matchGroup(key string) bool {
	shard := key2shard(key)
	return kv.gid == kv.currentConfig.Shards[shard] && (kv.shards[shard].Status == Normal || kv.shards[shard].Status == GC)
}
