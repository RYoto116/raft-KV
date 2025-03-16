package shardkv

import (
	"fmt"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongConfig = "ErrWrongConfig"
	ErrNotReady    = "ErrNotReady"
)

type Err string

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

const (
	ClientRequestTimeout   = 500 * time.Millisecond
	FetchConfigInterval    = 100 * time.Millisecond
	ShardMigrationInterval = 50 * time.Millisecond
	ShardGCInterval        = 50 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

type OpReply struct {
	Value string
	Err   Err
}

func getOperationType(op string) OperationType {
	switch op {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("unknown operation type: %s", op))
	}
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

// 去重表拷贝函数
func (info *LastOperationInfo) copyData() LastOperationInfo {
	return LastOperationInfo{
		SeqId: info.SeqId,
		Reply: &OpReply{
			Value: info.Reply.Value,
			Err:   info.Reply.Err,
		},
	}
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type RaftCommandType uint8

const (
	ClientOperation RaftCommandType = iota
	ConfigChange
	ShardMigration
	ShardGC
)

// 传入 raft 模块的操作类型
type RaftCommand struct {
	CmdType RaftCommandType
	Data    interface{}
	// ClientOperation -> Op, ConfigChange -> Config
	// ShardMigration  -> ShardOperationReply
	// ShardGC         -> ShardOperationArgs
}

type ShardOperationArgs struct {
	ConfigNum int
	ShardIDs  []int
}

type ShardOperationReply struct {
	ConfigNum      int
	ShardData      map[int]map[string]string
	DuplicateTable map[int64]LastOperationInfo
	Err            Err
}

type ShardStatus uint8

const (
	Normal ShardStatus = iota
	MoveIn
	MoveOut
	GC
)
