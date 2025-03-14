package shardkv

import (
	"course/shardctrler"
	"time"
)

// 被调用场景：ShardKV.Get/PutAppend，ShardKV.fetchConfigTask
func (kv *ShardKV) ConfigCommand(cmd RaftCommand, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// 重要！！不能直接访问notifyChans[index]
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) handleConfigChange(cmd RaftCommand) *OpReply {
	switch cmd.CmdType {
	case ConfigChange:
		newConfig := cmd.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	default:
		panic("unknown config change type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	// 确定新的配置编号是否顺序匹配
	if newConfig.Num == kv.currentConfig.Num+1 {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				// 新配置需要将shard迁移出当前group的情况
				// todo
			} else if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				// 新配置需要将shard加入当前group的情况
				// todo
			}
		}

		kv.currentConfig = newConfig
		return &OpReply{Err: OK}
	}
	return &OpReply{Err: ErrWrongConfig}
}
