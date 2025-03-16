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
	case result := <-notifyCh: // 阻塞等待applyTask的opReply
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
	case ConfigChange: // fetchConfigTask
		return kv.applyNewConfig(cmd.Data.(shardctrler.Config)) // 仅变更shard状态，不进行实际迁移
	case ShardMigration: // 处理 shardMigrationTask 向 raft 发送的 ShardMigration 类型指令
		shardsData := cmd.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardsData)
	case ShardGC:
		shardsInfo := cmd.Data.(ShardOperationArgs)
		return kv.applyShardGC(&shardsInfo)
	default:
		panic("unknown config change type")
	}
}

// 仅变更shard状态，不进行实际迁移；为prevConfig赋值
func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	// 确定新的配置编号是否顺序匹配
	// 重要！！
	if newConfig.Num == kv.currentConfig.Num+1 {
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				// 需要将shard加入当前group的情况
				gid := kv.currentConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveIn
				}
			}

			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				// 需要将shard迁移出当前group的情况
				gid := newConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveOut
				}
			}
		}

		kv.prevConfig = kv.currentConfig
		kv.currentConfig = newConfig
		return &OpReply{Err: OK}
	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardMigration(shardsData *ShardOperationReply) *OpReply {
	// 重要！！
	if shardsData.ConfigNum == kv.currentConfig.Num {
		for shardID, data := range shardsData.ShardData {
			// 将数据存储到当前group对应的shard中
			if kv.shards[shardID].Status == MoveIn {
				for k, v := range data {
					kv.shards[shardID].KV[k] = v
				}
				// 状态置为GC，等待清理
				kv.shards[shardID].Status = GC
			} else {
				break // 要迁移的shard数据不由当前group负责，直接退出数据迁移
			}
		}

		// 拷贝去重表数据
		for clientID, info := range shardsData.DuplicateTable {
			table, ok := kv.duplicateTable[clientID]
			// 需要存储的情况：原先去重表不存在client或info序号更大
			if !ok || table.SeqId < info.SeqId {
				kv.duplicateTable[clientID] = info
			}
		}

		return &OpReply{Err: OK}
	}

	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardGC(shardsInfo *ShardOperationArgs) *OpReply {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardID := range shardsInfo.ShardIDs {
			if kv.shards[shardID].Status == GC {
				// 将GC变为Normal
				kv.shards[shardID].Status = Normal
			} else if kv.shards[shardID].Status == MoveOut {
				// 删除已经MoveOut的shard
				kv.shards[shardID] = NewMemoryKVMachine()
			} else {
				break
			}
		}

		return &OpReply{Err: OK}
	}

	return &OpReply{Err: ErrWrongConfig}
}
