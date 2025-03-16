package shardkv

import (
	"sync"
	"time"
)

func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh: // applyCh中的message是经过raft_application提交后需要应用的日志
			if message.CommandValid {
				kv.mu.Lock()

				// 忽略已经处理过的消息
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				kv.lastApplied = message.CommandIndex

				// 取出用户的操作信息
				cmd := message.Command.(RaftCommand)
				var opReply *OpReply

				if cmd.CmdType == ClientOperation {
					op := cmd.Data.(Op)

					// message可能是乱序的。为了保持线性一致性需要在日志应用时判断同一客户端的操作是顺序的
					if op.OpType != OpGet && kv.isRequestDuplicate(op.ClientId, op.SeqId) {
						opReply = kv.duplicateTable[op.ClientId].Reply
					} else {
						// 将操作应用到状态机中
						opReply = kv.applyToStateMachine(op)

						// 更新duplicateTable
						if op.OpType != OpGet {
							kv.duplicateTable[op.ClientId] = LastOperationInfo{
								SeqId: op.SeqId,
								Reply: opReply,
							}
						}
					}
				} else {
					opReply = kv.handleConfigChange(cmd)
				}

				// 重要！！
				// 由Leader收集reply，通过RPC返回给客户端
				if _, isLeader := kv.rf.GetState(); isLeader {
					// 构造reply channel，发送opReply
					kv.getNotifyChannel(message.CommandIndex) <- opReply
				}

				// 判断当前server是否需要snapshot（server本地日志大小是否超过阈值）
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					kv.makeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()

			} else if message.SnapshotValid { // message是Snapshot类型，需要恢复状态机状态
				kv.mu.Lock()
				kv.restoreFromSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// shardkv 需要定时从 shardctrler 这边拉取最新的配置，然后根据配置来确定哪些 shard 应该是需要进行迁移的
// fetchConfigTask 定期从 shardctrler 拉取配置，拿到配置后构造一个配置变更的命令，传入到 raft 模块中进行状态同步。
func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		kv.mu.Lock()
		// 每次只能够拉取一个配置，并且按照顺序处理
		// 主要是为了避免覆盖还未完成的配置变更任务
		newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
		kv.mu.Unlock()

		// 将新配置传入shardCtrler的raft模块进行同步
		kv.ConfigCommand(RaftCommand{
			CmdType: ConfigChange,
			Data:    newConfig,
		}, &OpReply{})

		time.Sleep(FetchConfigInterval)
	}
}

func (kv *ShardKV) shardMigrationTask() {
	for !kv.killed() {
		// 由Leader进行shard迁移
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()

			// 寻找需要迁移进来的gid -> shards
			gidToShards := kv.getShardByStatus(MoveIn)

			// 通过RPC向上述group请求shard状态机数据
			var wg sync.WaitGroup
			for gid, shards := range gidToShards {
				wg.Add(1) // 为什么？
				// 参数：上一个配置的servers，当前配置编号，需要迁移的shard IDs
				go func(servers []string, configNum int, shardIDs []int) {
					defer wg.Done()

					args := ShardOperationArgs{
						ConfigNum: configNum,
						ShardIDs:  shardIDs,
					}
					// 遍历group当中每个节点，从Leader中读取对应的shard状态机数据
					for _, server := range servers {
						srv := kv.make_end(server)
						var reply ShardOperationReply
						ok := srv.Call("ShardKV.GetShardsData", &args, &reply)

						// 获取到shards数据，执行shard迁移
						if ok && reply.Err == OK {
							kv.ConfigCommand(RaftCommand{
								CmdType: ShardMigration,
								Data:    reply,
							}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shards)
			}

			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(ShardMigrationInterval)
	}
}

// shard清理：通过 RPC 修改shard状态 GC -> Normal，MoveOut -> 删除
func (kv *ShardKV) shardGCTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()

			gidToShards := kv.getShardByStatus(GC)

			var wg sync.WaitGroup
			for gid, shards := range gidToShards {
				wg.Add(1) // 为什么？

				go func(servers []string, configNum int, shardIDs []int) {
					defer wg.Done()

					args := ShardOperationArgs{
						ConfigNum: configNum,
						ShardIDs:  shardIDs,
					}

					// Leader向旧group的Leader发送RPC请求，删除旧group中shard数据（在旧group的状态为MoveOut）
					for _, server := range servers {
						var reply ShardOperationReply
						srv := kv.make_end(server)
						ok := srv.Call("ShardKV.DeleteShardsData", &args, &reply)

						if ok && reply.Err == OK {
							// Leader 同步日志，将当前group中处于GC状态的shard变为Normal
							kv.ConfigCommand(RaftCommand{
								CmdType: ShardGC,
								Data:    args,
							}, &OpReply{})
						}
					}

				}(kv.currentConfig.Groups[gid], kv.currentConfig.Num, shards)
			}

			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(ShardGCInterval)
	}
}

// 查找group中对应状态的shard
func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			// 重要！！获取shard原先所属的group
			gid := kv.prevConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}
				gidToShards[gid] = append(gidToShards[gid], i)
			}
		}
	}
	return gidToShards
}

func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断kv当前配置是否是RPC请求所需要的
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// 拷贝状态机数据
	reply.ShardData = make(map[int]map[string]string)
	for _, i := range args.ShardIDs {
		if _, ok := kv.shards[i]; ok {
			reply.ShardData[i] = kv.shards[i].copyKV()
		}
	}

	// 拷贝去重表数据，保证配置变更的线性一致性
	reply.DuplicateTable = make(map[int64]LastOperationInfo)
	for clientId, info := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = info.copyData()
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 如果server当前状态比args中的状态更新，则不需要删除shard
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		return
	}

	// Leader 同步日志，进行状态机删除
	var opReply OpReply
	kv.ConfigCommand(RaftCommand{
		CmdType: ShardGC,
		Data:    *args,
	}, &opReply)

	reply.Err = opReply.Err

}
