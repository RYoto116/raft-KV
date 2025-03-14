package shardkv

import "time"

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

// 获取当前配置。shardkv 需要定时从 shardctrler 这边拉取最新的配置，然后根据配置来确定哪些 shard 应该是需要进行迁移的
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
