package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait() // 释放mu，阻塞当前goroutine，等待其他goroutine通过Signal唤醒，并重新获得mu

		snapApply := rf.snapPending

		// 1. apply线程是由日志同步唤醒的
		// 收集所有  的日志（）
		entries := make([]LogEntry, 0)
		if !snapApply {
			// 需要检查[lastAppiled + 1, commitIndex]是否超出tailLog区间，并适当剪裁
			if rf.lastApplied < rf.log.snapLastIdx {
				rf.lastApplied = rf.log.snapLastIdx
			}

			start := rf.lastApplied + 1
			end := rf.commitIndex

			if end >= rf.log.size() {
				end = rf.log.size() - 1
			}

			for idx := start; idx <= end; idx++ {
				entries = append(entries, rf.log.at(idx))
			}
		}
		rf.mu.Unlock()

		// 逐一构造ApplyMsg进行apply
		// 临界区中不能有任何阻塞操作，因此channel不应该在临界区中（三段式）
		if !snapApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + 1 + i,
				}
			}
		} else {
			// 2. apply线程是由快照同步唤醒的
			// 应用snapshot即可，不收集需要应用的日志条目
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.log.snapshot,
				SnapshotTerm:  rf.log.snapLastTerm,
				SnapshotIndex: rf.log.snapLastIdx,
			}
		}

		rf.mu.Lock()
		if !snapApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0, %d]", rf.log.snapLastIdx)
			rf.lastApplied = rf.log.snapLastIdx

			// Raft日志同步是先提交后应用，但snapshot会打乱该逻辑
			// 因此需要对commitIndex和lastApplied的大小进行判断
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			// 重要！！否则将反复安装snapshot
			rf.snapPending = false
		}

		rf.mu.Unlock()
	}
}
