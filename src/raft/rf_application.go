package raft

func (rf *Raft) applicationTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.applyCond.Wait() // 释放mu，阻塞当前goroutine，等待其他goroutine通过Signal唤醒，并重新获得mu

		// 临界区中不能有任何阻塞操作
		// 1. 收集上一次应用之后的所有日志条目
		entries := make([]LogEntry, 0)
		for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
			entries = append(entries, rf.log.at(idx))
		}
		rf.mu.Unlock()

		// 逐一构造ApplyMsg进行apply
		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1 + i,
			}
		}

		rf.mu.Lock()

		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries)

		rf.mu.Unlock()
	}
}
