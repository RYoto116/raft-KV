package raft

func (rf *Raft) applicationTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.applyCond.Wait() // 释放mu，等待Signal唤醒重新获得mu

		// 临界区中不能有任何阻塞操作
		// 1. 收集上一次应用之后的所有日志条目
		entries := make([]LogEntry, 0)
		entries = append(entries, rf.log[rf.lastApplied+1:rf.commitIndex+1]...)
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
