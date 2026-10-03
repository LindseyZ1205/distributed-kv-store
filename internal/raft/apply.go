package raft

// applier is the only goroutine that sends on ApplyCh, which keeps
// messages in log order. It waits until commitIndex passes lastApplied or a
// snapshot arrives, then delivers without holding rf.mu, because the state
// machine calls back into Raft (Snapshot) while it processes messages.
func (rf *Node) applier() {
	for {
		rf.mu.Lock()
		for !rf.killed() && !rf.pendingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.pendingSnapshot {
			// InstallSnapshot only accepts snapshots past commitIndex, so
			// every command already delivered precedes this snapshot.
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotIndex: rf.log.startIndex,
				SnapshotTerm:  rf.log.term(rf.log.startIndex),
			}
			rf.pendingSnapshot = false
			rf.mu.Unlock()
			if !rf.deliver(msg) {
				return
			}
			continue
		}

		first, last := rf.lastApplied+1, rf.commitIndex
		msgs := make([]ApplyMsg, 0, last-first+1)
		for index := first; index <= last; index++ {
			e := rf.log.entry(index)
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      e.Data,
				CommandIndex: index,
				CommandTerm:  e.Term,
			})
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			if !rf.deliver(msg) {
				return
			}
		}

		rf.mu.Lock()
		if last > rf.lastApplied {
			rf.lastApplied = last
		}
		rf.mu.Unlock()
	}
}

// deliver sends msg on ApplyCh unless the replica stops first.
func (rf *Node) deliver(msg ApplyMsg) bool {
	select {
	case rf.cfg.ApplyCh <- msg:
		return true
	case <-rf.stopCh:
		return false
	}
}
