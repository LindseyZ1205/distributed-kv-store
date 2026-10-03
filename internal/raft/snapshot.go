package raft

import (
	"time"

	"github.com/LindseyZ1205/distributed-kv-store/internal/wal"
)

// Log compaction (§7). The state machine hands Raft a snapshot of itself
// through some index; Raft saves it, drops the log up to that index, and
// rewrites the WAL to hold only what remains. A follower that has fallen
// behind the leader's snapshot is brought up to date with InstallSnapshot.

// Snapshot tells Raft that data captures the state machine through index,
// so the log up to index is no longer needed.
func (rf *Node) Snapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Ignore stale requests, and never drop entries that are not committed.
	if rf.killed() || index <= rf.log.startIndex || index > rf.commitIndex {
		return
	}
	rf.log.compactTo(index, rf.log.term(index))
	rf.snapshot = data
	rf.persistSnapshot()
}

// persistSnapshot saves the snapshot, then compacts the WAL behind it. In
// that order a crash between the two leaves a snapshot that is ahead of the
// log, which New reconciles. Caller must hold rf.mu.
func (rf *Node) persistSnapshot() {
	index, term := rf.log.startIndex, rf.log.term(rf.log.startIndex)
	rf.must(wal.SaveSnapshot(rf.cfg.DataDir, index, term, rf.snapshot))
	rf.must(rf.wal.Compact(index, term, rf.hardState(), rf.walEntries(index+1)))
}

// HandleInstallSnapshot is the InstallSnapshot RPC handler. The whole
// snapshot travels in one message, so Figure 13's offset and done fields
// are not needed.
func (rf *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return nil, ErrStopped
	}
	reply := &InstallSnapshotReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply, nil
	}
	rf.becomeFollower(args.Term)
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// We already have everything the snapshot covers. Installing it would
	// move the state machine backwards.
	if args.LastIncludedIndex <= rf.commitIndex {
		return reply, nil
	}
	if args.LastIncludedIndex <= rf.log.lastIndex() &&
		rf.log.term(args.LastIncludedIndex) == args.LastIncludedTerm {
		// Our log agrees with the snapshot and extends past it; keep the rest.
		rf.log.compactTo(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		rf.log = makeLog(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persistSnapshot()

	// The applier delivers it, keeping it ordered with command messages.
	rf.pendingSnapshot = true
	rf.applyCond.Signal()
	return reply, nil
}

// Caller must hold rf.mu.
func (rf *Node) handleInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, seq uint64) {
	if rf.killed() {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}
	rf.lastAck[peer] = time.Now()
	if seq > rf.ackSeq[peer] {
		rf.ackSeq[peer] = seq
	}
	if args.LastIncludedIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	rf.advanceCommitIndex()
	rf.checkReads()
}
