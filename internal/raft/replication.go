package raft

import (
	"context"
	"sort"
	"time"
)

// Log replication (§5.3) and commitment (§5.4).
//
// The leader runs one replicator goroutine per follower. Each keeps at most
// one AppendEntries in flight and sends the next one as soon as the reply
// arrives if there is anything new, otherwise on the heartbeat interval.
// Proposals that arrive while an RPC is outstanding are batched into the
// next one, which is where most of the throughput under load comes from.

// HandleAppendEntries is the AppendEntries RPC handler.
func (rf *Node) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return nil, ErrStopped
	}
	reply := &AppendEntriesReply{Term: rf.currentTerm}
	if args.Term < rf.currentTerm {
		return reply, nil
	}
	// A leader exists for args.Term; a candidate in that term steps down.
	rf.becomeFollower(args.Term)
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// args is shared with the sender in tests, so work on copies.
	prevIndex, prevTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries

	// Entries inside our snapshot are committed, so they match the
	// leader's by definition. Skip them.
	if prevIndex < rf.log.startIndex {
		skip := rf.log.startIndex - prevIndex
		if skip > len(entries) {
			skip = len(entries)
		}
		entries = entries[skip:]
		prevIndex = rf.log.startIndex
		prevTerm = rf.log.term(rf.log.startIndex)
	}

	// Consistency check, with fast backup hints on failure.
	if prevIndex > rf.log.lastIndex() {
		reply.XTerm = -1
		reply.XLen = rf.log.lastIndex() + 1
		return reply, nil
	}
	if t := rf.log.term(prevIndex); t != prevTerm {
		reply.XTerm = t
		reply.XIndex = rf.log.firstIndexOfTerm(prevIndex)
		reply.XLen = rf.log.lastIndex() + 1
		return reply, nil
	}

	// Truncate only at the first real conflict. A delayed RPC can carry a
	// prefix of what we already have, and cutting the log back to it would
	// drop entries a newer RPC added.
	for i, e := range entries {
		index := prevIndex + 1 + i
		if index > rf.log.lastIndex() || rf.log.term(index) != e.Term {
			rf.log.truncateFrom(index)
			rf.log.append(entries[i:]...)
			rf.persistEntries(index)
			// The leader counts this reply toward a majority, so the
			// entries must be durable before we send it.
			rf.must(rf.wal.Sync())
			break
		}
	}
	reply.Success = true

	lastNew := prevIndex + len(entries)
	if newCommit := min(args.LeaderCommit, lastNew); newCommit > rf.commitIndex {
		rf.commitIndex = newCommit
		rf.applyCond.Signal()
	}
	return reply, nil
}

// replicator keeps follower peer up to date for as long as this replica
// leads term.
func (rf *Node) replicator(peer, term int, notify <-chan struct{}) {
	heartbeat := time.NewTicker(rf.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	for {
		rf.mu.Lock()
		if rf.killed() || rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		busy := rf.needsReplication(peer)
		rf.mu.Unlock()

		if !busy {
			select {
			case <-notify:
			case <-heartbeat.C:
			case <-rf.stopCh:
				return
			}
		}
		if !rf.replicateOnce(peer, term) {
			// Unreachable. Try again on the next heartbeat instead of
			// spinning.
			select {
			case <-heartbeat.C:
			case <-rf.stopCh:
				return
			}
		}
	}
}

// needsReplication reports whether peer is missing entries, has not heard
// the latest commit index, or has not yet confirmed a pending read.
// Caller must hold rf.mu.
func (rf *Node) needsReplication(peer int) bool {
	return rf.nextIndex[peer] <= rf.log.lastIndex() ||
		rf.sentCommit[peer] < rf.commitIndex ||
		(len(rf.reads) > 0 && rf.ackSeq[peer] < rf.readSeq)
}

// replicateOnce sends peer one AppendEntries, or InstallSnapshot if the
// entries it needs were compacted away, and processes the reply. It
// returns false if the RPC failed.
func (rf *Node) replicateOnce(peer, term int) bool {
	rf.mu.Lock()
	if rf.killed() || rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return true
	}
	// Any reply to an RPC sent from here on confirms reads registered so far.
	seq := rf.readSeq

	if rf.nextIndex[peer] <= rf.log.startIndex {
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.log.startIndex,
			LastIncludedTerm:  rf.log.term(rf.log.startIndex),
			Data:              rf.snapshot,
		}
		rf.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), snapshotRPCTimeout)
		reply, err := rf.cfg.Transport.InstallSnapshot(ctx, peer, args)
		cancel()
		if err != nil {
			return false
		}
		rf.mu.Lock()
		rf.handleInstallSnapshotReply(peer, args, reply, seq)
		rf.mu.Unlock()
		return true
	}

	next := rf.nextIndex[peer]
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.log.term(next - 1),
		Entries:      rf.log.from(next, rf.cfg.MaxBatch),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	reply, err := rf.cfg.Transport.AppendEntries(ctx, peer, args)
	cancel()
	if err != nil {
		return false
	}
	rf.mu.Lock()
	rf.handleAppendEntriesReply(peer, args, reply, seq)
	rf.mu.Unlock()
	return true
}

// Caller must hold rf.mu.
func (rf *Node) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply, seq uint64) {
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
	// The follower answered in our term, so it still accepts us as leader.
	rf.lastAck[peer] = time.Now()
	if seq > rf.ackSeq[peer] {
		rf.ackSeq[peer] = seq
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
		}
		if match+1 > rf.nextIndex[peer] {
			rf.nextIndex[peer] = match + 1
		}
		if args.LeaderCommit > rf.sentCommit[peer] {
			rf.sentCommit[peer] = args.LeaderCommit
		}
		rf.advanceCommitIndex()
	} else {
		// Fast backup: skip a whole term per round trip.
		if reply.XTerm == -1 {
			rf.nextIndex[peer] = reply.XLen
		} else if last := rf.log.lastIndexOfTerm(reply.XTerm); last != -1 {
			rf.nextIndex[peer] = last + 1
		} else {
			rf.nextIndex[peer] = reply.XIndex
		}
		if rf.nextIndex[peer] <= rf.matchIndex[peer] {
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		}
		if rf.nextIndex[peer] > rf.log.lastIndex()+1 {
			rf.nextIndex[peer] = rf.log.lastIndex() + 1
		}
		if rf.nextIndex[peer] < 1 {
			rf.nextIndex[peer] = 1
		}
	}
	rf.checkReads()
}

// advanceCommitIndex commits the highest index stored on a majority, if
// that entry is from the current term. Entries from earlier terms commit
// along with it (Figure 8). Caller must hold rf.mu.
func (rf *Node) advanceCommitIndex() {
	match := append([]int(nil), rf.matchIndex...)
	sort.Sort(sort.Reverse(sort.IntSlice(match)))
	n := match[rf.n/2] // stored on at least a majority
	if n <= rf.commitIndex || rf.log.term(n) != rf.currentTerm {
		return
	}
	rf.commitIndex = n
	rf.applyCond.Signal()
	rf.notifyAll() // followers learn the new commit index sooner
	rf.checkReads()
}

// checkQuorum steps down if no majority has answered within the maximum
// election timeout: a new leader may already exist on the other side of a
// partition, and clients should stop waiting on this one. Caller must hold
// rf.mu.
func (rf *Node) checkQuorum() {
	if rf.n == 1 {
		return
	}
	now := time.Now()
	reachable := 1
	for p := 0; p < rf.n; p++ {
		if p != rf.me && now.Sub(rf.lastAck[p]) < rf.cfg.ElectionTimeoutMax {
			reachable++
		}
	}
	if reachable <= rf.n/2 {
		rf.stepDown()
	}
}

// syncLoop fsyncs the leader's log in the background. Every proposal that
// arrived before an fsync starts is covered by it, so under load one
// fsync serves many proposals. Only then does the leader count its own
// copy toward a majority.
func (rf *Node) syncLoop() {
	for {
		select {
		case <-rf.syncCh:
		case <-rf.stopCh:
			return
		}
		rf.mu.Lock()
		term, last := rf.currentTerm, rf.log.lastIndex()
		rf.mu.Unlock()

		err := rf.wal.Sync()

		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.must(err)
		if rf.role == Leader && rf.currentTerm == term && last > rf.matchIndex[rf.me] {
			rf.matchIndex[rf.me] = last
			rf.advanceCommitIndex()
		}
		rf.mu.Unlock()
	}
}

// Caller must hold rf.mu.
func (rf *Node) signalSync() {
	select {
	case rf.syncCh <- struct{}{}:
	default:
	}
}

// notifyAll wakes every replicator. Caller must hold rf.mu.
func (rf *Node) notifyAll() {
	for p, ch := range rf.notify {
		if p == rf.me || ch == nil {
			continue
		}
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
