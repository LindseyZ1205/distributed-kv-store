package raft

import (
	"context"
	"sort"
)

// ReadIndex (§6.4 of Ongaro's dissertation) serves linearizable reads
// without appending to the log. The leader records its commit index, makes
// sure it is still the leader by hearing from a majority after the read
// arrived, and the read is then served once the state machine has applied
// that index.
//
// Confirmations are batched. Each read takes the next sequence number, each
// RPC carries the sequence number current when it was sent, and a reply in
// the leader's term acknowledges that number for its follower. A read is
// confirmed once a majority, counting the leader, has acknowledged a number
// at least as large as its own, which means they all answered an RPC sent
// after the read arrived. One round of RPCs confirms every read waiting on it.

type readWaiter struct {
	seq  uint64
	done chan readResult
}

type readResult struct {
	index int
	err   error
}

// ReadIndex returns an index such that a read of the state machine is
// linearizable once the state machine has applied that index. It fails
// with ErrNotLeader if this replica is not the leader or loses leadership
// before the read is confirmed.
func (rf *Node) ReadIndex(ctx context.Context) (int, error) {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return 0, ErrStopped
	}
	if rf.role != Leader {
		rf.mu.Unlock()
		return 0, ErrNotLeader
	}
	rf.readSeq++
	w := &readWaiter{seq: rf.readSeq, done: make(chan readResult, 1)}
	rf.reads = append(rf.reads, w)
	rf.checkReads() // a single-node group confirms immediately
	rf.notifyAll()
	rf.mu.Unlock()

	select {
	case r := <-w.done:
		return r.index, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// checkReads completes every waiting read that a majority has confirmed.
// Caller must hold rf.mu.
func (rf *Node) checkReads() {
	if len(rf.reads) == 0 {
		return
	}
	if rf.role != Leader {
		rf.failReads(ErrNotLeader)
		return
	}
	// Until an entry from this term commits, our commit index may be
	// behind entries the previous leader committed.
	if rf.log.term(rf.commitIndex) != rf.currentTerm {
		return
	}
	acked := rf.quorumAckSeq()
	i := 0
	for i < len(rf.reads) && rf.reads[i].seq <= acked {
		rf.reads[i].done <- readResult{index: rf.commitIndex}
		i++
	}
	if i == len(rf.reads) {
		rf.reads = nil
	} else {
		rf.reads = rf.reads[i:]
	}
}

// quorumAckSeq returns the largest read sequence number acknowledged by a
// majority, with the leader acknowledging everything. Caller must hold
// rf.mu.
func (rf *Node) quorumAckSeq() uint64 {
	acks := make([]uint64, rf.n)
	for p := 0; p < rf.n; p++ {
		if p == rf.me {
			acks[p] = rf.readSeq
		} else {
			acks[p] = rf.ackSeq[p]
		}
	}
	sort.Slice(acks, func(i, j int) bool { return acks[i] > acks[j] })
	return acks[rf.n/2]
}

// failReads fails every waiting read. Caller must hold rf.mu.
func (rf *Node) failReads(err error) {
	for _, w := range rf.reads {
		w.done <- readResult{err: err}
	}
	rf.reads = nil
}
