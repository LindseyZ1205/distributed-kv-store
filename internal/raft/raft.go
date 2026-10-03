// Package raft implements the Raft consensus algorithm (Ongaro and
// Ousterhout, "In Search of an Understandable Consensus Algorithm") for one
// replica of one shard group.
//
// The core started as a solution to MIT 6.5840 Lab 3 and was adapted to run
// as a real service:
//
//   - RPCs go through a Transport: gRPC between processes, an in-memory
//     network with fault injection in tests.
//   - State is made durable with a write-ahead log (package wal) instead of
//     re-encoding all of it on every change.
//   - Every follower has its own replication loop with one AppendEntries in
//     flight, so entries that arrive while an RPC is outstanding go out
//     together in the next one.
//   - The leader fsyncs its own log in the background (group commit) and
//     counts itself toward a majority only once that fsync completes.
//   - Linearizable reads use ReadIndex instead of going through the log,
//     and a leader that cannot reach a majority steps down (CheckQuorum).
//
// Files:
//
//	raft.go         state, lifecycle, timers, persistence helpers
//	election.go     RequestVote and leader election
//	replication.go  AppendEntries, per-follower replication, commitment
//	readindex.go    ReadIndex
//	snapshot.go     Snapshot and InstallSnapshot
//	apply.go        delivery of committed entries to the state machine
//	log.go          index arithmetic over the in-memory log
package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LindseyZ1205/distributed-kv-store/internal/wal"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		return "leader"
	}
}

var (
	// ErrNotLeader means this replica cannot serve the request because it
	// is not, or is no longer sure it is, the leader.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrStopped is returned after Stop.
	ErrStopped = errors.New("raft: stopped")
)

// ApplyMsg is delivered on Config.ApplyCh for every committed entry, in log
// order, and for a snapshot installed from the leader or loaded at startup.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte // empty for the no-op a new leader appends
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Config configures one replica.
type Config struct {
	ID        int // this replica's number, in [0, Peers)
	Peers     int // size of the group
	Transport Transport
	DataDir   string // WAL and snapshot directory, owned by this replica
	ApplyCh   chan<- ApplyMsg

	HeartbeatInterval  time.Duration // default 50ms
	ElectionTimeoutMin time.Duration // default 300ms
	ElectionTimeoutMax time.Duration // default 2 * ElectionTimeoutMin
	// PreferLeader halves this replica's election timeout so it usually
	// wins when no leader exists. The KV service sets it on a different
	// node for each shard group to spread leadership across nodes.
	PreferLeader bool
	// MaxBatch caps the entries per AppendEntries. Default 1024.
	MaxBatch int
	// Logf, if set, receives leadership changes.
	Logf func(format string, args ...any)
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 1024
	}
}

const (
	noVote             = -1
	tickInterval       = 10 * time.Millisecond
	rpcTimeout         = 500 * time.Millisecond
	snapshotRPCTimeout = 10 * time.Second
)

// Status is a point-in-time view of a replica, for monitoring.
type Status struct {
	Role          Role
	Term          int
	Leader        int // -1 if unknown
	CommitIndex   int
	AppliedIndex  int
	LastIndex     int
	SnapshotIndex int
}

// Node is one Raft replica.
type Node struct {
	mu     sync.Mutex
	cfg    Config
	me     int
	n      int
	wal    *wal.WAL
	dead   int32
	stopCh chan struct{}

	// Persistent state, written to the WAL before it is acted on.
	currentTerm int
	votedFor    int
	log         raftLog

	// Volatile state.
	role             Role
	leaderID         int
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time

	// Leader state, reset on every election win.
	nextIndex  []int
	matchIndex []int // matchIndex[me] counts only entries this node has fsynced
	sentCommit []int // commit index each follower has acknowledged receiving
	lastAck    []time.Time
	ackSeq     []uint64 // highest read sequence number each follower has acknowledged
	readSeq    uint64
	reads      []*readWaiter
	notify     []chan struct{} // wakes each follower's replicator

	snapshot        []byte // state machine snapshot through log.startIndex
	pendingSnapshot bool   // snapshot not yet delivered on ApplyCh

	applyCond *sync.Cond    // signalled when commitIndex moves or a snapshot arrives
	syncCh    chan struct{} // wakes the leader's background fsync
}

// New starts a replica, recovering its state from cfg.DataDir.
func New(cfg Config) (*Node, error) {
	cfg.setDefaults()
	if cfg.Peers < 1 || cfg.ID < 0 || cfg.ID >= cfg.Peers {
		return nil, fmt.Errorf("raft: replica %d of %d", cfg.ID, cfg.Peers)
	}
	w, st, err := wal.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	snapIndex, snapTerm, snapData, err := wal.LoadSnapshot(cfg.DataDir)
	if err != nil {
		w.Close()
		return nil, err
	}

	rf := &Node{
		cfg:        cfg,
		me:         cfg.ID,
		n:          cfg.Peers,
		wal:        w,
		stopCh:     make(chan struct{}),
		votedFor:   noVote,
		leaderID:   -1,
		nextIndex:  make([]int, cfg.Peers),
		matchIndex: make([]int, cfg.Peers),
		sentCommit: make([]int, cfg.Peers),
		lastAck:    make([]time.Time, cfg.Peers),
		ackSeq:     make([]uint64, cfg.Peers),
		notify:     make([]chan struct{}, cfg.Peers),
		syncCh:     make(chan struct{}, 1),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	if st.HasHardState {
		rf.currentTerm, rf.votedFor = st.HardState.Term, st.HardState.Vote
	}
	rf.log = makeLog(st.SnapIndex, st.SnapTerm)
	for _, e := range st.Entries {
		rf.log.append(LogEntry{Term: e.Term, Data: e.Data})
	}

	// Snapshot() saves the snapshot file before it compacts the log, so a
	// crash in between leaves the snapshot ahead of the log's own marker.
	switch {
	case snapIndex > rf.log.startIndex:
		if snapIndex <= rf.log.lastIndex() && rf.log.term(snapIndex) == snapTerm {
			rf.log.compactTo(snapIndex, snapTerm)
		} else {
			rf.log = makeLog(snapIndex, snapTerm)
		}
		if err := w.Compact(snapIndex, snapTerm, rf.hardState(), rf.walEntries(snapIndex+1)); err != nil {
			w.Close()
			return nil, err
		}
	case snapIndex < rf.log.startIndex:
		w.Close()
		return nil, fmt.Errorf("raft: log starts after entry %d but the snapshot only covers %d",
			rf.log.startIndex, snapIndex)
	}
	rf.snapshot = snapData
	rf.commitIndex = rf.log.startIndex
	rf.lastApplied = rf.log.startIndex
	// The state machine starts from the snapshot, before any entries.
	rf.pendingSnapshot = rf.log.startIndex > 0
	rf.resetElectionTimer()

	go rf.ticker()
	go rf.applier()
	go rf.syncLoop()
	return rf, nil
}

// GetState returns the current term and whether this replica believes it
// is the leader.
func (rf *Node) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Leader returns the replica this one believes is leader, or -1.
func (rf *Node) Leader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderID
}

// Status returns a snapshot of this replica's state.
func (rf *Node) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Status{
		Role:          rf.role,
		Term:          rf.currentTerm,
		Leader:        rf.leaderID,
		CommitIndex:   rf.commitIndex,
		AppliedIndex:  rf.lastApplied,
		LastIndex:     rf.log.lastIndex(),
		SnapshotIndex: rf.log.startIndex,
	}
}

// Start proposes command for the log. If this replica is the leader it
// returns the index the command will occupy if it commits, the current
// term, and true. The command is not committed until it shows up on
// ApplyCh at that index with the same term.
func (rf *Node) Start(command []byte) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	rf.log.append(LogEntry{Term: rf.currentTerm, Data: command})
	index := rf.log.lastIndex()
	// Write now, fsync in the background: concurrent proposals share one
	// fsync, and followers can receive the entry in the meantime.
	rf.persistEntries(index)
	rf.signalSync()
	rf.notifyAll()
	return index, rf.currentTerm, true
}

// Stop shuts the replica down. It does not wait for in-flight RPCs.
func (rf *Node) Stop() {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.stopCh)
	rf.failReads(ErrStopped)
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
	rf.wal.Close()
}

func (rf *Node) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// ticker drives elections, and CheckQuorum while leading.
func (rf *Node) ticker() {
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-rf.stopCh:
			return
		}
		rf.mu.Lock()
		if !rf.killed() {
			if rf.role == Leader {
				rf.checkQuorum()
			} else if time.Now().After(rf.electionDeadline) {
				rf.startElection()
			}
		}
		rf.mu.Unlock()
	}
}

// resetElectionTimer picks a new randomized election deadline. Caller must
// hold rf.mu.
func (rf *Node) resetElectionTimer() {
	lo, hi := rf.cfg.ElectionTimeoutMin, rf.cfg.ElectionTimeoutMax
	if rf.cfg.PreferLeader {
		lo, hi = lo/2, lo
	}
	timeout := lo + time.Duration(rand.Int63n(int64(hi-lo)+1))
	rf.electionDeadline = time.Now().Add(timeout)
}

// becomeFollower adopts term if it is newer and steps down to follower.
// Caller must hold rf.mu.
func (rf *Node) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = noVote
		rf.leaderID = -1
		rf.persistHardState()
	}
	if rf.role == Leader {
		rf.failReads(ErrNotLeader)
	}
	rf.role = Follower
}

// becomeLeader takes over for the current term. Caller must hold rf.mu.
func (rf *Node) becomeLeader() {
	rf.logf("became leader in term %d", rf.currentTerm)
	rf.role = Leader
	rf.leaderID = rf.me
	// A no-op from the new term lets entries from earlier terms commit
	// (counting replicas alone cannot commit them, Figure 8), and gives
	// ReadIndex a commit point in this term.
	rf.log.append(LogEntry{Term: rf.currentTerm})
	rf.persistEntries(rf.log.lastIndex())

	now := time.Now()
	for p := 0; p < rf.n; p++ {
		rf.nextIndex[p] = rf.log.lastIndex()
		rf.matchIndex[p] = 0
		rf.sentCommit[p] = 0
		rf.lastAck[p] = now
		rf.ackSeq[p] = 0
	}
	rf.readSeq = 0
	rf.reads = nil
	for p := 0; p < rf.n; p++ {
		if p == rf.me {
			continue
		}
		ch := make(chan struct{}, 1)
		rf.notify[p] = ch
		go rf.replicator(p, rf.currentTerm, ch)
	}
	// matchIndex[me] advances once the background fsync covers the log.
	rf.signalSync()
}

// stepDown gives up leadership without a term change. Caller must hold
// rf.mu.
func (rf *Node) stepDown() {
	rf.logf("stepping down in term %d: no contact with a majority", rf.currentTerm)
	rf.role = Follower
	rf.leaderID = -1
	rf.failReads(ErrNotLeader)
	rf.resetElectionTimer()
}

func (rf *Node) hardState() wal.HardState {
	return wal.HardState{Term: rf.currentTerm, Vote: rf.votedFor}
}

// persistHardState makes the term and vote durable. Caller must hold rf.mu
// and must not reply to anyone until this returns.
func (rf *Node) persistHardState() {
	rf.must(rf.wal.SaveHardState(rf.hardState()))
	rf.must(rf.wal.Sync())
}

// persistEntries writes the entries from index from to the end of the log
// to the WAL, without an fsync. Caller must hold rf.mu.
func (rf *Node) persistEntries(from int) {
	rf.must(rf.wal.Append(rf.walEntries(from)))
}

func (rf *Node) walEntries(from int) []wal.Entry {
	if from > rf.log.lastIndex() {
		return nil
	}
	out := make([]wal.Entry, 0, rf.log.lastIndex()-from+1)
	for i := from; i <= rf.log.lastIndex(); i++ {
		e := rf.log.entry(i)
		out = append(out, wal.Entry{Index: i, Term: e.Term, Data: e.Data})
	}
	return out
}

// must stops the process on a storage error. A replica that cannot persist
// must not keep acknowledging writes, and Raft tolerates a crashed replica
// but not one that forgets what it promised.
func (rf *Node) must(err error) {
	if err == nil || errors.Is(err, wal.ErrClosed) {
		return
	}
	log.Fatalf("raft: replica %d: storage failure: %v", rf.me, err)
}

func (rf *Node) logf(format string, args ...any) {
	if rf.cfg.Logf != nil {
		rf.cfg.Logf(format, args...)
	}
}
