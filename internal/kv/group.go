// Package kv is the replicated key-value state machine. A Group is one
// node's replica of one shard: a Raft replica plus the state machine its
// log drives.
//
// Writes go through the Raft log. Reads do not: they use ReadIndex, which
// confirms leadership with a round of heartbeats and then reads the local
// state once it has caught up to the commit index, so they are still
// linearizable.
package kv

import (
	"context"
	"errors"
	"log"

	"github.com/LindseyZ1205/distributed-kv-store/internal/raft"
)

var (
	// ErrWrongLeader means this replica cannot complete the request; the
	// client should try the leader. For a write it also covers the case
	// where the proposed entry was replaced by a new leader's log, so the
	// write's outcome is unknown and the client must retry it.
	ErrWrongLeader = errors.New("kv: not the leader of this shard")
	// ErrTimeout means the request did not finish in time. A write may or
	// may not have taken effect.
	ErrTimeout = errors.New("kv: timed out")
)

// Group is one replica of one shard.
type Group struct {
	id            int
	raft          *raft.Node
	sm            *stateMachine
	applyCh       chan raft.ApplyMsg
	snapshotEvery int
	stopCh        chan struct{}
	stopped       chan struct{}
}

// NewGroup starts a replica of shard id. It snapshots the state machine
// every snapshotEvery applied entries (never if snapshotEvery <= 0).
// cfg.ApplyCh is set by NewGroup.
func NewGroup(id int, cfg raft.Config, snapshotEvery int) (*Group, error) {
	applyCh := make(chan raft.ApplyMsg, 256)
	cfg.ApplyCh = applyCh
	node, err := raft.New(cfg)
	if err != nil {
		return nil, err
	}
	g := &Group{
		id:            id,
		raft:          node,
		sm:            newStateMachine(),
		applyCh:       applyCh,
		snapshotEvery: snapshotEvery,
		stopCh:        make(chan struct{}),
		stopped:       make(chan struct{}),
	}
	go g.applyLoop()
	return g, nil
}

// Raft returns the group's Raft replica.
func (g *Group) Raft() *raft.Node { return g.raft }

// Leader returns the node this replica believes leads the shard, or -1.
func (g *Group) Leader() int { return g.raft.Leader() }

// Write proposes a put or delete and waits until it is applied.
func (g *Group) Write(ctx context.Context, cmd Command) error {
	data := cmd.encode()

	// Hold sm.mu across Start and the waiter registration, so the entry
	// cannot be applied before anyone is waiting for it.
	g.sm.mu.Lock()
	index, term, ok := g.raft.Start(data)
	if !ok {
		g.sm.mu.Unlock()
		return ErrWrongLeader
	}
	w := &waiter{term: term, done: make(chan error, 1)}
	if old := g.sm.waiters[index]; old != nil {
		// An earlier proposal at this index lost its place in the log.
		old.done <- ErrWrongLeader
	}
	g.sm.waiters[index] = w
	g.sm.mu.Unlock()

	select {
	case err := <-w.done:
		return err
	case <-ctx.Done():
	case <-g.stopCh:
	}
	g.sm.mu.Lock()
	if g.sm.waiters[index] == w {
		delete(g.sm.waiters, index)
	}
	g.sm.mu.Unlock()
	return ErrTimeout
}

// Get returns the value of key with linearizable semantics.
func (g *Group) Get(ctx context.Context, key string) ([]byte, bool, error) {
	index, err := g.raft.ReadIndex(ctx)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrStopped) {
			return nil, false, ErrWrongLeader
		}
		return nil, false, ErrTimeout
	}
	if err := g.sm.waitApplied(ctx, index); err != nil {
		return nil, false, ErrTimeout
	}
	g.sm.mu.Lock()
	defer g.sm.mu.Unlock()
	v, ok := g.sm.data[key]
	return v, ok, nil
}

// Status describes this replica of the shard.
type Status struct {
	Shard        int
	Raft         raft.Status
	AppliedIndex int
	Keys         int
	Digest       uint64
}

func (g *Group) Status() Status {
	rs := g.raft.Status()
	g.sm.mu.Lock()
	defer g.sm.mu.Unlock()
	return Status{
		Shard:        g.id,
		Raft:         rs,
		AppliedIndex: g.sm.applied,
		Keys:         len(g.sm.data),
		Digest:       g.sm.digest(),
	}
}

// Stop shuts the replica down.
func (g *Group) Stop() {
	g.raft.Stop()
	close(g.stopCh)
	<-g.stopped
}

func (g *Group) applyLoop() {
	defer close(g.stopped)
	for {
		select {
		case msg := <-g.applyCh:
			g.apply(msg)
		case <-g.stopCh:
			return
		}
	}
}

func (g *Group) apply(msg raft.ApplyMsg) {
	sm := g.sm
	sm.mu.Lock()
	switch {
	case msg.SnapshotValid:
		if msg.SnapshotIndex <= sm.applied {
			sm.mu.Unlock()
			return
		}
		if err := sm.restore(msg.Snapshot); err != nil {
			log.Fatalf("kv: shard %d: cannot restore snapshot at index %d: %v", g.id, msg.SnapshotIndex, err)
		}
		sm.lastSnap = msg.SnapshotIndex
		// Writes waiting at or below the snapshot were either applied by
		// someone else's log or replaced; their outcome here is unknown.
		sm.failWaiters(msg.SnapshotIndex, ErrWrongLeader)
		sm.advance(msg.SnapshotIndex)
		sm.mu.Unlock()
		return

	case msg.CommandValid:
		if msg.CommandIndex <= sm.applied {
			sm.mu.Unlock()
			return // already covered by a snapshot
		}
		if len(msg.Command) > 0 { // empty: a new leader's no-op
			cmd, err := decodeCommand(msg.Command)
			if err != nil {
				log.Fatalf("kv: shard %d: entry %d: %v", g.id, msg.CommandIndex, err)
			}
			sm.applyCommand(cmd)
		}
		if w := sm.waiters[msg.CommandIndex]; w != nil {
			delete(sm.waiters, msg.CommandIndex)
			if w.term == msg.CommandTerm {
				w.done <- nil
			} else {
				w.done <- ErrWrongLeader
			}
		}
		sm.advance(msg.CommandIndex)

	default:
		sm.mu.Unlock()
		return
	}

	var snap []byte
	index := sm.applied
	if g.snapshotEvery > 0 && sm.applied-sm.lastSnap >= g.snapshotEvery {
		snap = sm.snapshot()
		sm.lastSnap = sm.applied
	}
	sm.mu.Unlock()
	if snap != nil {
		g.raft.Snapshot(index, snap)
	}
}
