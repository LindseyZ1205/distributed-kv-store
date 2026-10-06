package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"hash/fnv"
	"sort"
	"sync"
)

// stateMachine is one shard's data, rebuilt on every replica by applying
// the shard's Raft log in order.
type stateMachine struct {
	mu       sync.Mutex
	data     map[string][]byte
	sessions map[uint64]uint64 // client id -> highest write sequence applied
	applied  int               // index of the last log entry applied
	lastSnap int               // applied index at the last snapshot

	// appliedCh is closed and replaced every time applied moves, waking
	// reads that wait for a ReadIndex to be applied.
	appliedCh chan struct{}
	// waiters holds writes proposed on this replica, by log index, until
	// that index is applied.
	waiters map[int]*waiter
}

type waiter struct {
	term int
	done chan error
}

func newStateMachine() *stateMachine {
	return &stateMachine{
		data:      make(map[string][]byte),
		sessions:  make(map[uint64]uint64),
		appliedCh: make(chan struct{}),
		waiters:   make(map[int]*waiter),
	}
}

// applyCommand applies a write unless the client's session shows it was
// already applied. Caller must hold sm.mu.
func (sm *stateMachine) applyCommand(c Command) {
	if c.Seq <= sm.sessions[c.ClientID] {
		return // a retry of a write that already took effect
	}
	switch c.Op {
	case OpPut:
		sm.data[c.Key] = c.Value
	case OpDelete:
		delete(sm.data, c.Key)
	}
	sm.sessions[c.ClientID] = c.Seq
}

// advance records that index has been applied. Caller must hold sm.mu.
func (sm *stateMachine) advance(index int) {
	sm.applied = index
	close(sm.appliedCh)
	sm.appliedCh = make(chan struct{})
}

// waitApplied blocks until the state machine has applied index.
func (sm *stateMachine) waitApplied(ctx context.Context, index int) error {
	for {
		sm.mu.Lock()
		if sm.applied >= index {
			sm.mu.Unlock()
			return nil
		}
		ch := sm.appliedCh
		sm.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// failWaiters resolves every waiting write at or below index with err.
// Caller must hold sm.mu.
func (sm *stateMachine) failWaiters(index int, err error) {
	for i, w := range sm.waiters {
		if i <= index {
			w.done <- err
			delete(sm.waiters, i)
		}
	}
}

type snapshotState struct {
	Data     map[string][]byte
	Sessions map[uint64]uint64
}

// snapshot encodes the data and the client sessions. The sessions must
// travel with the data: without them a replica restored from a snapshot
// would apply a retried write a second time. Caller must hold sm.mu.
func (sm *stateMachine) snapshot() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshotState{Data: sm.data, Sessions: sm.sessions}); err != nil {
		panic(err) // maps of strings, bytes and integers always encode
	}
	return buf.Bytes()
}

// restore replaces the state with a snapshot. Caller must hold sm.mu.
func (sm *stateMachine) restore(snap []byte) error {
	var st snapshotState
	if err := gob.NewDecoder(bytes.NewReader(snap)).Decode(&st); err != nil {
		return err
	}
	if st.Data == nil {
		st.Data = make(map[string][]byte)
	}
	if st.Sessions == nil {
		st.Sessions = make(map[uint64]uint64)
	}
	sm.data, sm.sessions = st.Data, st.Sessions
	return nil
}

// digest hashes the data in key order. Caller must hold sm.mu.
func (sm *stateMachine) digest() uint64 {
	keys := make([]string, 0, len(sm.data))
	for k := range sm.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	var lenBuf [binary.MaxVarintLen64]byte
	for _, k := range keys {
		v := sm.data[k]
		h.Write(lenBuf[:binary.PutUvarint(lenBuf[:], uint64(len(k)))])
		h.Write([]byte(k))
		h.Write(lenBuf[:binary.PutUvarint(lenBuf[:], uint64(len(v)))])
		h.Write(v)
	}
	return h.Sum64()
}
