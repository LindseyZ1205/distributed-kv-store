package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// memNet is an in-memory network between the replicas of one test group.
// It can isolate replicas and drop or delay messages.
type memNet struct {
	mu       sync.Mutex
	nodes    []*Node
	up       []bool // false: cut off from every other replica
	dropRate float64
	maxDelay time.Duration
	rng      *rand.Rand
}

var errUnreachable = errors.New("memnet: unreachable")

func newMemNet(n int) *memNet {
	up := make([]bool, n)
	for i := range up {
		up[i] = true
	}
	return &memNet{nodes: make([]*Node, n), up: up, rng: rand.New(rand.NewSource(1))}
}

func (m *memNet) setNode(i int, nd *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[i] = nd
}

func (m *memNet) isolate(i int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.up[i] = false
}

func (m *memNet) connect(i int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.up[i] = true
}

func (m *memNet) isUp(i int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.up[i]
}

func (m *memNet) setUnreliable(dropRate float64, maxDelay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropRate, m.maxDelay = dropRate, maxDelay
}

// route decides whether a message from a reaches b, and after what delay.
func (m *memNet) route(a, b int) (*Node, time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.up[a] || !m.up[b] || m.nodes[b] == nil {
		return nil, 0, false
	}
	if m.dropRate > 0 && m.rng.Float64() < m.dropRate {
		return nil, 0, false
	}
	var d time.Duration
	if m.maxDelay > 0 {
		d = time.Duration(m.rng.Int63n(int64(m.maxDelay)))
	}
	return m.nodes[b], d, true
}

// replyArrives decides whether b's reply makes it back to a.
func (m *memNet) replyArrives(a, b int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.up[a] || !m.up[b] {
		return false
	}
	return m.dropRate == 0 || m.rng.Float64() >= m.dropRate
}

type memTransport struct {
	net  *memNet
	from int
}

func call[R any](ctx context.Context, t *memTransport, peer int, handle func(*Node) (R, error)) (R, error) {
	var zero R
	node, delay, ok := t.net.route(t.from, peer)
	if !ok {
		return zero, errUnreachable
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
	reply, err := handle(node)
	if err != nil {
		return zero, err
	}
	if !t.net.replyArrives(t.from, peer) {
		return zero, errUnreachable
	}
	return reply, nil
}

func (t *memTransport) RequestVote(ctx context.Context, peer int, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return call(ctx, t, peer, func(n *Node) (*RequestVoteReply, error) { return n.HandleRequestVote(args) })
}

func (t *memTransport) AppendEntries(ctx context.Context, peer int, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return call(ctx, t, peer, func(n *Node) (*AppendEntriesReply, error) { return n.HandleAppendEntries(args) })
}

func (t *memTransport) InstallSnapshot(ctx context.Context, peer int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return call(ctx, t, peer, func(n *Node) (*InstallSnapshotReply, error) { return n.HandleInstallSnapshot(args) })
}
