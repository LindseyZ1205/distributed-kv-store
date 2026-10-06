// Package client is the Go client for the key-value store.
//
// A Cluster holds one connection per node and the hash ring, and is shared
// by the whole process. A Session is one logical client: it numbers its
// writes so the servers can apply each one exactly once even when the
// client has to retry it at a new leader. A Session runs one operation at a
// time; use one Session per goroutine.
package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/LindseyZ1205/distributed-kv-store/gen/kvpb"
	"github.com/LindseyZ1205/distributed-kv-store/internal/ring"
	"github.com/LindseyZ1205/distributed-kv-store/internal/transport"
)

// Options tune retries. Zero values pick the defaults.
type Options struct {
	// AttemptTimeout bounds a single RPC. Default 1.5s, a little above the
	// server's own one-second limit.
	AttemptTimeout time.Duration
	// RetryDelay is the pause before trying another node when no leader is
	// known. Default 20ms.
	RetryDelay time.Duration
}

// Cluster is a connection to the whole store. It is safe for concurrent
// use.
type Cluster struct {
	opts    Options
	conns   []*grpc.ClientConn
	nodes   []kvpb.KVClient
	ring    *ring.Ring
	leaders []atomic.Int32 // last known leader of each shard
}

// Dial asks the first reachable seed for the topology and connects to
// every node.
func Dial(ctx context.Context, seeds []string, opts Options) (*Cluster, error) {
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = 1500 * time.Millisecond
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 20 * time.Millisecond
	}
	topo, err := topology(ctx, seeds)
	if err != nil {
		return nil, err
	}
	c := &Cluster{
		opts:    opts,
		ring:    ring.NewN(int(topo.Shards), int(topo.Vnodes)),
		leaders: make([]atomic.Int32, topo.Shards),
	}
	for _, addr := range topo.Nodes {
		conn, err := grpc.NewClient(addr, transport.DialOptions()...)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("client: dial %s: %w", addr, err)
		}
		c.conns = append(c.conns, conn)
		c.nodes = append(c.nodes, kvpb.NewKVClient(conn))
	}
	for s := range c.leaders {
		// The servers prefer node s mod n as the leader of shard s.
		c.leaders[s].Store(int32(s % len(c.nodes)))
	}
	return c, nil
}

func topology(ctx context.Context, seeds []string) (*kvpb.TopologyResponse, error) {
	var lastErr error = errors.New("client: no seed addresses")
	for _, addr := range seeds {
		conn, err := grpc.NewClient(addr, transport.DialOptions()...)
		if err != nil {
			lastErr = err
			continue
		}
		tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		topo, err := kvpb.NewKVClient(conn).Topology(tctx, &kvpb.TopologyRequest{})
		cancel()
		conn.Close()
		if err == nil && len(topo.Nodes) > 0 && topo.Shards > 0 {
			return topo, nil
		}
		if err == nil {
			err = errors.New("empty topology")
		}
		lastErr = fmt.Errorf("client: topology from %s: %w", addr, err)
	}
	return nil, lastErr
}

// Close closes every connection.
func (c *Cluster) Close() error {
	for _, conn := range c.conns {
		conn.Close()
	}
	return nil
}

// Nodes returns the number of nodes.
func (c *Cluster) Nodes() int { return len(c.nodes) }

// Node returns the KV client for node i, for tools that talk to one node.
func (c *Cluster) Node(i int) kvpb.KVClient { return c.nodes[i] }

// Session is one logical client. Its methods must not be called
// concurrently.
type Session struct {
	c   *Cluster
	mu  sync.Mutex
	id  uint64
	seq uint64
}

// NewSession starts a session with a random client id.
func (c *Cluster) NewSession() *Session {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			panic(err)
		}
		if id := binary.LittleEndian.Uint64(b[:]); id != 0 {
			return &Session{c: c, id: id}
		}
	}
}

// ID returns the session's client id.
func (s *Session) ID() uint64 { return s.id }

// Get returns the value of key and whether it exists.
func (s *Session) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var value []byte
	var found bool
	err := s.do(ctx, key, func(ctx context.Context, kc kvpb.KVClient) (kvpb.Code, int32, error) {
		resp, err := kc.Get(ctx, &kvpb.GetRequest{Key: key})
		if err != nil {
			return 0, -1, err
		}
		value, found = resp.Value, resp.Found
		return resp.Code, resp.LeaderHint, nil
	})
	return value, found, err
}

// Put sets key to value.
func (s *Session) Put(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	req := &kvpb.PutRequest{Key: key, Value: value, ClientId: s.id, Seq: s.seq}
	return s.do(ctx, key, func(ctx context.Context, kc kvpb.KVClient) (kvpb.Code, int32, error) {
		resp, err := kc.Put(ctx, req)
		if err != nil {
			return 0, -1, err
		}
		return resp.Code, resp.LeaderHint, nil
	})
}

// Delete removes key.
func (s *Session) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	req := &kvpb.DeleteRequest{Key: key, ClientId: s.id, Seq: s.seq}
	return s.do(ctx, key, func(ctx context.Context, kc kvpb.KVClient) (kvpb.Code, int32, error) {
		resp, err := kc.Delete(ctx, req)
		if err != nil {
			return 0, -1, err
		}
		return resp.Code, resp.LeaderHint, nil
	})
}

type attempt func(ctx context.Context, kc kvpb.KVClient) (kvpb.Code, int32, error)

// do runs call against the leader of key's shard until it succeeds or ctx
// ends. Writes are retried with the same sequence number, so a write that
// did take effect before a failure is not applied twice.
func (s *Session) do(ctx context.Context, key string, call attempt) error {
	c := s.c
	shard := c.ring.Lookup(key)
	n := len(c.nodes)
	target := int(c.leaders[shard].Load())
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
		code, hint, err := call(actx, c.nodes[target])
		cancel()

		switch {
		case err == nil && code == kvpb.Code_CODE_OK:
			c.leaders[shard].Store(int32(target))
			return nil
		case err == nil && code == kvpb.Code_CODE_WRONG_LEADER && hint >= 0 && int(hint) < n && int(hint) != target:
			target = int(hint)
			continue
		}
		// The node is unreachable, timed out, or knows no leader yet:
		// pause briefly and try the next one.
		target = (target + 1) % n
		select {
		case <-time.After(c.opts.RetryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
