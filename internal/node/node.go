// Package node assembles a storage node: one Raft replica and key-value
// state machine per shard, the transport between nodes, and the gRPC
// server that exposes both the Raft and the client-facing KV services.
//
// Every node hosts a replica of every shard. The consistent-hash ring
// decides which shard owns a key; each shard is its own Raft group with its
// own leader, so leadership, and with it the write load, is spread across
// the nodes.
package node

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"github.com/LindseyZ1205/distributed-kv-store/gen/kvpb"
	"github.com/LindseyZ1205/distributed-kv-store/gen/raftpb"
	"github.com/LindseyZ1205/distributed-kv-store/internal/kv"
	"github.com/LindseyZ1205/distributed-kv-store/internal/raft"
	"github.com/LindseyZ1205/distributed-kv-store/internal/ring"
	"github.com/LindseyZ1205/distributed-kv-store/internal/transport"
)

// Config configures a node.
type Config struct {
	ID      int
	Peers   []string // address of every node, indexed by node id
	DataDir string

	Shards        int
	VNodes        int // ring points per shard; default ring.DefaultVNodes
	SnapshotEvery int // applied entries between snapshots; default 10000

	// Raft timing. Zero values use the raft package defaults.
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// Logf, if set, receives leadership changes.
	Logf func(format string, args ...any)
}

// Node is a running storage node.
type Node struct {
	cfg    Config
	ring   *ring.Ring
	groups []*kv.Group
	peers  *transport.Peers
	server *grpc.Server
}

// Start starts a node that serves on lis.
func Start(cfg Config, lis net.Listener) (*Node, error) {
	if len(cfg.Peers) == 0 || cfg.ID < 0 || cfg.ID >= len(cfg.Peers) {
		return nil, fmt.Errorf("node: id %d is not in a cluster of %d", cfg.ID, len(cfg.Peers))
	}
	if cfg.Shards < 1 {
		return nil, errors.New("node: need at least one shard")
	}
	if cfg.VNodes <= 0 {
		cfg.VNodes = ring.DefaultVNodes
	}
	if cfg.SnapshotEvery <= 0 {
		cfg.SnapshotEvery = 10000
	}
	peers, err := transport.Dial(cfg.ID, cfg.Peers)
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, ring: ring.NewN(cfg.Shards, cfg.VNodes), peers: peers}
	for s := 0; s < cfg.Shards; s++ {
		g, err := kv.NewGroup(s, raft.Config{
			ID:                 cfg.ID,
			Peers:              len(cfg.Peers),
			Transport:          peers.Group(s),
			DataDir:            filepath.Join(cfg.DataDir, fmt.Sprintf("shard-%d", s)),
			PreferLeader:       s%len(cfg.Peers) == cfg.ID,
			HeartbeatInterval:  cfg.HeartbeatInterval,
			ElectionTimeoutMin: cfg.ElectionTimeoutMin,
			ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			Logf:               shardLogf(cfg.Logf, s),
		}, cfg.SnapshotEvery)
		if err != nil {
			n.stopGroups()
			peers.Close()
			return nil, fmt.Errorf("node: shard %d: %w", s, err)
		}
		n.groups = append(n.groups, g)
	}

	n.server = grpc.NewServer(transport.ServerOptions()...)
	raftpb.RegisterRaftServer(n.server, transport.NewServer(n.replica))
	kvpb.RegisterKVServer(n.server, &kvService{n: n})
	go n.server.Serve(lis)
	return n, nil
}

func shardLogf(logf func(string, ...any), shard int) func(string, ...any) {
	if logf == nil {
		return nil
	}
	return func(format string, args ...any) {
		logf("shard %d: "+format, append([]any{shard}, args...)...)
	}
}

// Stop shuts the node down. Its data directory is left for a restart.
func (n *Node) Stop() {
	n.server.Stop()
	n.stopGroups()
	n.peers.Close()
}

func (n *Node) stopGroups() {
	for _, g := range n.groups {
		g.Stop()
	}
}

// ID returns the node's id.
func (n *Node) ID() int { return n.cfg.ID }

// Peers returns the node's connections to the other nodes.
func (n *Node) Peers() *transport.Peers { return n.peers }

// Groups returns the node's shard replicas, indexed by shard.
func (n *Node) Groups() []*kv.Group { return n.groups }

func (n *Node) replica(group int) *raft.Node {
	if group < 0 || group >= len(n.groups) {
		return nil
	}
	return n.groups[group].Raft()
}

func (n *Node) groupFor(key string) *kv.Group {
	return n.groups[n.ring.Lookup(key)]
}
