// Package transport carries Raft RPCs between nodes over gRPC.
//
// Each node keeps one client connection to every other node. All shard
// groups share it: requests name their group, and the receiving node's
// Server hands each one to its replica of that group.
package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/LindseyZ1205/distributed-kv-store/gen/raftpb"
	"github.com/LindseyZ1205/distributed-kv-store/internal/raft"
)

// MaxMessageSize bounds gRPC messages; a snapshot travels in one message.
const MaxMessageSize = 64 << 20

// ErrBlocked is returned for RPCs to a peer cut off with SetBlocked.
var ErrBlocked = errors.New("transport: peer blocked")

// DialOptions are used for every connection, between nodes and from
// clients.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageSize),
			grpc.MaxCallSendMsgSize(MaxMessageSize),
		),
		// Reconnect quickly after a peer restarts. The default backoff
		// grows to two minutes, far longer than a failover.
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  50 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second,
			},
			MinConnectTimeout: time.Second,
		}),
		// Notice a peer that disappeared without closing the connection,
		// as happens in a network partition. 10s is the minimum gRPC allows.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}

// ServerOptions match DialOptions on the server side.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMessageSize),
		grpc.MaxSendMsgSize(MaxMessageSize),
		// Accept the clients' keepalive pings instead of closing the
		// connection for pinging too often.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
}

// Peers holds a connection to every other node.
type Peers struct {
	conns   []*grpc.ClientConn
	clients []raftpb.RaftClient

	mu      sync.RWMutex
	blocked []bool
}

// Dial prepares connections to every node in addrs except self. gRPC
// connects lazily, so peers that are not up yet are not an error.
func Dial(self int, addrs []string) (*Peers, error) {
	p := &Peers{
		conns:   make([]*grpc.ClientConn, len(addrs)),
		clients: make([]raftpb.RaftClient, len(addrs)),
		blocked: make([]bool, len(addrs)),
	}
	for i, addr := range addrs {
		if i == self {
			continue
		}
		conn, err := grpc.NewClient(addr, DialOptions()...)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("transport: dial %s: %w", addr, err)
		}
		p.conns[i] = conn
		p.clients[i] = raftpb.NewRaftClient(conn)
	}
	return p, nil
}

// Close closes every connection.
func (p *Peers) Close() {
	for _, c := range p.conns {
		if c != nil {
			c.Close()
		}
	}
}

// SetBlocked makes every RPC this node sends to peer fail while blocked is
// true. Tests use it to simulate network partitions.
func (p *Peers) SetBlocked(peer int, blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked[peer] = blocked
}

func (p *Peers) client(peer int) (raftpb.RaftClient, error) {
	if peer < 0 || peer >= len(p.clients) || p.clients[peer] == nil {
		return nil, fmt.Errorf("transport: no peer %d", peer)
	}
	p.mu.RLock()
	blocked := p.blocked[peer]
	p.mu.RUnlock()
	if blocked {
		return nil, ErrBlocked
	}
	return p.clients[peer], nil
}

// Group returns the raft.Transport for one shard group.
func (p *Peers) Group(group int) raft.Transport {
	return &groupTransport{peers: p, group: int32(group)}
}

type groupTransport struct {
	peers *Peers
	group int32
}

func (t *groupTransport) RequestVote(ctx context.Context, peer int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := t.peers.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Group:        t.group,
		Term:         int64(args.Term),
		CandidateId:  int32(args.CandidateID),
		LastLogIndex: int64(args.LastLogIndex),
		LastLogTerm:  int64(args.LastLogTerm),
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: int(resp.Term), VoteGranted: resp.VoteGranted}, nil
}

func (t *groupTransport) AppendEntries(ctx context.Context, peer int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := t.peers.client(peer)
	if err != nil {
		return nil, err
	}
	entries := make([]*raftpb.Entry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &raftpb.Entry{Term: int64(e.Term), Data: e.Data}
	}
	resp, err := c.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Group:        t.group,
		Term:         int64(args.Term),
		LeaderId:     int32(args.LeaderID),
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int64(args.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int64(args.LeaderCommit),
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:    int(resp.Term),
		Success: resp.Success,
		XTerm:   int(resp.XTerm),
		XIndex:  int(resp.XIndex),
		XLen:    int(resp.XLen),
	}, nil
}

func (t *groupTransport) InstallSnapshot(ctx context.Context, peer int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	c, err := t.peers.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Group:             t.group,
		Term:              int64(args.Term),
		LeaderId:          int32(args.LeaderID),
		LastIncludedIndex: int64(args.LastIncludedIndex),
		LastIncludedTerm:  int64(args.LastIncludedTerm),
		Data:              args.Data,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: int(resp.Term)}, nil
}

// Server hands incoming Raft RPCs to this node's replica of the named
// group.
type Server struct {
	raftpb.UnimplementedRaftServer
	lookup func(group int) *raft.Node
}

// NewServer returns a Server that finds replicas with lookup, which
// returns nil for a group this node does not host.
func NewServer(lookup func(group int) *raft.Node) *Server {
	return &Server{lookup: lookup}
}

func (s *Server) replica(group int32) (*raft.Node, error) {
	n := s.lookup(int(group))
	if n == nil {
		return nil, status.Errorf(codes.NotFound, "no replica of group %d", group)
	}
	return n, nil
}

func unavailable(err error) error { return status.Error(codes.Unavailable, err.Error()) }

func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	n, err := s.replica(req.Group)
	if err != nil {
		return nil, err
	}
	reply, err := n.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         int(req.Term),
		CandidateID:  int(req.CandidateId),
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	})
	if err != nil {
		return nil, unavailable(err)
	}
	return &raftpb.RequestVoteResponse{Term: int64(reply.Term), VoteGranted: reply.VoteGranted}, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	n, err := s.replica(req.Group)
	if err != nil {
		return nil, err
	}
	entries := make([]raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = raft.LogEntry{Term: int(e.Term), Data: e.Data}
	}
	reply, err := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         int(req.Term),
		LeaderID:     int(req.LeaderId),
		PrevLogIndex: int(req.PrevLogIndex),
		PrevLogTerm:  int(req.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int(req.LeaderCommit),
	})
	if err != nil {
		return nil, unavailable(err)
	}
	return &raftpb.AppendEntriesResponse{
		Term:    int64(reply.Term),
		Success: reply.Success,
		XTerm:   int64(reply.XTerm),
		XIndex:  int64(reply.XIndex),
		XLen:    int64(reply.XLen),
	}, nil
}

func (s *Server) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	n, err := s.replica(req.Group)
	if err != nil {
		return nil, err
	}
	reply, err := n.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              int(req.Term),
		LeaderID:          int(req.LeaderId),
		LastIncludedIndex: int(req.LastIncludedIndex),
		LastIncludedTerm:  int(req.LastIncludedTerm),
		Data:              req.Data,
	})
	if err != nil {
		return nil, unavailable(err)
	}
	return &raftpb.InstallSnapshotResponse{Term: int64(reply.Term)}, nil
}
