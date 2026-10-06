package node

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/LindseyZ1205/distributed-kv-store/gen/kvpb"
	"github.com/LindseyZ1205/distributed-kv-store/internal/kv"
)

// opTimeout bounds how long the server works on one request. Clients use a
// longer per-attempt deadline, so they normally get CODE_TIMEOUT back
// rather than a gRPC deadline error.
const opTimeout = time.Second

type kvService struct {
	kvpb.UnimplementedKVServer
	n *Node
}

func codeFor(err error) kvpb.Code {
	if errors.Is(err, kv.ErrWrongLeader) {
		return kvpb.Code_CODE_WRONG_LEADER
	}
	return kvpb.Code_CODE_TIMEOUT
}

func (s *kvService) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	g := s.n.groupFor(req.Key)
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	value, found, err := g.Get(ctx, req.Key)
	if err != nil {
		return &kvpb.GetResponse{Code: codeFor(err), LeaderHint: int32(g.Leader())}, nil
	}
	return &kvpb.GetResponse{
		Code:       kvpb.Code_CODE_OK,
		LeaderHint: int32(s.n.cfg.ID),
		Found:      found,
		Value:      value,
	}, nil
}

func validateWrite(clientID, seq uint64) error {
	if clientID == 0 || seq == 0 {
		return status.Error(codes.InvalidArgument, "client_id and seq must be set")
	}
	return nil
}

func (s *kvService) write(ctx context.Context, cmd kv.Command) (kvpb.Code, int32) {
	g := s.n.groupFor(cmd.Key)
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if err := g.Write(ctx, cmd); err != nil {
		return codeFor(err), int32(g.Leader())
	}
	return kvpb.Code_CODE_OK, int32(s.n.cfg.ID)
}

func (s *kvService) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := validateWrite(req.ClientId, req.Seq); err != nil {
		return nil, err
	}
	code, hint := s.write(ctx, kv.Command{
		Op:       kv.OpPut,
		Key:      req.Key,
		Value:    req.Value,
		ClientID: req.ClientId,
		Seq:      req.Seq,
	})
	return &kvpb.PutResponse{Code: code, LeaderHint: hint}, nil
}

func (s *kvService) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := validateWrite(req.ClientId, req.Seq); err != nil {
		return nil, err
	}
	code, hint := s.write(ctx, kv.Command{
		Op:       kv.OpDelete,
		Key:      req.Key,
		ClientID: req.ClientId,
		Seq:      req.Seq,
	})
	return &kvpb.DeleteResponse{Code: code, LeaderHint: hint}, nil
}

func (s *kvService) Topology(ctx context.Context, req *kvpb.TopologyRequest) (*kvpb.TopologyResponse, error) {
	return &kvpb.TopologyResponse{
		Nodes:  s.n.cfg.Peers,
		Shards: int32(s.n.cfg.Shards),
		Vnodes: int32(s.n.cfg.VNodes),
	}, nil
}

func (s *kvService) Status(ctx context.Context, req *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	resp := &kvpb.StatusResponse{NodeId: int32(s.n.cfg.ID)}
	for _, g := range s.n.groups {
		st := g.Status()
		resp.Shards = append(resp.Shards, &kvpb.ShardStatus{
			Shard:         int32(st.Shard),
			Role:          st.Raft.Role.String(),
			Term:          int64(st.Raft.Term),
			Leader:        int32(st.Raft.Leader),
			CommitIndex:   int64(st.Raft.CommitIndex),
			AppliedIndex:  int64(st.AppliedIndex),
			LastIndex:     int64(st.Raft.LastIndex),
			SnapshotIndex: int64(st.Raft.SnapshotIndex),
			Keys:          int64(st.Keys),
			Digest:        st.Digest,
		})
	}
	return resp, nil
}
