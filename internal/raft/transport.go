package raft

import "context"

// RPC messages. They follow Figure 2 of the Raft paper, plus the fast
// backup hints XTerm, XIndex and XLen on AppendEntries replies.

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply carries, on a failed consistency check, enough for the
// leader to skip back a whole term per round trip:
//
//	XTerm   term of the follower's entry at PrevLogIndex, or -1 if its
//	        log is too short to have one
//	XIndex  first index the follower holds for XTerm
//	XLen    length of the follower's log (last index + 1)
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// Transport delivers RPCs to the other replicas of this node's group.
// Peers are numbered 0..Peers-1. Implementations must not modify the
// arguments they are given; the in-memory test transport passes them to
// the receiving replica as is.
type Transport interface {
	RequestVote(ctx context.Context, peer int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, peer int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}
