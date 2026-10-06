package raft

import "context"

// Leader election (§5.2 and §5.4.1 of the Raft paper).

// HandleRequestVote is the RequestVote RPC handler.
func (rf *Node) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return nil, ErrStopped
	}
	if args.Term < rf.currentTerm {
		return &RequestVoteReply{Term: rf.currentTerm}, nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply := &RequestVoteReply{Term: rf.currentTerm}
	if (rf.votedFor == noVote || rf.votedFor == args.CandidateID) &&
		rf.candidateIsUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateID
		rf.persistHardState()
		// Reset only when granting a vote, so a candidate that cannot win
		// does not hold back elections it would lose anyway.
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply, nil
}

// candidateIsUpToDate is the election restriction: vote only for a
// candidate whose log is at least as up to date as ours, so every leader
// holds every committed entry. Caller must hold rf.mu.
func (rf *Node) candidateIsUpToDate(lastLogIndex, lastLogTerm int) bool {
	if lastLogTerm != rf.log.lastTerm() {
		return lastLogTerm > rf.log.lastTerm()
	}
	return lastLogIndex >= rf.log.lastIndex()
}

// startElection becomes a candidate for the next term and requests votes
// in parallel. Caller must hold rf.mu.
func (rf *Node) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderID = -1
	rf.persistHardState()
	rf.resetElectionTimer()

	if rf.n == 1 {
		rf.becomeLeader()
		return
	}

	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: rf.log.lastIndex(),
		LastLogTerm:  rf.log.lastTerm(),
	}
	votes := 1
	for p := 0; p < rf.n; p++ {
		if p == rf.me {
			continue
		}
		go func(peer int) {
			ctx, cancel := context.WithTimeout(context.Background(), rf.cfg.ElectionTimeoutMin)
			defer cancel()
			reply, err := rf.cfg.Transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			// Ignore replies that arrive after this election is decided.
			if rf.role != Candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > rf.n/2 {
				rf.becomeLeader()
			}
		}(p)
	}
}
