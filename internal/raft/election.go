package raft

import (
	"context"
	"time"
)

// run is the node's single background control loop: it ticks at a fixed
// resolution and reacts to elapsed deadlines. Keeping all timing decisions
// on one goroutine avoids races between "am I leader" and "should I start
// an election".
func (r *Raft) run() {
	ticker := time.NewTicker(r.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

func (r *Raft) tick() {
	r.mu.Lock()
	role := r.role
	now := time.Now()
	electionExpired := now.After(r.electionDeadline)
	term := r.currentTerm
	heartbeatDue := role == Leader && now.After(r.nextHeartbeat)
	if heartbeatDue {
		r.nextHeartbeat = now.Add(r.heartbeatInterval)
	}
	r.mu.Unlock()

	switch {
	case role != Leader && electionExpired:
		r.startElection()
	case heartbeatDue:
		go r.broadcastHeartbeats(term)
	}
}

// startElection converts the node to Candidate for a new term and asks
// every peer for its vote in parallel. It returns immediately; vote
// counting happens asynchronously as replies arrive.
func (r *Raft) startElection() {
	r.mu.Lock()
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.id
	term := r.currentTerm
	lastLogIndex, lastLogTerm := r.lastLogIndexTermLocked()
	r.resetElectionDeadlineLocked()
	_ = r.persistHardStateLocked()
	peers := append([]NodeID(nil), r.peers...)
	id := r.id
	total := len(r.peers) + 1
	timeout := r.rpcTimeoutLocked()
	r.mu.Unlock()

	r.logf("starting election for term %d", term)

	majority := total/2 + 1
	votes := 1 // vote for self

	if votes >= majority {
		// Degenerate single-node "cluster"; still go through
		// becomeLeaderLocked for consistent state setup.
		r.mu.Lock()
		if r.role == Candidate && r.currentTerm == term {
			r.becomeLeaderLocked()
		}
		r.mu.Unlock()
		return
	}

	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			reply, err := r.transport.SendRequestVote(ctx, peer, &RequestVoteArgs{
				Term:         term,
				CandidateID:  id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil || reply == nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if r.role != Candidate || r.currentTerm != term {
				return // election already resolved or superseded
			}
			if reply.VoteGranted {
				votes++
				if votes >= majority {
					r.becomeLeaderLocked()
				}
			}
		}()
	}
}

// becomeFollowerLocked steps the node down to Follower. If term is newer
// than the node's current term, the term is adopted and any vote already
// cast is forgotten, as required by Raft's safety rules.
func (r *Raft) becomeFollowerLocked(term Term) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
	}
	if r.role != Follower {
		r.logf("%s -> Follower (term %d)", r.role, term)
	}
	r.role = Follower
	r.leaderID = ""
	r.resetElectionDeadlineLocked()
	_ = r.persistHardStateLocked()
}

// becomeLeaderLocked promotes the node to Leader for its current term and
// immediately broadcasts a heartbeat so followers don't time out waiting
// to find out.
func (r *Raft) becomeLeaderLocked() {
	if r.role == Leader {
		return
	}
	r.role = Leader
	r.leaderID = r.id

	lastIdx := r.lastLogIndexLocked()
	r.nextIndex = make(map[NodeID]LogIndex, len(r.peers))
	r.matchIndex = make(map[NodeID]LogIndex, len(r.peers))
	for _, p := range r.peers {
		r.nextIndex[p] = lastIdx + 1
		r.matchIndex[p] = 0
	}

	r.nextHeartbeat = time.Now().Add(r.heartbeatInterval)
	term := r.currentTerm
	r.logf("became leader for term %d", term)
	go r.broadcastHeartbeats(term)
}

// broadcastHeartbeats sends an empty AppendEntries (heartbeat) to every
// peer. Log replication (real Entries, nextIndex-driven resends) is added
// in Phase 3; for now this only asserts leadership and carries the
// leader's commit index.
func (r *Raft) broadcastHeartbeats(term Term) {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}
	peers := append([]NodeID(nil), r.peers...)
	id := r.id
	commitIndex := r.commitIndex
	timeout := r.rpcTimeoutLocked()
	r.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			reply, err := r.transport.SendAppendEntries(ctx, peer, &AppendEntriesArgs{
				Term:         term,
				LeaderID:     id,
				LeaderCommit: commitIndex,
			})
			if err != nil || reply == nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
			}
		}()
	}
}

// RequestVote handles an incoming vote request from a candidate.
func (r *Raft) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}

	lastIndex, lastTerm := r.lastLogIndexTermLocked()
	logUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	grant := logUpToDate && (r.votedFor == "" || r.votedFor == args.CandidateID)
	if grant {
		r.votedFor = args.CandidateID
		// Granting a vote means we've just heard from a legitimate,
		// up-to-date candidate: don't race it to start our own election.
		r.resetElectionDeadlineLocked()
		_ = r.persistHardStateLocked()
	}

	return &RequestVoteReply{Term: r.currentTerm, VoteGranted: grant}
}

// AppendEntries handles an incoming heartbeat/replication call from the
// leader. Phase 2 scope: term and leadership bookkeeping only. Log
// matching, entry appending, and commit-index advancement are added in
// Phase 3.
func (r *Raft) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	} else if r.role == Candidate {
		// Same term, but another node already secured it: stand down.
		r.role = Follower
	}

	r.leaderID = args.LeaderID
	r.resetElectionDeadlineLocked()

	return &AppendEntriesReply{Term: r.currentTerm, Success: true}
}
