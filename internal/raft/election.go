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
		go r.replicate(term)
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

// becomeLeaderLocked promotes the node to Leader for its current term,
// appends a no-op entry to establish a commit point in the new term (see
// tryAdvanceCommitIndexLocked), and immediately triggers replication so
// followers don't time out waiting to find out.
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

	if _, err := r.proposeLocked(nil); err != nil {
		r.logf("append no-op leader entry: %v", err)
	}
	r.tryAdvanceCommitIndexLocked() // covers the no-peers / single-node case

	r.nextHeartbeat = time.Now().Add(r.heartbeatInterval)
	term := r.currentTerm
	r.logf("became leader for term %d", term)
	go r.replicate(term)
}

// RequestVote handles an incoming vote request from a candidate.
func (r *Raft) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		// A stopped node simulates a crashed/killed process: it must
		// not still answer RPCs or touch disk, even if a request from
		// a goroutine that started before it stopped arrives late.
		return &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}

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
// leader: term/leadership bookkeeping, the log-matching consistency
// check, appending any new entries (repairing a conflicting suffix
// first if needed), and advancing commitIndex from the leader's.
func (r *Raft) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

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

	lastIndex := r.lastLogIndexLocked()
	if args.PrevLogIndex > lastIndex {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}
	// PrevLogIndex before our snapshot base can't be verified against
	// our (now-compacted) log; this shouldn't happen given the leader
	// only falls back to InstallSnapshot once nextIndex reaches that
	// point, but reject conservatively if it ever does.
	prevPos := int(args.PrevLogIndex - r.lastIncludedIndex)
	if prevPos < 0 {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}
	if r.log[prevPos].Term != args.PrevLogTerm {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	// PrevLogIndex/PrevLogTerm matched: args.Entries belong right after
	// it. Any existing entry that conflicts (same index, different
	// term) means everything from there on must be discarded first;
	// an existing entry that already matches is left alone (idempotent
	// re-application of a retried/duplicated RPC).
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + LogIndex(i)
		pos := int(idx - r.lastIncludedIndex)
		if pos < len(r.log) {
			if r.log[pos].Term == entry.Term {
				continue
			}
			r.truncateLogLocked(idx)
		}
		if err := r.appendLogEntryLocked(entry); err != nil {
			r.logf("append replicated entry %d: %v", idx, err)
			return &AppendEntriesReply{Term: r.currentTerm, Success: false}
		}
	}

	if args.LeaderCommit > r.commitIndex {
		newCommit := args.LeaderCommit
		if last := r.lastLogIndexLocked(); newCommit > last {
			newCommit = last
		}
		if newCommit > r.commitIndex {
			r.commitIndex = newCommit
			r.signalApply()
		}
	}

	return &AppendEntriesReply{Term: r.currentTerm, Success: true}
}
