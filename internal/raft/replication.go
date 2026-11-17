package raft

import (
	"context"
	"sort"
	"time"
)

// Propose appends command to the leader's log and kicks off replication
// to the rest of the cluster. It returns immediately (ok=false if this
// node isn't the leader); callers must wait for the entry to actually be
// committed (e.g. by polling CommitIndex, or observing it via ApplyFn)
// before treating it as durable — Propose itself makes no such guarantee.
func (r *Raft) Propose(command []byte) (LogIndex, Term, bool) {
	r.mu.Lock()
	if r.role != Leader {
		r.mu.Unlock()
		return 0, 0, false
	}

	entry, err := r.proposeLocked(command)
	if err != nil {
		r.logf("propose: %v", err)
		r.mu.Unlock()
		return 0, 0, false
	}
	term := r.currentTerm
	r.tryAdvanceCommitIndexLocked() // handles the no-peers / already-quorate case
	r.mu.Unlock()

	go r.replicate(term)
	return entry.Index, term, true
}

// replicate sends AppendEntries (carrying whatever entries each peer is
// missing, per nextIndex — possibly none, i.e. a plain heartbeat) to every
// peer in parallel. It's called both periodically for heartbeats and
// immediately after Propose for low latency.
func (r *Raft) replicate(term Term) {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}
	peers := append([]NodeID(nil), r.peers...)
	timeout := r.rpcTimeoutLocked()
	r.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		go r.replicateTo(peer, term, timeout)
	}
}

func (r *Raft) replicateTo(peer NodeID, term Term, timeout time.Duration) {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return
	}

	nextIdx := r.nextIndex[peer]
	if nextIdx < 1 {
		nextIdx = 1
	}

	if nextIdx <= r.lastIncludedIndex {
		// Whatever this follower needs has already been compacted out
		// of our log; it has to catch up via a full snapshot instead.
		r.mu.Unlock()
		r.sendInstallSnapshotTo(peer, term, timeout)
		return
	}

	prevLogIndex := nextIdx - 1
	prevPos := int(prevLogIndex - r.lastIncludedIndex)
	var prevLogTerm Term
	if prevPos >= 0 && prevPos < len(r.log) {
		prevLogTerm = r.log[prevPos].Term
	}
	var entries []LogEntry
	if curPos := int(nextIdx - r.lastIncludedIndex); curPos >= 0 && curPos < len(r.log) {
		entries = append([]LogEntry(nil), r.log[curPos:]...)
	}
	leaderCommit := r.commitIndex
	id := r.id
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	reply, err := r.transport.SendAppendEntries(ctx, peer, &AppendEntriesArgs{
		Term:         term,
		LeaderID:     id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
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
	if r.role != Leader || r.currentTerm != term {
		return // superseded by a later term/role change; this reply is stale
	}

	if reply.Success {
		matched := prevLogIndex + LogIndex(len(entries))
		if matched > r.matchIndex[peer] {
			r.matchIndex[peer] = matched
		}
		r.nextIndex[peer] = matched + 1
		r.tryAdvanceCommitIndexLocked()
		return
	}

	// Consistency check failed on the follower: back off by one and
	// retry on the next heartbeat/trigger. (A real backtrack-by-term
	// optimization would converge faster on long divergences; skipped
	// here in favor of a simpler, easier-to-verify implementation.)
	if r.nextIndex[peer] > 1 {
		r.nextIndex[peer]--
	}
}

// sendInstallSnapshotTo sends the leader's current snapshot to peer when
// the entries it needs have already been compacted out of the leader's
// log, then advances its nextIndex/matchIndex past what the snapshot
// already covers.
func (r *Raft) sendInstallSnapshotTo(peer NodeID, term Term, timeout time.Duration) {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term || r.stateMachine == nil {
		r.mu.Unlock()
		return
	}
	data, err := r.stateMachine.Snapshot()
	if err != nil {
		r.logf("install snapshot to %s: snapshot state machine: %v", peer, err)
		r.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          r.id,
		LastIncludedIndex: r.lastIncludedIndex,
		LastIncludedTerm:  r.lastIncludedTerm,
		Data:              data,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	reply, err := r.transport.SendInstallSnapshot(ctx, peer, args)
	if err != nil || reply == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return
	}
	if r.role != Leader || r.currentTerm != term {
		return
	}

	if args.LastIncludedIndex+1 > r.nextIndex[peer] {
		r.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	if args.LastIncludedIndex > r.matchIndex[peer] {
		r.matchIndex[peer] = args.LastIncludedIndex
	}
	r.tryAdvanceCommitIndexLocked()
}

// tryAdvanceCommitIndexLocked checks whether some index is now present on
// a majority of nodes (self included) *in the leader's current term* —
// Raft never commits entries from older terms purely by counting
// replicas, since a later leader could still legitimately overwrite them
// (§5.4.2 of the Raft paper). If so, commitIndex advances and the apply
// loop is woken.
func (r *Raft) tryAdvanceCommitIndexLocked() {
	if r.role != Leader {
		return
	}

	indices := make([]LogIndex, 0, len(r.peers)+1)
	indices = append(indices, r.lastLogIndexLocked()) // the leader's own log
	for _, idx := range r.matchIndex {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })

	majority := len(indices)/2 + 1
	candidate := indices[len(indices)-majority]
	candidatePos := int(candidate - r.lastIncludedIndex)

	if candidate > r.commitIndex && candidatePos >= 0 && candidatePos < len(r.log) && r.log[candidatePos].Term == r.currentTerm {
		r.commitIndex = candidate
		r.signalApply()
	}
}
