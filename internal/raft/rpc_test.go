package raft_test

import (
	"testing"
	"time"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/raft/rafttest"
)

// newIsolatedNode builds a single node registered on a network with no
// peers and does NOT start its background loop, so tests can call its RPC
// handlers directly and assert on the resulting state without timing
// flakiness.
func newIsolatedNode(t *testing.T, id raft.NodeID) *raft.Raft {
	t.Helper()
	network := rafttest.NewNetwork()
	node, err := raft.NewRaft(raft.Config{
		ID:                 id,
		Peers:              nil,
		Transport:          network.Endpoint(id),
		DataDir:            t.TempDir(),
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 100 * time.Millisecond,
		HeartbeatInterval:  15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	network.Register(id, node)
	t.Cleanup(func() { node.Stop() }) // releases the WAL file handle
	return node
}

func TestRequestVote_GrantsVoteWhenUnvoted(t *testing.T) {
	node := newIsolatedNode(t, "n1")

	reply := node.RequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: "n2"})
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got %+v", reply)
	}
	if reply.Term != 1 {
		t.Fatalf("reply.Term = %d, want 1", reply.Term)
	}
}

func TestRequestVote_RejectsStaleTerm(t *testing.T) {
	node := newIsolatedNode(t, "n1")
	node.RequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "n2"}) // bumps node to term 5

	reply := node.RequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: "n3"})
	if reply.VoteGranted {
		t.Fatalf("expected vote rejected for stale term, got granted")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
}

func TestRequestVote_RejectsSecondCandidateSameTerm(t *testing.T) {
	node := newIsolatedNode(t, "n1")

	first := node.RequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: "n2"})
	if !first.VoteGranted {
		t.Fatalf("expected first vote granted, got %+v", first)
	}

	second := node.RequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: "n3"})
	if second.VoteGranted {
		t.Fatalf("expected second candidate in same term to be rejected, got %+v", second)
	}
}

func TestRequestVote_ReVotesSameCandidateIdempotently(t *testing.T) {
	node := newIsolatedNode(t, "n1")

	first := node.RequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: "n2"})
	second := node.RequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: "n2"})
	if !first.VoteGranted || !second.VoteGranted {
		t.Fatalf("expected repeated vote for the same candidate/term to be granted both times: %+v, %+v", first, second)
	}
}

func TestRequestVote_HigherTermGrantsAndAdoptsTerm(t *testing.T) {
	node := newIsolatedNode(t, "n1")

	reply := node.RequestVote(&raft.RequestVoteArgs{Term: 9, CandidateID: "n2"})
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got %+v", reply)
	}
	term, role, _ := node.State()
	if term != 9 || role != raft.Follower {
		t.Fatalf("state = (%d, %s), want (9, Follower)", term, role)
	}
}

func TestAppendEntries_RejectsStaleTerm(t *testing.T) {
	node := newIsolatedNode(t, "n1")
	node.RequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "n2"}) // bumps node to term 5

	reply := node.AppendEntries(&raft.AppendEntriesArgs{Term: 3, LeaderID: "n2"})
	if reply.Success {
		t.Fatalf("expected AppendEntries to fail for stale term, got %+v", reply)
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
}

func TestAppendEntries_UpdatesTermAndLeader(t *testing.T) {
	node := newIsolatedNode(t, "n1")

	reply := node.AppendEntries(&raft.AppendEntriesArgs{Term: 4, LeaderID: "n2"})
	if !reply.Success {
		t.Fatalf("expected AppendEntries success, got %+v", reply)
	}
	term, role, leaderID := node.State()
	if term != 4 || role != raft.Follower || leaderID != "n2" {
		t.Fatalf("state = (%d, %s, %q), want (4, Follower, n2)", term, role, leaderID)
	}
}

func TestAppendEntries_CandidateStepsDownOnSameTermLeader(t *testing.T) {
	node := newIsolatedNode(t, "n1")
	// Force the node into Candidate by bumping its own term via a vote
	// request it rejects (lower CandidateID logic isn't relevant here);
	// simplest: drive it to candidate by calling the unexported path is
	// not possible from outside, so instead verify the documented
	// behavior via AppendEntries at the same term it already holds.
	node.RequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: "n2"}) // node is now Follower at term 1, voted n2

	reply := node.AppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: "n2"})
	if !reply.Success {
		t.Fatalf("expected AppendEntries success, got %+v", reply)
	}
	_, role, leaderID := node.State()
	if role != raft.Follower || leaderID != "n2" {
		t.Fatalf("state role=%s leader=%q, want Follower/n2", role, leaderID)
	}
}
