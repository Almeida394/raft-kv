package raft_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/raft/rafttest"
)

// appliedLog records entries passed to a node's ApplyFn in order, safe
// for concurrent use since ApplyFn can be called while the test goroutine
// is also reading it.
type appliedLog struct {
	mu      sync.Mutex
	entries []raft.LogEntry
}

func (a *appliedLog) record(e raft.LogEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, e)
}

func (a *appliedLog) commands() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.entries))
	for i, e := range a.entries {
		out[i] = string(e.Command)
	}
	return out
}

// newTestClusterWithApply is like newTestCluster but wires each node's
// ApplyFn to its own appliedLog so tests can assert on what got committed
// and in what order.
func newTestClusterWithApply(t *testing.T, n int) ([]*raft.Raft, []*appliedLog, *rafttest.Network) {
	t.Helper()

	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i))
	}

	network := rafttest.NewNetwork()
	nodes := make([]*raft.Raft, n)
	logs := make([]*appliedLog, n)

	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		al := &appliedLog{}
		logs[i] = al

		node, err := raft.NewRaft(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          network.Endpoint(id),
			DataDir:            t.TempDir(),
			ApplyFn:            al.record,
			ElectionTimeoutMin: 50 * time.Millisecond,
			ElectionTimeoutMax: 100 * time.Millisecond,
			HeartbeatInterval:  15 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaft(%s): %v", id, err)
		}
		network.Register(id, node)
		nodes[i] = node
	}

	for _, node := range nodes {
		node.Start()
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
	})

	return nodes, logs, network
}

func awaitCommitIndexAtLeast(t *testing.T, node *raft.Raft, want raft.LogIndex, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if node.CommitIndex() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %s commitIndex never reached %d (stuck at %d)", node.ID(), want, node.CommitIndex())
}

func TestPropose_RejectedByNonLeader(t *testing.T) {
	nodes, _, _ := newTestClusterWithApply(t, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)

	for _, n := range nodes {
		if n.ID() == leader.ID() {
			continue
		}
		if _, _, ok := n.Propose([]byte("should-fail")); ok {
			t.Fatalf("Propose on non-leader %s unexpectedly succeeded", n.ID())
		}
		break
	}
}

func TestPropose_CommittedOnMajorityAndAppliedInOrder(t *testing.T) {
	nodes, logs, _ := newTestClusterWithApply(t, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)

	var leaderLog *appliedLog
	for i, n := range nodes {
		if n.ID() == leader.ID() {
			leaderLog = logs[i]
		}
	}

	want := []string{"put:a=1", "put:b=2", "delete:a"}
	var lastIndex raft.LogIndex
	for _, cmd := range want {
		idx, _, ok := leader.Propose([]byte(cmd))
		if !ok {
			t.Fatalf("Propose(%q) rejected by leader", cmd)
		}
		lastIndex = idx
	}

	awaitCommitIndexAtLeast(t, leader, lastIndex, 2*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(leaderLog.commands()) < len(want) {
		time.Sleep(10 * time.Millisecond)
	}

	got := leaderLog.commands()
	if len(got) != len(want) {
		t.Fatalf("leader applied %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("leader applied[%d] = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}

	// Every follower must eventually catch up and apply the same
	// commands in the same order.
	for i, n := range nodes {
		if n.ID() == leader.ID() {
			continue
		}
		fLog := logs[i]
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && len(fLog.commands()) < len(want) {
			time.Sleep(10 * time.Millisecond)
		}
		got := fLog.commands()
		if len(got) != len(want) {
			t.Fatalf("follower %s applied %v, want %v", n.ID(), got, want)
		}
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("follower %s applied[%d] = %q, want %q", n.ID(), j, got[j], want[j])
			}
		}
	}
}

func TestReplication_SlowFollowerCatchesUpAfterPartitionHeals(t *testing.T) {
	nodes, logs, network := newTestClusterWithApply(t, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)

	var cutOff *raft.Raft
	var cutOffLog *appliedLog
	for i, n := range nodes {
		if n.ID() != leader.ID() {
			cutOff = n
			cutOffLog = logs[i]
			break
		}
	}

	network.Partition(cutOff.ID())

	want := []string{"x=1", "x=2", "x=3"}
	var lastIndex raft.LogIndex
	for _, cmd := range want {
		idx, _, ok := leader.Propose([]byte(cmd))
		if !ok {
			t.Fatalf("Propose(%q) rejected", cmd)
		}
		lastIndex = idx
	}
	awaitCommitIndexAtLeast(t, leader, lastIndex, 2*time.Second)

	// The partitioned follower must NOT have these yet.
	time.Sleep(200 * time.Millisecond)
	if got := len(cutOffLog.commands()); got != 0 {
		t.Fatalf("partitioned follower applied %d entries before healing, want 0", got)
	}

	network.Heal(cutOff.ID())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(cutOffLog.commands()) < len(want) {
		time.Sleep(10 * time.Millisecond)
	}
	got := cutOffLog.commands()
	if len(got) != len(want) {
		t.Fatalf("healed follower applied %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("healed follower applied[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestAppendEntries_RepairsConflictingSuffix drives the AppendEntries
// handler directly (no network/timing involved) to verify the log-repair
// path: a follower with stale, uncommitted entries from an old leader
// must have them overwritten once a legitimate leader's entries arrive.
func TestAppendEntries_RepairsConflictingSuffix(t *testing.T) {
	node := newIsolatedNode(t, "follower")

	// Simulate the follower having accepted entries from an old leader
	// at term 1: index 1 and 2 both at term 1.
	r1 := node.AppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "old-leader", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []raft.LogEntry{
			{Term: 1, Index: 1, Command: []byte("stale-1")},
			{Term: 1, Index: 2, Command: []byte("stale-2")},
		},
	})
	if !r1.Success {
		t.Fatalf("seeding stale entries failed: %+v", r1)
	}

	// A new leader at term 2 has a different entry at index 2 (its
	// term-1 entry at index 1 matches, so PrevLogIndex/PrevLogTerm = 1/1).
	r2 := node.AppendEntries(&raft.AppendEntriesArgs{
		Term: 2, LeaderID: "new-leader", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.LogEntry{
			{Term: 2, Index: 2, Command: []byte("real-2")},
			{Term: 2, Index: 3, Command: []byte("real-3")},
		},
	})
	if !r2.Success {
		t.Fatalf("repairing conflicting suffix failed: %+v", r2)
	}

	logEntries := node.LogEntries()
	if len(logEntries) != 4 { // sentinel + 3 real entries
		t.Fatalf("got %d log entries, want 4: %+v", len(logEntries), logEntries)
	}
	if string(logEntries[1].Command) != "stale-1" {
		t.Fatalf("index 1 = %q, want unchanged stale-1", logEntries[1].Command)
	}
	if string(logEntries[2].Command) != "real-2" || logEntries[2].Term != 2 {
		t.Fatalf("index 2 = %q (term %d), want real-2 (term 2)", logEntries[2].Command, logEntries[2].Term)
	}
	if string(logEntries[3].Command) != "real-3" {
		t.Fatalf("index 3 = %q, want real-3", logEntries[3].Command)
	}
}

func TestAppendEntries_RejectsOnLogGap(t *testing.T) {
	node := newIsolatedNode(t, "follower")

	// PrevLogIndex 5 can't possibly match on a node whose log is empty.
	reply := node.AppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: "leader", PrevLogIndex: 5, PrevLogTerm: 1,
	})
	if reply.Success {
		t.Fatalf("expected failure on log gap, got success")
	}
}
