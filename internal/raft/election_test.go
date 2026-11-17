package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/raft/rafttest"
)

// newTestCluster builds n nodes wired together over a rafttest.Network,
// starts them all, and arranges for them to be stopped when the test
// ends. Short timeouts keep the test suite fast; they're still large
// enough relative to tickInterval to avoid flakiness on a loaded CI box.
func newTestCluster(t *testing.T, n int) ([]*raft.Raft, *rafttest.Network) {
	t.Helper()

	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i))
	}

	network := rafttest.NewNetwork()
	nodes := make([]*raft.Raft, n)
	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		node, err := raft.NewRaft(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          network.Endpoint(id),
			HardStateDir:       t.TempDir(),
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

	return nodes, network
}

// awaitLeader polls nodes until exactly one of them reports itself as
// leader AND every other node in the slice has already heard about it
// (leaderID matches). The second condition matters because becoming
// leader and the first heartbeat reaching followers are two separate,
// asynchronous steps: without it, a test could observe "there is a
// leader" a few microseconds before followers find out, and flake on
// asserting the follower's view of who the leader is.
func awaitLeader(t *testing.T, nodes []*raft.Raft, timeout time.Duration) *raft.Raft {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *raft.Raft
		agreed := true
		for _, n := range nodes {
			if n.IsLeader() {
				if leader != nil {
					agreed = false // more than one self-reported leader: not converged yet
					break
				}
				leader = n
			}
		}

		if leader != nil && agreed {
			allKnow := true
			for _, n := range nodes {
				if n.ID() == leader.ID() {
					continue
				}
				_, _, leaderID := n.State()
				if leaderID != leader.ID() {
					allKnow = false
					break
				}
			}
			if allKnow {
				return leader
			}
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("no single, cluster-wide agreed leader among %d nodes within %s", len(nodes), timeout)
	return nil
}

func TestSingleLeaderElected(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)

	leaderTerm, leaderRole, _ := leader.State()
	if leaderRole != raft.Leader {
		t.Fatalf("awaitLeader returned a node not in Leader role: %s", leaderRole)
	}

	for _, n := range nodes {
		term, role, leaderID := n.State()
		if term != leaderTerm {
			t.Errorf("node %s term=%d, want %d (leader's term)", n.ID(), term, leaderTerm)
		}
		if n == leader {
			continue
		}
		if role == raft.Leader {
			t.Errorf("node %s also believes it is leader", n.ID())
		}
		if leaderID != leader.ID() {
			t.Errorf("follower %s thinks leader is %q, want %q", n.ID(), leaderID, leader.ID())
		}
	}
}

func TestNoSpuriousReelectionWhileLeaderAlive(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)
	termBefore, _, _ := leader.State()

	// Several election-timeout windows' worth of heartbeats; a correct
	// implementation keeps the same leader and term throughout.
	time.Sleep(500 * time.Millisecond)

	termAfter, role, _ := leader.State()
	if termAfter != termBefore || role != raft.Leader {
		t.Fatalf("stable leader changed unexpectedly: term %d -> %d, role=%s", termBefore, termAfter, role)
	}
	if !leader.IsLeader() {
		t.Fatalf("original leader %s is no longer leader", leader.ID())
	}
}

func TestReElectionAfterLeaderStops(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)
	oldTerm, _, _ := leader.State()

	leader.Stop()

	var remaining []*raft.Raft
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			remaining = append(remaining, n)
		}
	}

	newLeader := awaitLeader(t, remaining, 2*time.Second)
	newTerm, _, _ := newLeader.State()

	if newLeader.ID() == leader.ID() {
		t.Fatalf("new leader should differ from the stopped leader")
	}
	if newTerm <= oldTerm {
		t.Fatalf("new leader's term %d is not greater than old leader's term %d", newTerm, oldTerm)
	}
}

func TestMinorityPartitionCannotElectLeader(t *testing.T) {
	nodes, network := newTestCluster(t, 5)
	leader := awaitLeader(t, nodes, 2*time.Second)

	// Partition two followers (a minority of 5) away from everyone else,
	// including each other's view of the rest of the cluster.
	var minority []*raft.Raft
	for _, n := range nodes {
		if n.ID() == leader.ID() {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, n)
			network.Partition(n.ID())
		}
	}

	time.Sleep(500 * time.Millisecond)

	for _, n := range minority {
		if n.IsLeader() {
			t.Fatalf("minority node %s became leader despite lacking quorum", n.ID())
		}
	}

	var majority []*raft.Raft
	for _, n := range nodes {
		isMinority := false
		for _, m := range minority {
			if m.ID() == n.ID() {
				isMinority = true
			}
		}
		if !isMinority {
			majority = append(majority, n)
		}
	}
	// The majority side (3 of 5) must still make progress and agree on
	// exactly one leader.
	awaitLeader(t, majority, 2*time.Second)
}

func TestPartitionedLeaderStepsDownOnRejoin(t *testing.T) {
	nodes, network := newTestCluster(t, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)
	oldLeaderID := leader.ID()
	oldTerm, _, _ := leader.State()

	// Partition the leader away: the other two can still form a majority
	// and must elect a new leader at a higher term.
	network.Partition(oldLeaderID)

	var remaining []*raft.Raft
	for _, n := range nodes {
		if n.ID() != oldLeaderID {
			remaining = append(remaining, n)
		}
	}
	newLeader := awaitLeader(t, remaining, 2*time.Second)
	newTerm, _, _ := newLeader.State()
	if newTerm <= oldTerm {
		t.Fatalf("new leader's term %d is not greater than old leader's term %d", newTerm, oldTerm)
	}

	// Heal the partition: the old leader must observe the higher term
	// (via AppendEntries/RequestVote from the new leader) and step down
	// to Follower rather than continuing to act as leader.
	network.Heal(oldLeaderID)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !leader.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader.IsLeader() {
		t.Fatalf("old leader %s is still acting as leader after rejoining a cluster with a newer term", oldLeaderID)
	}
}
