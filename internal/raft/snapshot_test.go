package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/raft/rafttest"
	"github.com/Almeida394/raft-kv/internal/storage"
)

// newSnapshottingCluster is like newTestClusterWithApply but backs each
// node's ApplyFn with a real storage.KVStore (also wired as the node's
// StateMachine) and a low SnapshotThreshold, so compaction kicks in after
// just a handful of entries instead of needing hundreds of Proposes.
func newSnapshottingCluster(t *testing.T, n, snapshotThreshold int) ([]*raft.Raft, []*storage.KVStore, *rafttest.Network) {
	t.Helper()

	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i))
	}

	network := rafttest.NewNetwork()
	nodes := make([]*raft.Raft, n)
	kvs := make([]*storage.KVStore, n)

	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		kv := storage.NewKVStore()
		kvs[i] = kv

		node, err := raft.NewRaft(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          network.Endpoint(id),
			DataDir:            t.TempDir(),
			StateMachine:       kv,
			SnapshotThreshold:  snapshotThreshold,
			ApplyFn: func(e raft.LogEntry) {
				cmd, err := storage.DecodeCommand(e.Command)
				if err != nil {
					t.Errorf("decode command: %v", err)
					return
				}
				_ = kv.Apply(cmd)
			},
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

	return nodes, kvs, network
}

func proposePut(t *testing.T, leader *raft.Raft, key, value string) raft.LogIndex {
	t.Helper()
	idx, _, ok := leader.Propose(storage.Command{Op: storage.OpPut, Key: []byte(key), Value: []byte(value)}.Encode())
	if !ok {
		t.Fatalf("Propose(put %s=%s) rejected by leader", key, value)
	}
	return idx
}

func TestSnapshot_TakenAfterThresholdAndLogCompacted(t *testing.T) {
	nodes, _, _ := newSnapshottingCluster(t, 3, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)

	var lastIndex raft.LogIndex
	for i := 0; i < 10; i++ {
		lastIndex = proposePut(t, leader, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	awaitCommitIndexAtLeast(t, leader, lastIndex, 2*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if idx, _ := leader.SnapshotInfo(); idx > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	snapIdx, _ := leader.SnapshotInfo()
	if snapIdx == 0 {
		t.Fatalf("leader never took a snapshot despite exceeding the threshold")
	}

	logEntries := leader.LogEntries()
	if raft.LogIndex(len(logEntries)-1) >= lastIndex {
		t.Fatalf("log was not compacted: still has %d entries for lastIndex=%d", len(logEntries), lastIndex)
	}
}

func TestSnapshot_FarBehindFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	nodes, kvs, network := newSnapshottingCluster(t, 3, 3)
	leader := awaitLeader(t, nodes, 2*time.Second)

	var cutOff *raft.Raft
	var cutOffKV *storage.KVStore
	for i, n := range nodes {
		if n.ID() != leader.ID() {
			cutOff = n
			cutOffKV = kvs[i]
			break
		}
	}
	network.Partition(cutOff.ID())

	// Propose enough entries that the leader (and the still-connected
	// follower) will compact past what the partitioned node has, so it
	// can only catch up via a snapshot rather than replayed entries.
	var lastIndex raft.LogIndex
	for i := 0; i < 15; i++ {
		lastIndex = proposePut(t, leader, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	awaitCommitIndexAtLeast(t, leader, lastIndex, 2*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if idx, _ := leader.SnapshotInfo(); idx > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if idx, _ := leader.SnapshotInfo(); idx == 0 {
		t.Fatalf("leader never snapshotted; test setup assumption violated")
	}

	network.Heal(cutOff.ID())

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && cutOffKV.Len() < 15 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := cutOffKV.Len(); got != 15 {
		t.Fatalf("healed far-behind follower has %d keys, want 15", got)
	}
	for i := 0; i < 15; i++ {
		key, want := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		got, ok := cutOffKV.Get([]byte(key))
		if !ok || string(got) != want {
			t.Fatalf("follower %s = (%q, %v), want (%q, true)", key, got, ok, want)
		}
	}

	snapIdx, _ := cutOff.SnapshotInfo()
	if snapIdx == 0 {
		t.Fatalf("far-behind follower caught up without ever installing a snapshot")
	}
}

func TestInstallSnapshot_StaleSnapshotIgnored(t *testing.T) {
	node := newIsolatedNode(t, "n1")

	first := node.InstallSnapshot(&raft.InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 10, LastIncludedTerm: 1, Data: []byte("v1"),
	})
	if first.Term != 1 {
		t.Fatalf("first InstallSnapshot reply term = %d, want 1", first.Term)
	}
	if idx, _ := node.SnapshotInfo(); idx != 10 {
		t.Fatalf("SnapshotInfo index = %d, want 10", idx)
	}

	// A snapshot at an earlier-or-equal index must be a no-op.
	stale := node.InstallSnapshot(&raft.InstallSnapshotArgs{
		Term: 1, LeaderID: "leader", LastIncludedIndex: 5, LastIncludedTerm: 1, Data: []byte("stale"),
	})
	if stale.Term != 1 {
		t.Fatalf("stale InstallSnapshot reply term = %d, want 1", stale.Term)
	}
	if idx, _ := node.SnapshotInfo(); idx != 10 {
		t.Fatalf("SnapshotInfo index after stale snapshot = %d, want unchanged 10", idx)
	}
}

func TestInstallSnapshot_RejectsStaleTerm(t *testing.T) {
	node := newIsolatedNode(t, "n1")
	node.RequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: "other"}) // bump term to 5

	reply := node.InstallSnapshot(&raft.InstallSnapshotArgs{
		Term: 3, LeaderID: "leader", LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("x"),
	})
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
	if idx, _ := node.SnapshotInfo(); idx != 0 {
		t.Fatalf("stale-term InstallSnapshot must not be applied, SnapshotInfo index = %d", idx)
	}
}

func TestNode_RestartRestoresStateFromSnapshotAndTailLog(t *testing.T) {
	dataDir := t.TempDir()
	network := rafttest.NewNetwork()
	kv := storage.NewKVStore()

	node, err := raft.NewRaft(raft.Config{
		ID:                 "solo",
		Peers:              nil,
		Transport:          network.Endpoint("solo"),
		DataDir:            dataDir,
		StateMachine:       kv,
		SnapshotThreshold:  3,
		ApplyFn: func(e raft.LogEntry) {
			cmd, err := storage.DecodeCommand(e.Command)
			if err != nil {
				t.Errorf("decode command: %v", err)
				return
			}
			_ = kv.Apply(cmd)
		},
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	network.Register("solo", node)
	node.Start()

	leader := awaitLeader(t, []*raft.Raft{node}, 2*time.Second)
	for i := 0; i < 5; i++ {
		proposePut(t, leader, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && kv.Len() < 5 {
		time.Sleep(10 * time.Millisecond)
	}
	if kv.Len() != 5 {
		t.Fatalf("pre-restart kv has %d keys, want 5", kv.Len())
	}
	node.Stop()

	// "Restart": brand new process-equivalent state (fresh KVStore,
	// fresh network, fresh Raft instance) pointed at the same DataDir.
	kv2 := storage.NewKVStore()
	network2 := rafttest.NewNetwork()
	node2, err := raft.NewRaft(raft.Config{
		ID:                 "solo",
		Peers:              nil,
		Transport:          network2.Endpoint("solo"),
		DataDir:            dataDir,
		StateMachine:       kv2,
		SnapshotThreshold:  3,
		ApplyFn: func(e raft.LogEntry) {
			cmd, err := storage.DecodeCommand(e.Command)
			if err != nil {
				t.Errorf("decode command: %v", err)
				return
			}
			_ = kv2.Apply(cmd)
		},
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRaft (restart): %v", err)
	}

	// Restore() (called inside NewRaft when a snapshot exists) already
	// brings kv2 up to the snapshot. Any entries after the snapshot
	// still need to be re-applied, which only happens once commitIndex
	// is re-established; becoming leader (no peers) does that via the
	// no-op-entry mechanism.
	network2.Register("solo", node2)
	node2.Start()
	t.Cleanup(node2.Stop)

	awaitLeader(t, []*raft.Raft{node2}, 2*time.Second)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && kv2.Len() < 5 {
		time.Sleep(10 * time.Millisecond)
	}
	if kv2.Len() != 5 {
		t.Fatalf("restarted node's kv has %d keys, want 5", kv2.Len())
	}
	for i := 0; i < 5; i++ {
		key, want := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		got, ok := kv2.Get([]byte(key))
		if !ok || string(got) != want {
			t.Fatalf("restarted kv[%s] = (%q, %v), want (%q, true)", key, got, ok, want)
		}
	}
}
