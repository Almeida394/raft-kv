package transport_test

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/storage"
	"github.com/Almeida394/raft-kv/internal/transport"
)

// TestGRPCTransport_ElectsLeaderAndReplicatesOverRealNetwork wires three
// Raft nodes together over real gRPC connections on loopback TCP sockets
// (not the in-memory rafttest.Network used by internal/raft's own tests)
// to prove the generated raftpb service and the Client/Server glue in
// this package actually work end-to-end: a real listener, a real dial, a
// real RPC, correctly translated back into raft package types.
func TestGRPCTransport_ElectsLeaderAndReplicatesOverRealNetwork(t *testing.T) {
	n := 3
	ids := make([]raft.NodeID, n)
	addresses := make(map[raft.NodeID]string, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i))
		addresses[ids[i]] = fmt.Sprintf("127.0.0.1:%d", 19870+i)
	}

	nodes := make([]*raft.Raft, n)
	kvs := make([]*storage.KVStore, n)
	servers := make([]*grpc.Server, n)
	clients := make([]*transport.Client, n)

	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		client := transport.NewClient(addresses)
		clients[i] = client

		kv := storage.NewKVStore()
		kvs[i] = kv

		node, err := raft.NewRaft(raft.Config{
			ID:        id,
			Peers:     peers,
			Transport: client,
			DataDir:   t.TempDir(),
			ApplyFn: func(e raft.LogEntry) {
				cmd, err := storage.DecodeCommand(e.Command)
				if err != nil {
					t.Errorf("decode command: %v", err)
					return
				}
				_ = kv.Apply(cmd)
			},
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaft(%s): %v", id, err)
		}
		nodes[i] = node

		srv, _, err := transport.Serve(addresses[id], node)
		if err != nil {
			t.Fatalf("transport.Serve(%s): %v", id, err)
		}
		servers[i] = srv
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
		for _, srv := range servers {
			srv.Stop()
		}
		for _, c := range clients {
			_ = c.Close()
		}
	})

	for _, n := range nodes {
		n.Start()
	}

	var leader *raft.Raft
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatalf("no leader elected over real gRPC connections within timeout")
	}

	idx, _, ok := leader.Propose(storage.Command{Op: storage.OpPut, Key: []byte("k"), Value: []byte("v")}.Encode())
	if !ok {
		t.Fatalf("Propose rejected by leader")
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && leader.CommitIndex() < idx {
		time.Sleep(20 * time.Millisecond)
	}
	if leader.CommitIndex() < idx {
		t.Fatalf("leader commitIndex=%d never reached %d", leader.CommitIndex(), idx)
	}

	for i, kv := range kvs {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if v, ok := kv.Get([]byte("k")); ok && string(v) == "v" {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		v, ok := kv.Get([]byte("k"))
		if !ok || string(v) != "v" {
			t.Fatalf("node %d kv[\"k\"] = (%q, %v), want (\"v\", true)", i, v, ok)
		}
	}
}
