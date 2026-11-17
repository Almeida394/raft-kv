package api_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Almeida394/raft-kv/internal/api"
	"github.com/Almeida394/raft-kv/internal/api/kvpb"
	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/raft/rafttest"
	"github.com/Almeida394/raft-kv/internal/storage"
)

type grpcTestNode struct {
	id   raft.NodeID
	raft *raft.Raft
	srv  *grpc.Server
	conn *grpc.ClientConn
}

// newGRPCCluster wires n nodes over the in-memory rafttest network for
// Raft, each exposing the KV service over a real gRPC server on
// loopback TCP, with PeerConn resolving to real client connections
// between them - so forwarding from a follower to the leader is a real
// gRPC call, not an in-process shortcut.
func newGRPCCluster(t *testing.T, n int) []*grpcTestNode {
	t.Helper()

	ids := make([]raft.NodeID, n)
	addrs := make(map[raft.NodeID]string, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i))
		addrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", 19980+i)
	}

	network := rafttest.NewNetwork()
	nodes := make([]*grpcTestNode, n)
	conns := make(map[raft.NodeID]*grpc.ClientConn, n)

	connFor := func(target raft.NodeID) (*grpc.ClientConn, error) {
		if conn, ok := conns[target]; ok {
			return conn, nil
		}
		conn, err := grpc.NewClient(addrs[target], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns[target] = conn
		return conn, nil
	}

	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		apiServer := api.NewServer(storage.NewKVStore(), nil, connFor)

		node, err := raft.NewRaft(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          network.Endpoint(id),
			DataDir:            t.TempDir(),
			ApplyFn:            apiServer.ApplyFn,
			ElectionTimeoutMin: 50 * time.Millisecond,
			ElectionTimeoutMax: 100 * time.Millisecond,
			HeartbeatInterval:  15 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaft(%s): %v", id, err)
		}
		network.Register(id, node)
		apiServer.Raft = node

		srv, _, err := serveKV(addrs[id], apiServer)
		if err != nil {
			t.Fatalf("serveKV(%s): %v", id, err)
		}

		nodes[i] = &grpcTestNode{id: id, raft: node, srv: srv}
	}

	for _, n := range nodes {
		n.raft.Start()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.raft.Stop()
			n.srv.Stop()
		}
		for _, c := range conns {
			_ = c.Close()
		}
	})

	// Resolve self-connections too, so a test can always dial any node by id.
	for _, id := range ids {
		conn, err := connFor(id)
		if err != nil {
			t.Fatalf("connFor(%s): %v", id, err)
		}
		nodes[idIndex(ids, id)].conn = conn
	}

	return nodes
}

func idIndex(ids []raft.NodeID, id raft.NodeID) int {
	for i, x := range ids {
		if x == id {
			return i
		}
	}
	return -1
}

func serveKV(addr string, s *api.Server) (*grpc.Server, string, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}
	srv := grpc.NewServer()
	kvpb.RegisterKVServer(srv, api.NewGRPCServer(s))
	go func() { _ = srv.Serve(lis) }()
	return srv, addr, nil
}

func awaitGRPCLeader(t *testing.T, nodes []*grpcTestNode, timeout time.Duration) *grpcTestNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func TestGRPC_PutGetDeleteOnLeader(t *testing.T) {
	nodes := newGRPCCluster(t, 3)
	leader := awaitGRPCLeader(t, nodes, 2*time.Second)
	client := kvpb.NewKVClient(leader.conn)
	ctx := context.Background()

	if _, err := client.Put(ctx, &kvpb.PutRequest{Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, err := client.Get(ctx, &kvpb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "v" {
		t.Fatalf("Get = (found=%v, value=%q), want (true, \"v\")", resp.GetFound(), resp.GetValue())
	}

	if _, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: "k"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	resp, err = client.Get(ctx, &kvpb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if resp.GetFound() {
		t.Fatalf("Get after delete: found=true, want false")
	}
}

func TestGRPC_FollowerForwardsToLeader(t *testing.T) {
	nodes := newGRPCCluster(t, 3)
	leader := awaitGRPCLeader(t, nodes, 2*time.Second)

	var follower *grpcTestNode
	for _, n := range nodes {
		if n.id != leader.id {
			follower = n
			break
		}
	}

	client := kvpb.NewKVClient(follower.conn)
	ctx := context.Background()

	if _, err := client.Put(ctx, &kvpb.PutRequest{Key: "k2", Value: []byte("v2")}); err != nil {
		t.Fatalf("Put via follower: %v", err)
	}
	resp, err := client.Get(ctx, &kvpb.GetRequest{Key: "k2"})
	if err != nil {
		t.Fatalf("Get via follower: %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "v2" {
		t.Fatalf("Get via follower = (found=%v, value=%q), want (true, \"v2\")", resp.GetFound(), resp.GetValue())
	}
}
