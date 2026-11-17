package transport

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/transport/raftpb"
)

// Client is a raft.Transport that sends Raft RPCs to peers over real gRPC
// connections, resolving each peer's address from a static node-id ->
// address map. Connections are created lazily, on first use, and reused
// afterward.
type Client struct {
	addresses map[raft.NodeID]string

	mu    sync.Mutex
	conns map[raft.NodeID]*grpc.ClientConn
}

// NewClient builds a Client that dials peers at the given addresses
// (e.g. "10.0.0.2:7000") as needed.
func NewClient(addresses map[raft.NodeID]string) *Client {
	return &Client{
		addresses: addresses,
		conns:     make(map[raft.NodeID]*grpc.ClientConn),
	}
}

func (c *Client) stubFor(target raft.NodeID) (raftpb.RaftTransportClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[target]; ok {
		return raftpb.NewRaftTransportClient(conn), nil
	}

	addr, ok := c.addresses[target]
	if !ok {
		return nil, fmt.Errorf("transport: no address known for peer %s", target)
	}

	// grpc.NewClient does not dial eagerly; the connection is
	// established lazily on the first RPC and kept/reconnected by
	// gRPC's own client machinery after that.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("transport: create client for %s (%s): %w", target, addr, err)
	}
	c.conns[target] = conn
	return raftpb.NewRaftTransportClient(conn), nil
}

func (c *Client) SendRequestVote(ctx context.Context, target raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	stub, err := c.stubFor(target)
	if err != nil {
		return nil, err
	}
	resp, err := stub.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         uint64(args.Term),
		CandidateId:  string(args.CandidateID),
		LastLogIndex: uint64(args.LastLogIndex),
		LastLogTerm:  uint64(args.LastLogTerm),
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: raft.Term(resp.GetTerm()), VoteGranted: resp.GetVoteGranted()}, nil
}

func (c *Client) SendAppendEntries(ctx context.Context, target raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	stub, err := c.stubFor(target)
	if err != nil {
		return nil, err
	}

	entries := make([]*raftpb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &raftpb.LogEntry{Term: uint64(e.Term), Index: uint64(e.Index), Command: e.Command}
	}

	resp, err := stub.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         uint64(args.Term),
		LeaderId:     string(args.LeaderID),
		PrevLogIndex: uint64(args.PrevLogIndex),
		PrevLogTerm:  uint64(args.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: uint64(args.LeaderCommit),
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{Term: raft.Term(resp.GetTerm()), Success: resp.GetSuccess()}, nil
}

func (c *Client) SendInstallSnapshot(ctx context.Context, target raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	stub, err := c.stubFor(target)
	if err != nil {
		return nil, err
	}
	resp, err := stub.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term:              uint64(args.Term),
		LeaderId:          string(args.LeaderID),
		LastIncludedIndex: uint64(args.LastIncludedIndex),
		LastIncludedTerm:  uint64(args.LastIncludedTerm),
		Data:              args.Data,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: raft.Term(resp.GetTerm())}, nil
}

// Close releases every connection this client has opened.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
