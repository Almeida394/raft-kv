package raft

import "context"

// RPCHandler is the set of incoming RPCs a Raft node must answer. *Raft
// implements it; a Transport delivers incoming network requests to these
// methods.
type RPCHandler interface {
	RequestVote(args *RequestVoteArgs) *RequestVoteReply
	AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	InstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}

// Transport is how a Raft node sends RPCs to a peer. There are two
// implementations in this project: rafttest.Network (in-memory, used by
// unit tests to simulate partitions and latency) and the real gRPC
// transport in internal/transport that carries these RPCs over the
// network between cluster nodes.
type Transport interface {
	SendRequestVote(ctx context.Context, target NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, target NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, target NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}
