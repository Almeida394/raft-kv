package transport

import (
	"context"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/transport/raftpb"
)

// Server adapts a raft.RPCHandler (a local *raft.Raft node) to the
// generated gRPC service, translating between raftpb wire messages and
// the raft package's own types. It has no logic of its own beyond that
// translation - the actual consensus rules live in internal/raft.
type Server struct {
	raftpb.UnimplementedRaftTransportServer
	handler raft.RPCHandler
}

// NewServer wraps handler so it can be registered on a *grpc.Server via
// raftpb.RegisterRaftTransportServer.
func NewServer(handler raft.RPCHandler) *Server {
	return &Server{handler: handler}
}

func (s *Server) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	reply := s.handler.RequestVote(&raft.RequestVoteArgs{
		Term:         raft.Term(req.GetTerm()),
		CandidateID:  raft.NodeID(req.GetCandidateId()),
		LastLogIndex: raft.LogIndex(req.GetLastLogIndex()),
		LastLogTerm:  raft.Term(req.GetLastLogTerm()),
	})
	return &raftpb.RequestVoteResponse{
		Term:        uint64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *Server) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	entries := make([]raft.LogEntry, len(req.GetEntries()))
	for i, e := range req.GetEntries() {
		entries[i] = raft.LogEntry{
			Term:    raft.Term(e.GetTerm()),
			Index:   raft.LogIndex(e.GetIndex()),
			Command: e.GetCommand(),
		}
	}

	reply := s.handler.AppendEntries(&raft.AppendEntriesArgs{
		Term:         raft.Term(req.GetTerm()),
		LeaderID:     raft.NodeID(req.GetLeaderId()),
		PrevLogIndex: raft.LogIndex(req.GetPrevLogIndex()),
		PrevLogTerm:  raft.Term(req.GetPrevLogTerm()),
		Entries:      entries,
		LeaderCommit: raft.LogIndex(req.GetLeaderCommit()),
	})
	return &raftpb.AppendEntriesResponse{
		Term:    uint64(reply.Term),
		Success: reply.Success,
	}, nil
}

func (s *Server) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	reply := s.handler.InstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              raft.Term(req.GetTerm()),
		LeaderID:          raft.NodeID(req.GetLeaderId()),
		LastIncludedIndex: raft.LogIndex(req.GetLastIncludedIndex()),
		LastIncludedTerm:  raft.Term(req.GetLastIncludedTerm()),
		Data:              req.GetData(),
	})
	return &raftpb.InstallSnapshotResponse{
		Term: uint64(reply.Term),
	}, nil
}
