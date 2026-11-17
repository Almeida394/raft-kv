package api

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Almeida394/raft-kv/internal/api/kvpb"
	"github.com/Almeida394/raft-kv/internal/storage"
)

// GRPCServer adapts Server to the generated kvpb.KVServer interface.
type GRPCServer struct {
	kvpb.UnimplementedKVServer
	s *Server
}

// NewGRPCServer wraps s so it can be registered on a *grpc.Server via
// kvpb.RegisterKVServer.
func NewGRPCServer(s *Server) *GRPCServer {
	return &GRPCServer{s: s}
}

func (g *GRPCServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if !g.s.Raft.IsLeader() {
		conn, err := g.s.PeerConn(g.s.leaderID())
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "no leader known: %v", err)
		}
		return kvpb.NewKVClient(conn).Get(ctx, req)
	}

	value, ok := g.s.KV.Get([]byte(req.GetKey()))
	return &kvpb.GetResponse{Value: value, Found: ok}, nil
}

func (g *GRPCServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if !g.s.Raft.IsLeader() {
		conn, err := g.s.PeerConn(g.s.leaderID())
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "no leader known: %v", err)
		}
		return kvpb.NewKVClient(conn).Put(ctx, req)
	}

	cmd := storage.Command{Op: storage.OpPut, Key: []byte(req.GetKey()), Value: req.GetValue()}
	if err := g.s.proposeAndWaitGRPC(ctx, cmd); err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{}, nil
}

func (g *GRPCServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if !g.s.Raft.IsLeader() {
		conn, err := g.s.PeerConn(g.s.leaderID())
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "no leader known: %v", err)
		}
		return kvpb.NewKVClient(conn).Delete(ctx, req)
	}

	cmd := storage.Command{Op: storage.OpDelete, Key: []byte(req.GetKey())}
	if err := g.s.proposeAndWaitGRPC(ctx, cmd); err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{}, nil
}

func (s *Server) proposeAndWaitGRPC(ctx context.Context, cmd storage.Command) error {
	index, _, ok := s.Raft.Propose(cmd.Encode())
	if !ok {
		return status.Error(codes.Unavailable, "not leader")
	}

	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := s.WaitForIndex(ctx, index); err != nil {
		return status.Errorf(codes.DeadlineExceeded, "timed out waiting for commit: %v", err)
	}
	return nil
}
