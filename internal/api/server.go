package api

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/Almeida394/raft-kv/internal/raft"
	"github.com/Almeida394/raft-kv/internal/storage"
)

// Server holds the state shared by the HTTP and gRPC front ends: the
// local Raft node, the KV state machine it drives, and the bookkeeping
// needed to block a write request until its entry is actually committed.
//
// Raft is set after construction (see NewServer) rather than taken as a
// constructor argument: building the *raft.Raft needs ApplyFn, which is
// a method on *Server, so Server must exist first. ApplyFn itself never
// reads Raft, so this ordering is safe as long as Raft is assigned
// before the HTTP/gRPC listeners start (which are the only things that
// read it).
type Server struct {
	Raft *raft.Raft
	KV   *storage.KVStore

	// HTTPAddrs/PeerConn resolve where to forward a request when this
	// node isn't the leader - the HTTP address and the gRPC connection
	// (shared with the Raft transport) for each peer, respectively.
	HTTPAddrs map[raft.NodeID]string
	PeerConn  func(raft.NodeID) (*grpc.ClientConn, error)

	mu              sync.Mutex
	lastAppliedSeen raft.LogIndex
	waiters         map[raft.LogIndex]chan struct{}
}

// NewServer wires up a Server with everything except Raft, which must be
// assigned (s.Raft = node) once the *raft.Raft has been constructed with
// this Server's ApplyFn, and before the HTTP/gRPC listeners start.
func NewServer(kv *storage.KVStore, httpAddrs map[raft.NodeID]string, peerConn func(raft.NodeID) (*grpc.ClientConn, error)) *Server {
	return &Server{
		KV:        kv,
		HTTPAddrs: httpAddrs,
		PeerConn:  peerConn,
		waiters:   make(map[raft.LogIndex]chan struct{}),
	}
}

// ApplyFn applies a committed entry to the KV state machine and wakes any
// request blocked in WaitForIndex on that index.
func (s *Server) ApplyFn(e raft.LogEntry) {
	cmd, err := storage.DecodeCommand(e.Command)
	if err == nil {
		_ = s.KV.Apply(cmd)
	}

	s.mu.Lock()
	s.lastAppliedSeen = e.Index
	if ch, ok := s.waiters[e.Index]; ok {
		close(ch)
		delete(s.waiters, e.Index)
	}
	s.mu.Unlock()
}

// waitChannelFor returns a channel that's closed once index has been
// applied (nil if it already has been). Checking and registering happen
// under the same lock ApplyFn uses to close waiters, so a signal can
// never be missed between the check and the registration.
func (s *Server) waitChannelFor(index raft.LogIndex) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index <= s.lastAppliedSeen {
		return nil
	}
	ch := make(chan struct{})
	s.waiters[index] = ch
	return ch
}

// leaderHTTPAddr returns the current leader's HTTP address, or ok=false
// if no leader is known or we don't have an address for it.
func (s *Server) leaderHTTPAddr() (string, bool) {
	_, _, leaderID := s.Raft.State()
	if leaderID == "" {
		return "", false
	}
	addr, ok := s.HTTPAddrs[leaderID]
	return addr, ok
}

// WaitForIndex blocks until index has been committed and applied, or ctx
// is done. It's how a write handler turns Propose's "accepted" into
// "durable" before answering the client.
func (s *Server) WaitForIndex(ctx context.Context, index raft.LogIndex) error {
	ch := s.waitChannelFor(index)
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// leaderID returns the current leader's NodeID, or "" if unknown.
func (s *Server) leaderID() raft.NodeID {
	_, _, leaderID := s.Raft.State()
	return leaderID
}
