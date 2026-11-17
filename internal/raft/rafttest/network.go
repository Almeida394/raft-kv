// Package rafttest provides an in-memory raft.Transport for tests: every
// node runs in the same process and RPCs are plain function calls, but the
// Network can simulate partitions and added latency so Raft's behavior
// under network faults can be exercised deterministically and fast,
// without real sockets or sleeping for real network timeouts.
package rafttest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Almeida394/raft-kv/internal/raft"
)

// ErrUnreachable is returned by an endpoint's Send* methods when the
// target is unreachable because either side has been partitioned.
var ErrUnreachable = errors.New("rafttest: node unreachable")

// Network wires a set of raft.RPCHandler together and hands out a
// raft.Transport per node for sending to the others.
type Network struct {
	mu          sync.Mutex
	handlers    map[raft.NodeID]raft.RPCHandler
	partitioned map[raft.NodeID]bool
	delay       map[raft.NodeID]time.Duration
}

// NewNetwork creates an empty network with no nodes registered yet.
func NewNetwork() *Network {
	return &Network{
		handlers:    make(map[raft.NodeID]raft.RPCHandler),
		partitioned: make(map[raft.NodeID]bool),
		delay:       make(map[raft.NodeID]time.Duration),
	}
}

// Register associates id with the handler that should receive RPCs sent
// to it.
func (n *Network) Register(id raft.NodeID, h raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Endpoint returns the Transport that node `from` should use to reach its
// peers.
func (n *Network) Endpoint(from raft.NodeID) raft.Transport {
	return &endpoint{net: n, from: from}
}

// Partition makes id unable to send or receive any RPC, simulating a
// network partition that isolates it from the rest of the cluster.
func (n *Network) Partition(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned[id] = true
}

// Heal reconnects a previously partitioned node.
func (n *Network) Heal(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned[id] = false
}

// SetDelay adds artificial latency to every RPC sent from id, simulating a
// slow node or a slow link.
func (n *Network) SetDelay(id raft.NodeID, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay[id] = d
}

func (n *Network) connected(a, b raft.NodeID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.partitioned[a] && !n.partitioned[b]
}

func (n *Network) delayFor(id raft.NodeID) time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.delay[id]
}

func (n *Network) handlerFor(id raft.NodeID) (raft.RPCHandler, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h, ok := n.handlers[id]
	return h, ok
}

type endpoint struct {
	net  *Network
	from raft.NodeID
}

// wait blocks for the sender's simulated delay (if any) and reports
// ErrUnreachable if either endpoint is partitioned, checked both before
// and after the delay since a partition can be introduced mid-flight.
func (e *endpoint) wait(ctx context.Context, to raft.NodeID) error {
	if !e.net.connected(e.from, to) {
		return ErrUnreachable
	}
	if d := e.net.delayFor(e.from); d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if !e.net.connected(e.from, to) {
		return ErrUnreachable
	}
	return nil
}

func (e *endpoint) SendRequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	if err := e.wait(ctx, to); err != nil {
		return nil, err
	}
	h, ok := e.net.handlerFor(to)
	if !ok {
		return nil, ErrUnreachable
	}
	return h.RequestVote(args), nil
}

func (e *endpoint) SendAppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	if err := e.wait(ctx, to); err != nil {
		return nil, err
	}
	h, ok := e.net.handlerFor(to)
	if !ok {
		return nil, ErrUnreachable
	}
	return h.AppendEntries(args), nil
}

func (e *endpoint) SendInstallSnapshot(ctx context.Context, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	if err := e.wait(ctx, to); err != nil {
		return nil, err
	}
	h, ok := e.net.handlerFor(to)
	if !ok {
		return nil, ErrUnreachable
	}
	return h.InstallSnapshot(args), nil
}
