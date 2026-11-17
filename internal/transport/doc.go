// Package transport wires the Raft consensus RPCs (internal/raft) onto
// real gRPC connections between cluster nodes, using the generated code
// in raftpb. It implements raft.Transport (Client, for outgoing calls)
// and adapts raft.RPCHandler to the generated gRPC service (Server, for
// incoming calls).
package transport
