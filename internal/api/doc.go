// Package api is the client-facing HTTP and gRPC API (GET/PUT/DELETE).
// Every request - reads included - is served by the current leader: a
// node that isn't the leader forwards the request rather than rejecting
// it, so any node can be used as the entry point. Writes are Proposed
// through Raft and the request blocks until the entry is committed and
// applied. This gives read-your-writes consistency through the leader
// without implementing read-index/lease-based linearizability - see the
// README for that trade-off.
package api
