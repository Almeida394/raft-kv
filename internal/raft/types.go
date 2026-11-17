package raft

// NodeID identifies a member of the Raft cluster.
type NodeID string

// Term is a Raft election term: a logical clock that increases monotonically
// every time a new election starts.
type Term uint64

// LogIndex is a 1-based position in the replicated log. Index 0 is reserved
// for the sentinel entry that precedes the real log, so PrevLogIndex/Term
// comparisons work uniformly even when a log is empty.
type LogIndex uint64

// LogEntry is a single slot in the replicated log.
type LogEntry struct {
	Term    Term
	Index   LogIndex
	Command []byte
}

// Role is the state a node is in within the Raft state machine.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}
