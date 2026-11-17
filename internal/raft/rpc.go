package raft

// RequestVoteArgs is sent by a candidate to ask a peer for its vote.
type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex LogIndex
	LastLogTerm  Term
}

// RequestVoteReply is a peer's response to a RequestVote.
type RequestVoteReply struct {
	Term        Term
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader both to replicate log entries and,
// with an empty Entries slice, as a heartbeat to keep followers from timing
// out and starting an election.
type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
}

// AppendEntriesReply is a follower's response to AppendEntries.
type AppendEntriesReply struct {
	Term    Term
	Success bool
}
