package raft

import (
	"fmt"
	"path/filepath"

	"github.com/Almeida394/raft-kv/internal/storage"
)

// StateMachine is whatever a Raft node is replicating commands into. It
// must be able to serialize its entire current state (Snapshot) and
// rebuild itself from that serialized form (Restore), so the log entries
// that produced that state can be discarded. storage.KVStore already
// satisfies this interface.
type StateMachine interface {
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// InstallSnapshotArgs is sent by the leader to a follower that has fallen
// far enough behind that the entries it needs have already been
// compacted out of the leader's log: instead of replaying history, the
// follower just adopts the leader's state wholesale.
type InstallSnapshotArgs struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	Data              []byte
}

// InstallSnapshotReply is a follower's response to InstallSnapshot.
type InstallSnapshotReply struct {
	Term Term
}

// InstallSnapshot handles an incoming snapshot from the leader.
func (r *Raft) InstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		// A stopped node simulates a crashed/killed process: it must
		// not still touch disk (SaveSnapshot, WAL compaction) just
		// because a request sent before it stopped arrives late.
		return &InstallSnapshotReply{Term: r.currentTerm}
	}

	if args.Term < r.currentTerm {
		return &InstallSnapshotReply{Term: r.currentTerm}
	}
	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}
	r.leaderID = args.LeaderID
	r.resetElectionDeadlineLocked()

	if args.LastIncludedIndex <= r.lastIncludedIndex {
		// We already have an equal or newer snapshot; nothing to do.
		return &InstallSnapshotReply{Term: r.currentTerm}
	}

	if r.stateMachine != nil {
		if err := r.stateMachine.Restore(args.Data); err != nil {
			r.logf("install snapshot: restore state machine: %v", err)
			return &InstallSnapshotReply{Term: r.currentTerm}
		}
	}
	if err := storage.SaveSnapshot(r.dataDir, storage.Snapshot{
		LastIncludedIndex: uint64(args.LastIncludedIndex),
		LastIncludedTerm:  uint64(args.LastIncludedTerm),
		Data:              args.Data,
	}); err != nil {
		r.logf("install snapshot: persist snapshot: %v", err)
		return &InstallSnapshotReply{Term: r.currentTerm}
	}

	// Keep any suffix of our own log that's still consistent with (i.e.
	// comes after) the snapshot; otherwise the snapshot fully replaces
	// what we had.
	var retained []LogEntry
	if pos := int(args.LastIncludedIndex - r.lastIncludedIndex); pos >= 0 && pos < len(r.log) && r.log[pos].Term == args.LastIncludedTerm {
		retained = append([]LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}, r.log[pos+1:]...)
	} else {
		retained = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}

	if err := r.compactWALLocked(retained); err != nil {
		r.logf("install snapshot: wal compaction: %v", err)
		return &InstallSnapshotReply{Term: r.currentTerm}
	}

	r.log = retained
	r.lastIncludedIndex = args.LastIncludedIndex
	r.lastIncludedTerm = args.LastIncludedTerm
	if r.commitIndex < args.LastIncludedIndex {
		r.commitIndex = args.LastIncludedIndex
	}
	if r.lastApplied < args.LastIncludedIndex {
		r.lastApplied = args.LastIncludedIndex
	}

	return &InstallSnapshotReply{Term: r.currentTerm}
}

// maybeSnapshotLocked takes a new snapshot once enough entries have been
// applied past the last one, bounding how much log a restart or a
// far-behind follower ever needs to replay.
func (r *Raft) maybeSnapshotLocked() {
	if r.stopped || r.stateMachine == nil || r.snapshotThreshold <= 0 {
		return
	}
	if int(r.lastApplied-r.lastIncludedIndex) < r.snapshotThreshold {
		return
	}
	r.takeSnapshotLocked(r.lastApplied)
}

// takeSnapshotLocked snapshots the state machine as of upTo and discards
// the log (and WAL) up to that point.
func (r *Raft) takeSnapshotLocked(upTo LogIndex) {
	pos := int(upTo - r.lastIncludedIndex)
	if pos <= 0 || pos >= len(r.log) {
		return
	}
	term := r.log[pos].Term

	data, err := r.stateMachine.Snapshot()
	if err != nil {
		r.logf("snapshot: state machine snapshot failed: %v", err)
		return
	}
	if err := storage.SaveSnapshot(r.dataDir, storage.Snapshot{
		LastIncludedIndex: uint64(upTo),
		LastIncludedTerm:  uint64(term),
		Data:              data,
	}); err != nil {
		r.logf("snapshot: save failed: %v", err)
		return
	}

	retained := append([]LogEntry{{Term: term, Index: upTo}}, r.log[pos+1:]...)
	if err := r.compactWALLocked(retained); err != nil {
		r.logf("snapshot: wal compaction failed: %v", err)
		return
	}

	r.log = retained
	r.lastIncludedIndex = upTo
	r.lastIncludedTerm = term
	r.logf("snapshot: compacted log up to index %d", upTo)
}

// compactWALLocked rewrites the WAL to contain only retained[1:] (
// retained[0] is the new sentinel entry and is never itself persisted,
// matching how the original index-0 sentinel was never written either),
// via write-new-file-then-atomically-rename so a crash mid-compaction
// leaves either the old, complete WAL or the new, compacted one — never
// a half-written file.
func (r *Raft) compactWALLocked(retained []LogEntry) error {
	if r.stopped {
		// The node is shutting down: Stop() has already decided which
		// WAL handle it will close. Starting a compaction now would
		// leave a freshly-opened file nobody ever closes.
		return fmt.Errorf("raft: node stopped")
	}

	finalPath := filepath.Join(r.dataDir, "raft.wal")
	tmpPath := finalPath + ".compact.tmp"

	if err := storage.RemoveIfExists(tmpPath); err != nil {
		return fmt.Errorf("remove stale temp wal: %w", err)
	}

	tmpWAL, err := storage.OpenWAL(tmpPath)
	if err != nil {
		return fmt.Errorf("open temp wal: %w", err)
	}

	offsets := make([]int64, 0, len(retained)-1)
	for _, entry := range retained[1:] {
		offset, err := tmpWAL.Append(encodeLogEntry(entry))
		if err != nil {
			tmpWAL.Close()
			return fmt.Errorf("write retained entry: %w", err)
		}
		offsets = append(offsets, offset)
	}
	if err := tmpWAL.Sync(); err != nil {
		tmpWAL.Close()
		return fmt.Errorf("sync temp wal: %w", err)
	}
	if err := tmpWAL.Close(); err != nil {
		return fmt.Errorf("close temp wal: %w", err)
	}

	if err := r.wal.Close(); err != nil {
		return fmt.Errorf("close old wal: %w", err)
	}
	if err := storage.RenameWithRetry(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename temp wal over old wal: %w", err)
	}

	newWAL, err := storage.OpenWAL(finalPath)
	if err != nil {
		return fmt.Errorf("reopen compacted wal: %w", err)
	}

	r.wal = newWAL
	r.logOffsets = offsets
	return nil
}
