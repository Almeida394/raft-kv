package raft

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"path/filepath"
	"sync"
	"time"

	"github.com/Almeida394/raft-kv/internal/storage"
)

// Config configures a single Raft node. Peers lists every other node in
// the cluster (not including this one).
type Config struct {
	ID        NodeID
	Peers     []NodeID
	Transport Transport

	// DataDir is where this node's durable state lives: currentTerm and
	// votedFor (the "hard state") and the replicated log (raft.wal).
	DataDir string

	// ApplyFn is invoked, in log order, for every entry once it has been
	// committed (replicated to a majority). It is called from a single
	// dedicated goroutine, never concurrently, and never holding the
	// node's internal lock, so it may safely do its own I/O (e.g.
	// applying to a KV state machine). No-op entries (nil Command) that
	// the leader appends on election are not passed to ApplyFn.
	ApplyFn func(LogEntry)

	// ElectionTimeoutMin/Max bound the randomized election timeout.
	// Defaults: 150ms / 300ms.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is how often a leader sends AppendEntries to
	// keep followers from timing out. Default: ElectionTimeoutMin / 3.
	HeartbeatInterval time.Duration

	// Logger receives diagnostic output. Defaults to a no-op logger.
	Logger *log.Logger
}

// Raft is a single node's view of the cluster. All mutable state is
// guarded by mu; methods suffixed Locked assume the caller already holds
// it.
type Raft struct {
	mu sync.Mutex

	id        NodeID
	peers     []NodeID
	transport Transport

	dataDir string
	wal     *storage.WAL

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	tickInterval       time.Duration

	// Persistent state (must be written to disk before it is relied upon
	// by an RPC reply — see persistHardStateLocked callers).
	currentTerm Term
	votedFor    NodeID

	// log[0] is a zero-value sentinel (Term 0, Index 0) so prevLogIndex
	// comparisons work uniformly even when no real entries exist yet.
	// Invariant: log[i].Index == LogIndex(i) for every i (no gaps; Phase
	// 4 snapshotting is the only thing allowed to break this by
	// shifting the base). logOffsets[i] is the WAL byte offset of
	// log[i+1]'s record (logOffsets has no entry for the sentinel).
	log        []LogEntry
	logOffsets []int64

	// Volatile state.
	role        Role
	leaderID    NodeID
	commitIndex LogIndex
	lastApplied LogIndex

	// Leader-only volatile state (reset on every accession to leader).
	nextIndex  map[NodeID]LogIndex
	matchIndex map[NodeID]LogIndex

	electionDeadline time.Time
	nextHeartbeat    time.Time

	applyFn func(LogEntry)
	applyCh chan struct{}

	stopCh  chan struct{}
	stopped bool

	logger *log.Logger
	rng    *rand.Rand
}

// NewRaft constructs a node from cfg, restoring any previously persisted
// term, vote, and log from DataDir. It does not start the node; call
// Start for that.
func NewRaft(cfg Config) (*Raft, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: Config.ID is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("raft: Config.Transport is required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("raft: Config.DataDir is required")
	}
	for _, p := range cfg.Peers {
		if p == cfg.ID {
			return nil, fmt.Errorf("raft: Config.Peers must not include self (%s)", cfg.ID)
		}
	}

	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin * 2
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.ElectionTimeoutMin / 3
	}

	hs, err := storage.LoadHardState(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("raft: load hard state: %w", err)
	}

	wal, err := storage.OpenWAL(filepath.Join(cfg.DataDir, "raft.wal"))
	if err != nil {
		return nil, fmt.Errorf("raft: open wal: %w", err)
	}
	records, err := wal.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("raft: read wal: %w", err)
	}

	raftLog := []LogEntry{{Term: 0, Index: 0}}
	offsets := make([]int64, 0, len(records))
	for _, rec := range records {
		entry, err := decodeLogEntry(rec.Payload)
		if err != nil {
			return nil, fmt.Errorf("raft: decode persisted log entry: %w", err)
		}
		raftLog = append(raftLog, entry)
		offsets = append(offsets, rec.Offset)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	tickInterval := cfg.ElectionTimeoutMin / 5
	if tickInterval > 10*time.Millisecond {
		tickInterval = 10 * time.Millisecond
	}
	if tickInterval <= 0 {
		tickInterval = time.Millisecond
	}

	r := &Raft{
		id:                 cfg.ID,
		peers:              append([]NodeID(nil), cfg.Peers...),
		transport:          cfg.Transport,
		dataDir:            cfg.DataDir,
		wal:                wal,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		tickInterval:       tickInterval,
		currentTerm:        Term(hs.Term),
		votedFor:           NodeID(hs.VotedFor),
		role:               Follower,
		log:                raftLog,
		logOffsets:         offsets,
		applyFn:            cfg.ApplyFn,
		applyCh:            make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		logger:             logger,
		rng:                rand.New(rand.NewSource(seedFor(cfg.ID))),
	}

	r.mu.Lock()
	r.resetElectionDeadlineLocked()
	r.mu.Unlock()

	return r, nil
}

// Start launches the node's background goroutines: the ticker that drives
// election timeouts and (while leader) heartbeats/replication, and the
// apply loop that feeds committed entries to ApplyFn.
func (r *Raft) Start() {
	go r.run()
	go r.runApplyLoop()
}

// Stop permanently halts the node's background goroutines and releases
// its WAL file handle. A stopped node no longer sends heartbeats,
// replicates, or starts elections; it simulates a crashed or killed
// process for tests.
func (r *Raft) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()
	close(r.stopCh)
	_ = r.wal.Close()
}

// ID returns the node's own identifier.
func (r *Raft) ID() NodeID {
	return r.id
}

// State returns the node's current term, role, and (if known) the current
// leader's ID.
func (r *Raft) State() (Term, Role, NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role, r.leaderID
}

// IsLeader reports whether the node currently believes it is the leader.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role == Leader
}

// CommitIndex returns the highest log index known to be committed.
func (r *Raft) CommitIndex() LogIndex {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

// LogEntries returns a copy of the node's log, including the sentinel at
// index 0. It's meant for tests and debugging, not the hot path.
func (r *Raft) LogEntries() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]LogEntry(nil), r.log...)
}

func (r *Raft) lastLogIndexTermLocked() (LogIndex, Term) {
	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

func (r *Raft) lastLogIndexLocked() LogIndex {
	idx, _ := r.lastLogIndexTermLocked()
	return idx
}

func (r *Raft) persistHardStateLocked() error {
	if err := storage.SaveHardState(r.dataDir, storage.HardState{
		Term:     uint64(r.currentTerm),
		VotedFor: string(r.votedFor),
	}); err != nil {
		r.logf("persist hard state: %v", err)
		return err
	}
	return nil
}

// appendLogEntryLocked durably persists entry to the WAL before adding it
// to the in-memory log, so a crash right after this call still leaves the
// entry recoverable on restart.
func (r *Raft) appendLogEntryLocked(entry LogEntry) error {
	offset, err := r.wal.Append(encodeLogEntry(entry))
	if err != nil {
		return fmt.Errorf("append log entry to wal: %w", err)
	}
	if err := r.wal.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	r.log = append(r.log, entry)
	r.logOffsets = append(r.logOffsets, offset)
	return nil
}

// proposeLocked appends a new entry for command at the next log index in
// the current term. Used both for client-proposed commands (Propose) and
// for the no-op entry a new leader appends on election.
func (r *Raft) proposeLocked(command []byte) (LogEntry, error) {
	entry := LogEntry{
		Term:    r.currentTerm,
		Index:   r.lastLogIndexLocked() + 1,
		Command: command,
	}
	if err := r.appendLogEntryLocked(entry); err != nil {
		return LogEntry{}, err
	}
	return entry, nil
}

// truncateLogLocked discards fromIndex and everything after it, both from
// the WAL and the in-memory log, used when a follower's log conflicts
// with the leader's and must be repaired.
func (r *Raft) truncateLogLocked(fromIndex LogIndex) {
	if fromIndex < 1 || int(fromIndex) >= len(r.log) {
		return
	}
	offset := r.logOffsets[fromIndex-1]
	if err := r.wal.Truncate(offset); err != nil {
		r.logf("truncate wal at offset %d: %v", offset, err)
	}
	r.log = r.log[:fromIndex]
	r.logOffsets = r.logOffsets[:fromIndex-1]
}

func (r *Raft) resetElectionDeadlineLocked() {
	span := r.electionTimeoutMax - r.electionTimeoutMin
	jitter := time.Duration(0)
	if span > 0 {
		jitter = time.Duration(r.rng.Int63n(int64(span)))
	}
	r.electionDeadline = time.Now().Add(r.electionTimeoutMin + jitter)
}

// rpcTimeoutLocked bounds how long a single outgoing RPC may block,
// independent of the caller's own election/heartbeat cadence, so a slow
// or unreachable peer can't stall the node's control loop.
func (r *Raft) rpcTimeoutLocked() time.Duration {
	return r.electionTimeoutMin
}

// signalApply wakes the apply loop if it isn't already pending a run; it
// never blocks, so it's safe to call while holding mu.
func (r *Raft) signalApply() {
	select {
	case r.applyCh <- struct{}{}:
	default:
	}
}

// runApplyLoop feeds every newly committed entry to ApplyFn, in order,
// from a single goroutine so ApplyFn never needs its own synchronization.
func (r *Raft) runApplyLoop() {
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.applyCh:
			r.applyPending()
		}
	}
}

func (r *Raft) applyPending() {
	for {
		r.mu.Lock()
		if r.lastApplied >= r.commitIndex {
			r.mu.Unlock()
			return
		}
		r.lastApplied++
		entry := r.log[r.lastApplied]
		fn := r.applyFn
		r.mu.Unlock()

		if fn != nil && len(entry.Command) > 0 {
			fn(entry)
		}
	}
}

func (r *Raft) logf(format string, args ...any) {
	r.logger.Printf("[%s] "+format, append([]any{r.id}, args...)...)
}

func seedFor(id NodeID) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return int64(h.Sum64()) ^ time.Now().UnixNano()
}
