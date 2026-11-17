package raft

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
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

	// HardStateDir is where currentTerm/votedFor are durably persisted
	// across restarts.
	HardStateDir string

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

	hardStateDir string

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
	log []LogEntry

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

	stopCh  chan struct{}
	stopped bool

	logger *log.Logger
	rng    *rand.Rand
}

// NewRaft constructs a node from cfg, restoring any previously persisted
// term/vote. It does not start the node; call Start for that.
func NewRaft(cfg Config) (*Raft, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: Config.ID is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("raft: Config.Transport is required")
	}
	if cfg.HardStateDir == "" {
		return nil, errors.New("raft: Config.HardStateDir is required")
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

	hs, err := storage.LoadHardState(cfg.HardStateDir)
	if err != nil {
		return nil, fmt.Errorf("raft: load hard state: %w", err)
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
		hardStateDir:       cfg.HardStateDir,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		tickInterval:       tickInterval,
		currentTerm:        Term(hs.Term),
		votedFor:           NodeID(hs.VotedFor),
		role:               Follower,
		log:                []LogEntry{{Term: 0, Index: 0}},
		stopCh:             make(chan struct{}),
		logger:             logger,
		rng:                rand.New(rand.NewSource(seedFor(cfg.ID))),
	}

	r.mu.Lock()
	r.resetElectionDeadlineLocked()
	r.mu.Unlock()

	return r, nil
}

// Start launches the node's background ticker goroutine, which drives
// election timeouts and (while leader) heartbeats.
func (r *Raft) Start() {
	go r.run()
}

// Stop permanently halts the node's background goroutine. A stopped node
// no longer sends heartbeats or starts elections; it simulates a crashed
// or killed process for tests.
func (r *Raft) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()
	close(r.stopCh)
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

func (r *Raft) lastLogIndexTermLocked() (LogIndex, Term) {
	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

func (r *Raft) lastLogIndexLocked() LogIndex {
	idx, _ := r.lastLogIndexTermLocked()
	return idx
}

func (r *Raft) persistHardStateLocked() error {
	if err := storage.SaveHardState(r.hardStateDir, storage.HardState{
		Term:     uint64(r.currentTerm),
		VotedFor: string(r.votedFor),
	}); err != nil {
		r.logf("persist hard state: %v", err)
		return err
	}
	return nil
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

func (r *Raft) logf(format string, args ...any) {
	r.logger.Printf("[%s] "+format, append([]any{r.id}, args...)...)
}

func seedFor(id NodeID) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return int64(h.Sum64()) ^ time.Now().UnixNano()
}
