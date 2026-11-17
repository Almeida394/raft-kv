package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// HardState is the small piece of Raft state that must survive a restart
// and be durable *before* a node casts a vote or acknowledges an entry at
// a given term: the current term and who it voted for in that term.
//
// It changes far less often than the log, so it is persisted as its own
// small file instead of being mixed into the WAL, using a write-temp-then-
// rename pattern so a crash mid-write can never leave a half-written file
// in place of the previous valid one.
type HardState struct {
	Term     uint64
	VotedFor string // empty string means "no vote cast this term"
}

const hardStateFileName = "hardstate"

// SaveHardState durably persists hs into dir, atomically replacing any
// previous value.
func SaveHardState(dir string, hs HardState) error {
	final := filepath.Join(dir, hardStateFileName)
	tmp := final + ".tmp"

	votedForBytes := []byte(hs.VotedFor)
	buf := make([]byte, 8+4+len(votedForBytes))
	binary.BigEndian.PutUint64(buf[0:8], hs.Term)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(votedForBytes)))
	copy(buf[12:], votedForBytes)

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp hardstate: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("write temp hardstate: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp hardstate: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp hardstate: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("rename hardstate: %w", err)
	}
	return nil
}

// LoadHardState reads the persisted hard state from dir. If no hard state
// has ever been saved (first boot), it returns the zero value and a nil
// error.
func LoadHardState(dir string) (HardState, error) {
	final := filepath.Join(dir, hardStateFileName)
	data, err := os.ReadFile(final)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return HardState{}, nil
		}
		return HardState{}, fmt.Errorf("read hardstate: %w", err)
	}
	if len(data) < 12 {
		return HardState{}, fmt.Errorf("hardstate file too short: %d bytes", len(data))
	}

	term := binary.BigEndian.Uint64(data[0:8])
	n := binary.BigEndian.Uint32(data[8:12])
	if 12+int(n) > len(data) {
		return HardState{}, fmt.Errorf("hardstate file truncated")
	}
	votedFor := string(data[12 : 12+int(n)])

	return HardState{Term: term, VotedFor: votedFor}, nil
}
