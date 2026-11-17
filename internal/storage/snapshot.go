package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Snapshot is a point-in-time copy of the state machine, paired with the
// Raft log position it corresponds to: every entry up to and including
// LastIncludedIndex (at LastIncludedTerm) is reflected in Data, so the
// log before that point can be discarded once this snapshot is durable.
type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

const snapshotFileName = "snapshot"

// SaveSnapshot durably persists snap into dir, atomically replacing any
// previous snapshot (write-temp-then-rename, same pattern as
// SaveHardState: a crash mid-write can never leave a half-written
// snapshot in place of a valid previous one).
func SaveSnapshot(dir string, snap Snapshot) error {
	final := filepath.Join(dir, snapshotFileName)
	tmp := final + ".tmp"

	buf := make([]byte, 8+8+len(snap.Data))
	binary.BigEndian.PutUint64(buf[0:8], snap.LastIncludedIndex)
	binary.BigEndian.PutUint64(buf[8:16], snap.LastIncludedTerm)
	copy(buf[16:], snap.Data)

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp snapshot: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("write temp snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp snapshot: %w", err)
	}
	if err := RenameWithRetry(tmp, final); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}

// LoadSnapshot reads the persisted snapshot from dir. If none has ever
// been saved, it returns ok=false and a nil error.
func LoadSnapshot(dir string) (snap Snapshot, ok bool, err error) {
	final := filepath.Join(dir, snapshotFileName)
	data, err := os.ReadFile(final)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, fmt.Errorf("read snapshot: %w", err)
	}
	if len(data) < 16 {
		return Snapshot{}, false, fmt.Errorf("snapshot file too short: %d bytes", len(data))
	}

	lastIncludedIndex := binary.BigEndian.Uint64(data[0:8])
	lastIncludedTerm := binary.BigEndian.Uint64(data[8:16])
	payload := append([]byte(nil), data[16:]...)

	return Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              payload,
	}, true, nil
}
