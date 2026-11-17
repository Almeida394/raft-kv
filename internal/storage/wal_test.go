package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndReadAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	want := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	for _, payload := range want {
		if _, err := w.Append(payload); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	records, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d", len(records), len(want))
	}
	for i, r := range records {
		if string(r.Payload) != string(want[i]) {
			t.Errorf("record %d = %q, want %q", i, r.Payload, want[i])
		}
	}
}

func TestWAL_ReadAllOnEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	records, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("got %d records, want 0", len(records))
	}
}

// TestWAL_RecoversFromTruncatedTail simulates a crash that happens mid-
// Append: the process died after writing only part of the last record's
// bytes. ReadAll must return every earlier, fully-written record and
// silently drop the partial tail instead of failing.
func TestWAL_RecoversFromTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	if _, err := w.Append([]byte("committed-1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := w.Append([]byte("committed-2")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	fullSize, err := w.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}

	// Simulate a third record that only half made it to disk before the
	// crash: append a well-formed record, then chop off its last few
	// bytes so the header claims more payload than is actually present.
	if _, err := w.Append([]byte("never-fully-written")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := os.Truncate(path, fullSize+recordHeaderSize+5); err != nil {
		t.Fatalf("os.Truncate: %v", err)
	}

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("re-OpenWAL: %v", err)
	}
	defer w2.Close()

	records, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after crash: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2 (the partial 3rd must be dropped)", len(records))
	}
	if string(records[0].Payload) != "committed-1" || string(records[1].Payload) != "committed-2" {
		t.Fatalf("unexpected records after recovery: %+v", records)
	}
}

// TestWAL_RecoversFromCorruptChecksum simulates bit-rot/corruption in an
// already "fully written" record: the length header is intact but the
// payload bytes don't match their checksum. That record and anything
// after it must be discarded.
func TestWAL_RecoversFromCorruptChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	if _, err := w.Append([]byte("good-record")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	badOffset, err := w.Append([]byte("will-be-corrupted"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a byte inside the second record's payload without touching its
	// length header, so the CRC no longer matches.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, badOffset+recordHeaderSize); err != nil {
		t.Fatalf("corrupt byte: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close after corruption: %v", err)
	}

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("re-OpenWAL: %v", err)
	}
	defer w2.Close()

	records, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after corruption: %v", err)
	}
	if len(records) != 1 || string(records[0].Payload) != "good-record" {
		t.Fatalf("got %+v, want only the first good record", records)
	}
}

func TestWAL_TruncateThenAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncate.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer w.Close()

	off0, err := w.Append([]byte("keep"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	offConflict, err := w.Append([]byte("conflicting-entry"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	_ = off0

	// Discard the conflicting entry and anything after it, as Raft does
	// when a follower's log diverges from the leader's.
	if err := w.Truncate(offConflict); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if _, err := w.Append([]byte("replacement-entry")); err != nil {
		t.Fatalf("Append after truncate: %v", err)
	}

	records, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if string(records[0].Payload) != "keep" || string(records[1].Payload) != "replacement-entry" {
		t.Fatalf("unexpected records after truncate+append: %+v", records)
	}
}

func TestWAL_ReopenPreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.wal")

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if _, err := w.Append([]byte("persisted")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("re-OpenWAL: %v", err)
	}
	defer w2.Close()

	records, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(records) != 1 || string(records[0].Payload) != "persisted" {
		t.Fatalf("got %+v, want [\"persisted\"]", records)
	}
}
