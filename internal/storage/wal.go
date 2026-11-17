package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// WAL is an append-only, crash-safe write-ahead log.
//
// On-disk record format (repeated):
//
//	[4 bytes length N][4 bytes CRC32 of payload][N bytes payload]
//
// A process can crash mid-Append, leaving a truncated or corrupt record at
// the tail of the file. ReadAll treats that tail as the boundary of valid
// data rather than a fatal error: everything before it was durably
// committed (length+CRC verified), everything from that point on was never
// fully written and is discarded, matching the entries the writer could
// actually have observed as acknowledged.
type WAL struct {
	mu sync.Mutex
	f  *os.File
}

const recordHeaderSize = 8 // 4 bytes length + 4 bytes crc32

// OpenWAL opens the WAL file at path, creating it if it doesn't exist.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{f: f}, nil
}

// Append writes payload as a new record at the end of the file and returns
// the byte offset where the record starts (its header), which callers can
// later pass to Truncate to discard it and everything after it.
//
// Append does not fsync; call Sync explicitly once a batch of durability
// matters (e.g. before acknowledging a write to a client).
func (w *WAL) Append(payload []byte) (offset int64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	offset, err = w.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("seek end: %w", err)
	}

	header := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))

	if _, err := w.f.Write(header); err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}
	if _, err := w.f.Write(payload); err != nil {
		return 0, fmt.Errorf("write payload: %w", err)
	}
	return offset, nil
}

// Sync flushes the WAL's data to stable storage.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// Truncate discards the record at offset and everything after it,
// repositioning the log so the next Append starts at offset again. It is
// used to drop a conflicting suffix of the log (e.g. Raft log repair).
func (w *WAL) Truncate(offset int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Truncate(offset)
}

// Size returns the current size of the WAL file in bytes.
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// Record pairs a decoded payload with the file offset its header started
// at, so callers can map log indexes to truncation points.
type Record struct {
	Offset  int64
	Payload []byte
}

// ReadAll reads every valid record from the start of the file. If the file
// ends in a truncated or corrupt record (e.g. a crash during Append), that
// partial tail is silently discarded and the clean prefix is returned.
func (w *WAL) ReadAll() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek start: %w", err)
	}
	r := bufio.NewReader(w.f)

	var records []Record
	var offset int64

	for {
		header := make([]byte, recordHeaderSize)
		n, err := io.ReadFull(r, header)
		if err != nil {
			if n == 0 && err == io.EOF {
				break // clean end of file
			}
			break // truncated header mid-write: stop here
		}

		length := binary.BigEndian.Uint32(header[0:4])
		wantCRC := binary.BigEndian.Uint32(header[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // truncated payload mid-write: stop here
		}

		if crc32.ChecksumIEEE(payload) != wantCRC {
			break // corrupt payload: stop here, discard this and anything after
		}

		records = append(records, Record{Offset: offset, Payload: payload})
		offset += recordHeaderSize + int64(length)
	}

	return records, nil
}
