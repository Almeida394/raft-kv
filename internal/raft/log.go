package raft

import (
	"encoding/binary"
	"fmt"
)

// encodeLogEntry serializes a LogEntry for WAL storage as:
//
//	[8 bytes term][8 bytes index][4 bytes commandLen][commandLen bytes command]
func encodeLogEntry(e LogEntry) []byte {
	buf := make([]byte, 8+8+4+len(e.Command))
	binary.BigEndian.PutUint64(buf[0:8], uint64(e.Term))
	binary.BigEndian.PutUint64(buf[8:16], uint64(e.Index))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(e.Command)))
	copy(buf[20:], e.Command)
	return buf
}

// decodeLogEntry parses the wire format produced by encodeLogEntry.
func decodeLogEntry(data []byte) (LogEntry, error) {
	if len(data) < 20 {
		return LogEntry{}, fmt.Errorf("log entry too short: %d bytes", len(data))
	}
	term := Term(binary.BigEndian.Uint64(data[0:8]))
	index := LogIndex(binary.BigEndian.Uint64(data[8:16]))
	cmdLen := binary.BigEndian.Uint32(data[16:20])
	if 20+int(cmdLen) > len(data) {
		return LogEntry{}, fmt.Errorf("log entry truncated reading command")
	}
	command := append([]byte(nil), data[20:20+int(cmdLen)]...)
	return LogEntry{Term: term, Index: index, Command: command}, nil
}
