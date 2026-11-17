package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// Op identifies the kind of mutation a Command applies to the KV state
// machine.
type Op byte

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

// Command is the unit of replication: it's what gets written to the Raft
// log and, once committed, applied to the KVStore. Encode/Decode define
// the wire format stored inside WAL records and snapshots.
type Command struct {
	Op    Op
	Key   []byte
	Value []byte // unused for OpDelete
}

// Encode serializes the command as:
//
//	[1 byte op][4 bytes keyLen][keyLen bytes key][4 bytes valueLen][valueLen bytes value]
func (c Command) Encode() []byte {
	buf := make([]byte, 1+4+len(c.Key)+4+len(c.Value))
	buf[0] = byte(c.Op)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(c.Key)))
	off := 5
	copy(buf[off:], c.Key)
	off += len(c.Key)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(c.Value)))
	off += 4
	copy(buf[off:], c.Value)
	return buf
}

// DecodeCommand parses the wire format produced by Command.Encode.
func DecodeCommand(data []byte) (Command, error) {
	if len(data) < 1+4 {
		return Command{}, fmt.Errorf("command too short: %d bytes", len(data))
	}
	op := Op(data[0])
	keyLen := binary.BigEndian.Uint32(data[1:5])
	off := 5
	if off+int(keyLen)+4 > len(data) {
		return Command{}, fmt.Errorf("command truncated reading key")
	}
	key := data[off : off+int(keyLen)]
	off += int(keyLen)
	valueLen := binary.BigEndian.Uint32(data[off : off+4])
	off += 4
	if off+int(valueLen) > len(data) {
		return Command{}, fmt.Errorf("command truncated reading value")
	}
	value := data[off : off+int(valueLen)]
	return Command{Op: op, Key: key, Value: value}, nil
}

// KVStore is the replicated state machine: a plain in-memory map that
// commands are applied to once the Raft layer has committed them. It has
// no durability of its own beyond the periodic snapshots it can produce —
// durability comes from replaying the WAL of commands that led to this
// state.
type KVStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewKVStore creates an empty KV state machine.
func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string][]byte)}
}

// Apply mutates the store according to cmd. It is meant to be called only
// with commands that have already been committed by Raft, in log order.
func (s *KVStore) Apply(cmd Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		s.data[string(cmd.Key)] = append([]byte(nil), cmd.Value...)
	case OpDelete:
		delete(s.data, string(cmd.Key))
	default:
		return fmt.Errorf("unknown op %d", cmd.Op)
	}
	return nil
}

// Get returns the value for key and whether it was present.
func (s *KVStore) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// Snapshot serializes the entire current state as a sequence of:
//
//	[4 bytes keyLen][key][4 bytes valueLen][value]
//
// so it can be written to disk and later restored, letting the log that
// produced this state be discarded (compacted).
func (s *KVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var size int
	for k, v := range s.data {
		size += 4 + len(k) + 4 + len(v)
	}

	buf := make([]byte, size)
	off := 0
	for k, v := range s.data {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(k)))
		off += 4
		copy(buf[off:], k)
		off += len(k)
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(v)))
		off += 4
		copy(buf[off:], v)
		off += len(v)
	}
	return buf, nil
}

// Restore replaces the store's contents with the state encoded by
// Snapshot.
func (s *KVStore) Restore(data []byte) error {
	newData := make(map[string][]byte)
	off := 0
	for off < len(data) {
		if off+4 > len(data) {
			return fmt.Errorf("snapshot truncated reading key length")
		}
		keyLen := int(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4
		if off+keyLen+4 > len(data) {
			return fmt.Errorf("snapshot truncated reading key/value length")
		}
		key := string(data[off : off+keyLen])
		off += keyLen
		valueLen := int(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4
		if off+valueLen > len(data) {
			return fmt.Errorf("snapshot truncated reading value")
		}
		value := append([]byte(nil), data[off:off+valueLen]...)
		off += valueLen
		newData[key] = value
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = newData
	return nil
}

// Len returns the number of keys currently stored (mainly for tests).
func (s *KVStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
