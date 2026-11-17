package storage

import "testing"

func TestCommand_EncodeDecodeRoundTrip(t *testing.T) {
	want := Command{Op: OpPut, Key: []byte("hello"), Value: []byte("world")}
	got, err := DecodeCommand(want.Encode())
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if got.Op != want.Op || string(got.Key) != string(want.Key) || string(got.Value) != string(want.Value) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestDecodeCommand_RejectsTruncatedInput(t *testing.T) {
	full := Command{Op: OpPut, Key: []byte("k"), Value: []byte("v")}.Encode()
	if _, err := DecodeCommand(full[:len(full)-1]); err == nil {
		t.Fatalf("expected error decoding truncated command")
	}
}

func TestKVStore_PutGetDelete(t *testing.T) {
	kv := NewKVStore()

	if err := kv.Apply(Command{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Apply Put: %v", err)
	}
	v, ok := kv.Get([]byte("k1"))
	if !ok || string(v) != "v1" {
		t.Fatalf("Get after Put = (%q, %v), want (v1, true)", v, ok)
	}

	if err := kv.Apply(Command{Op: OpDelete, Key: []byte("k1")}); err != nil {
		t.Fatalf("Apply Delete: %v", err)
	}
	if _, ok := kv.Get([]byte("k1")); ok {
		t.Fatalf("Get after Delete: key still present")
	}
}

func TestKVStore_SnapshotRestoreRoundTrip(t *testing.T) {
	kv := NewKVStore()
	entries := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range entries {
		if err := kv.Apply(Command{Op: OpPut, Key: []byte(k), Value: []byte(v)}); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}

	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := NewKVStore()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if restored.Len() != len(entries) {
		t.Fatalf("restored.Len() = %d, want %d", restored.Len(), len(entries))
	}
	for k, want := range entries {
		got, ok := restored.Get([]byte(k))
		if !ok || string(got) != want {
			t.Fatalf("restored.Get(%q) = (%q, %v), want (%q, true)", k, got, ok, want)
		}
	}
}

func TestKVStore_RestoreReplacesPreviousState(t *testing.T) {
	kv := NewKVStore()
	if err := kv.Apply(Command{Op: OpPut, Key: []byte("stale"), Value: []byte("x")}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	snap, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	kv2 := NewKVStore()
	if err := kv2.Apply(Command{Op: OpPut, Key: []byte("should-be-gone"), Value: []byte("y")}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := kv2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, ok := kv2.Get([]byte("should-be-gone")); ok {
		t.Fatalf("Restore did not replace previous state")
	}
	if _, ok := kv2.Get([]byte("stale")); !ok {
		t.Fatalf("Restore did not bring in snapshotted state")
	}
}
