package storage

import "testing"

func TestSnapshot_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()

	want := Snapshot{LastIncludedIndex: 42, LastIncludedTerm: 3, Data: []byte("kv-state-bytes")}
	if err := SaveSnapshot(dir, want); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	got, ok, err := LoadSnapshot(dir)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if !ok {
		t.Fatalf("LoadSnapshot: ok=false, want true")
	}
	if got.LastIncludedIndex != want.LastIncludedIndex || got.LastIncludedTerm != want.LastIncludedTerm || string(got.Data) != string(want.Data) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSnapshot_LoadMissingFileReturnsNotOK(t *testing.T) {
	dir := t.TempDir()

	_, ok, err := LoadSnapshot(dir)
	if err != nil {
		t.Fatalf("LoadSnapshot on missing file: %v", err)
	}
	if ok {
		t.Fatalf("LoadSnapshot: ok=true on missing file, want false")
	}
}

func TestSnapshot_SaveOverwritesPreviousSnapshot(t *testing.T) {
	dir := t.TempDir()

	if err := SaveSnapshot(dir, Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("old")}); err != nil {
		t.Fatalf("first SaveSnapshot: %v", err)
	}
	if err := SaveSnapshot(dir, Snapshot{LastIncludedIndex: 2, LastIncludedTerm: 1, Data: []byte("new")}); err != nil {
		t.Fatalf("second SaveSnapshot: %v", err)
	}

	got, ok, err := LoadSnapshot(dir)
	if err != nil || !ok {
		t.Fatalf("LoadSnapshot: ok=%v, err=%v", ok, err)
	}
	if got.LastIncludedIndex != 2 || string(got.Data) != "new" {
		t.Fatalf("got %+v, want the newer snapshot", got)
	}
}
