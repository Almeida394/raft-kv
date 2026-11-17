package storage

import "testing"

func TestHardState_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()

	want := HardState{Term: 7, VotedFor: "node-2"}
	if err := SaveHardState(dir, want); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}

	got, err := LoadHardState(dir)
	if err != nil {
		t.Fatalf("LoadHardState: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestHardState_LoadMissingFileIsZeroValue(t *testing.T) {
	dir := t.TempDir()

	got, err := LoadHardState(dir)
	if err != nil {
		t.Fatalf("LoadHardState on missing file: %v", err)
	}
	if got != (HardState{}) {
		t.Fatalf("got %+v, want zero value on first boot", got)
	}
}

func TestHardState_SaveOverwritesPreviousValue(t *testing.T) {
	dir := t.TempDir()

	if err := SaveHardState(dir, HardState{Term: 1, VotedFor: "node-1"}); err != nil {
		t.Fatalf("first SaveHardState: %v", err)
	}
	if err := SaveHardState(dir, HardState{Term: 2, VotedFor: ""}); err != nil {
		t.Fatalf("second SaveHardState: %v", err)
	}

	got, err := LoadHardState(dir)
	if err != nil {
		t.Fatalf("LoadHardState: %v", err)
	}
	want := HardState{Term: 2, VotedFor: ""}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
