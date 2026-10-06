package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func makeEntries(from, to, term int) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Index: i, Term: term, Data: []byte(fmt.Sprintf("e%d-t%d", i, term))})
	}
	return out
}

func mustOpen(t *testing.T, dir string) (*WAL, *State) {
	t.Helper()
	w, st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w, st
}

func mustClose(t *testing.T, w *WAL) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func checkEntries(t *testing.T, st *State, want []Entry) {
	t.Helper()
	if len(st.Entries) != len(want) {
		t.Fatalf("recovered %d entries, want %d", len(st.Entries), len(want))
	}
	for i := range want {
		got := st.Entries[i]
		if got.Index != want[i].Index || got.Term != want[i].Term || !bytes.Equal(got.Data, want[i].Data) {
			t.Fatalf("entry %d = {%d %d %q}, want {%d %d %q}", i,
				got.Index, got.Term, got.Data, want[i].Index, want[i].Term, want[i].Data)
		}
	}
}

func TestReplayRecoversStateAndEntries(t *testing.T) {
	dir := t.TempDir()
	w, st := mustOpen(t, dir)
	if st.HasHardState || len(st.Entries) != 0 {
		t.Fatalf("new log is not empty: %+v", st)
	}
	if err := w.SaveHardState(HardState{Term: 3, Vote: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(makeEntries(1, 5, 3)); err != nil {
		t.Fatal(err)
	}
	if err := w.SaveHardState(HardState{Term: 4, Vote: -1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	_, st = mustOpen(t, dir)
	if !st.HasHardState || st.HardState != (HardState{Term: 4, Vote: -1}) {
		t.Fatalf("hard state = %+v, want the last one written", st.HardState)
	}
	checkEntries(t, st, makeEntries(1, 5, 3))
}

func TestLaterEntryReplacesConflictingSuffix(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Append(makeEntries(1, 5, 1))
	// A new leader's entries at 3 and 4 replace 3, 4 and 5.
	w.Append(makeEntries(3, 4, 2))
	mustClose(t, w)

	_, st := mustOpen(t, dir)
	want := append(makeEntries(1, 2, 1), makeEntries(3, 4, 2)...)
	checkEntries(t, st, want)
}

func TestTornTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Append(makeEntries(1, 3, 1))
	mustClose(t, w)

	// Simulate a crash in the middle of writing entry 3.
	path := filepath.Join(dir, logName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	w, st := mustOpen(t, dir)
	checkEntries(t, st, makeEntries(1, 2, 1))

	// New records must land after the last good one, not after the garbage.
	w.Append(makeEntries(3, 3, 2))
	mustClose(t, w)
	_, st = mustOpen(t, dir)
	checkEntries(t, st, append(makeEntries(1, 2, 1), makeEntries(3, 3, 2)...))
}

func TestChecksumMismatchStopsReplay(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Append(makeEntries(1, 3, 1))
	mustClose(t, w)

	path := filepath.Join(dir, logName)
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[len(buf)-1] ^= 0xff // corrupt the payload of entry 3
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	_, st := mustOpen(t, dir)
	checkEntries(t, st, makeEntries(1, 2, 1))
}

func TestGapInIndexesIsAnError(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Append(makeEntries(1, 1, 1))
	w.Append(makeEntries(3, 3, 1))
	mustClose(t, w)

	if _, _, err := Open(dir); err == nil {
		t.Fatal("Open accepted a log with a gap between entries 1 and 3")
	}
}

func TestCompactKeepsOnlyEntriesAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Append(makeEntries(1, 10, 1))
	hs := HardState{Term: 2, Vote: 0}
	if err := w.Compact(6, 1, hs, makeEntries(7, 10, 1)); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// The log stays writable after the swap.
	w.Append(makeEntries(11, 12, 2))
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	mustClose(t, w)

	_, st := mustOpen(t, dir)
	if st.SnapIndex != 6 || st.SnapTerm != 1 {
		t.Fatalf("snapshot marker = (%d, %d), want (6, 1)", st.SnapIndex, st.SnapTerm)
	}
	if st.HardState != hs {
		t.Fatalf("hard state = %+v, want %+v", st.HardState, hs)
	}
	checkEntries(t, st, append(makeEntries(7, 10, 1), makeEntries(11, 12, 2)...))
}

func TestCompactWithNoRemainingEntries(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Append(makeEntries(1, 4, 1))
	if err := w.Compact(4, 1, HardState{Term: 1, Vote: 2}, nil); err != nil {
		t.Fatal(err)
	}
	w.Append(makeEntries(5, 5, 1))
	mustClose(t, w)

	_, st := mustOpen(t, dir)
	if st.SnapIndex != 4 {
		t.Fatalf("SnapIndex = %d, want 4", st.SnapIndex)
	}
	checkEntries(t, st, makeEntries(5, 5, 1))
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if i, _, data, err := LoadSnapshot(dir); err != nil || i != 0 || data != nil {
		t.Fatalf("LoadSnapshot on an empty dir = (%d, %v, %v), want (0, nil, nil)", i, data, err)
	}
	if err := SaveSnapshot(dir, 42, 7, []byte("state")); err != nil {
		t.Fatal(err)
	}
	// Replacing an existing snapshot works too.
	if err := SaveSnapshot(dir, 50, 8, []byte("newer state")); err != nil {
		t.Fatal(err)
	}
	i, term, data, err := LoadSnapshot(dir)
	if err != nil || i != 50 || term != 8 || string(data) != "newer state" {
		t.Fatalf("LoadSnapshot = (%d, %d, %q, %v), want (50, 8, \"newer state\", nil)", i, term, data, err)
	}
}

func TestCorruptSnapshotIsAnError(t *testing.T) {
	dir := t.TempDir()
	if err := SaveSnapshot(dir, 1, 1, []byte("state")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, snapshotName)
	buf, _ := os.ReadFile(path)
	buf[len(buf)-1] ^= 0xff
	os.WriteFile(path, buf, 0o644)
	if _, _, _, err := LoadSnapshot(dir); err == nil {
		t.Fatal("LoadSnapshot accepted a corrupt snapshot")
	}
}

func TestMethodsFailAfterClose(t *testing.T) {
	w, _ := mustOpen(t, t.TempDir())
	mustClose(t, w)
	if err := w.Append(makeEntries(1, 1, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close = %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close = %v, want ErrClosed", err)
	}
}
