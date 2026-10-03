package kv

import (
	"bytes"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	for _, c := range []Command{
		{Op: OpPut, Key: "user:42", Value: []byte("hello"), ClientID: 7, Seq: 1},
		{Op: OpPut, Key: "", Value: nil, ClientID: 1 << 63, Seq: 1 << 40},
		{Op: OpPut, Key: "k", Value: []byte{0, 1, 2, 0xff}, ClientID: 3, Seq: 9},
		{Op: OpDelete, Key: "gone", ClientID: 5, Seq: 2},
	} {
		got, err := decodeCommand(c.encode())
		if err != nil {
			t.Fatalf("decode(encode(%+v)): %v", c, err)
		}
		if got.Op != c.Op || got.Key != c.Key || got.ClientID != c.ClientID ||
			got.Seq != c.Seq || !bytes.Equal(got.Value, c.Value) {
			t.Fatalf("round trip of %+v gave %+v", c, got)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{nil, {9}, {byte(OpPut)}, {byte(OpPut), 1, 1, 50, 'k'}} {
		if _, err := decodeCommand(b); err == nil {
			t.Fatalf("decodeCommand(%v) accepted garbage", b)
		}
	}
}

func TestRetriedWriteAppliesOnce(t *testing.T) {
	sm := newStateMachine()
	sm.applyCommand(Command{Op: OpPut, Key: "k", Value: []byte("1"), ClientID: 1, Seq: 1})
	sm.applyCommand(Command{Op: OpPut, Key: "k", Value: []byte("2"), ClientID: 2, Seq: 1})
	// Client 1 retries its first write after it had already committed.
	sm.applyCommand(Command{Op: OpPut, Key: "k", Value: []byte("1"), ClientID: 1, Seq: 1})
	if got := string(sm.data["k"]); got != "2" {
		t.Fatalf("after a duplicate of an old write, k = %q, want %q", got, "2")
	}
	sm.applyCommand(Command{Op: OpDelete, Key: "k", ClientID: 1, Seq: 2})
	if _, ok := sm.data["k"]; ok {
		t.Fatal("a new write from the same client was ignored")
	}
}

func TestSnapshotKeepsSessions(t *testing.T) {
	sm := newStateMachine()
	sm.applyCommand(Command{Op: OpPut, Key: "a", Value: []byte("x"), ClientID: 1, Seq: 5})
	snap := sm.snapshot()

	restored := newStateMachine()
	if err := restored.restore(snap); err != nil {
		t.Fatal(err)
	}
	if string(restored.data["a"]) != "x" {
		t.Fatalf("restored data = %v", restored.data)
	}
	// Without the session table this retry would be applied again.
	restored.applyCommand(Command{Op: OpPut, Key: "a", Value: []byte("retry"), ClientID: 1, Seq: 5})
	if string(restored.data["a"]) != "x" {
		t.Fatal("a snapshot lost the session table: a retried write was applied twice")
	}
	if restored.digest() != sm.digest() {
		t.Fatal("restored state has a different digest")
	}
}

func TestDigestDependsOnData(t *testing.T) {
	a, b := newStateMachine(), newStateMachine()
	a.data["k"], b.data["k"] = []byte("1"), []byte("1")
	if a.digest() != b.digest() {
		t.Fatal("equal data, different digests")
	}
	// Length prefixes keep "ab"+"c" and "a"+"bc" apart.
	a.data = map[string][]byte{"ab": []byte("c")}
	b.data = map[string][]byte{"a": []byte("bc")}
	if a.digest() == b.digest() {
		t.Fatal("different data, same digest")
	}
}
