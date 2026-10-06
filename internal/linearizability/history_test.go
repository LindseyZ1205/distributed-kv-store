package linearizability

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// The checker is only worth something if it rejects bad histories, so
// these tests feed it histories with known answers.

func check(t *testing.T, r *Recorder) porcupine.CheckResult {
	t.Helper()
	res, _ := r.Check(10 * time.Second)
	return res
}

func TestAcceptsSequentialHistory(t *testing.T) {
	r := NewRecorder()
	r.Record(0, Input{Kind: Put, Key: "k", Value: "a"}, Output{}, 0, 10)
	r.Record(1, Input{Kind: Get, Key: "k"}, Output{Found: true, Value: "a"}, 20, 30)
	r.Record(0, Input{Kind: Delete, Key: "k"}, Output{}, 40, 50)
	r.Record(1, Input{Kind: Get, Key: "k"}, Output{}, 60, 70)
	if res := check(t, r); res != porcupine.Ok {
		t.Fatalf("sequential history: got %s, want Ok", res)
	}
}

func TestRejectsStaleRead(t *testing.T) {
	r := NewRecorder()
	r.Record(0, Input{Kind: Put, Key: "k", Value: "a"}, Output{}, 0, 10)
	r.Record(0, Input{Kind: Put, Key: "k", Value: "b"}, Output{}, 20, 30)
	// Starts after the second put finished, yet sees the first value:
	// what a deposed leader serving reads from its own state would return.
	r.Record(1, Input{Kind: Get, Key: "k"}, Output{Found: true, Value: "a"}, 40, 50)
	if res := check(t, r); res != porcupine.Illegal {
		t.Fatalf("stale read: got %s, want Illegal", res)
	}
}

func TestRejectsLostAcknowledgedWrite(t *testing.T) {
	r := NewRecorder()
	r.Record(0, Input{Kind: Put, Key: "k", Value: "a"}, Output{}, 0, 10)
	r.Record(1, Input{Kind: Get, Key: "k"}, Output{}, 20, 30) // not found
	if res := check(t, r); res != porcupine.Illegal {
		t.Fatalf("lost write: got %s, want Illegal", res)
	}
}

func TestConcurrentOperationsMayOrderEitherWay(t *testing.T) {
	r := NewRecorder()
	r.Record(0, Input{Kind: Put, Key: "k", Value: "a"}, Output{}, 0, 100)
	// Overlaps the put, so seeing either the old or the new state is fine.
	r.Record(1, Input{Kind: Get, Key: "k"}, Output{}, 10, 20)
	r.Record(2, Input{Kind: Get, Key: "k"}, Output{Found: true, Value: "a"}, 30, 40)
	if res := check(t, r); res != porcupine.Ok {
		t.Fatalf("concurrent history: got %s, want Ok", res)
	}
}

func TestUnknownWriteMayOrMayNotTakeEffect(t *testing.T) {
	took := NewRecorder()
	took.RecordUnknown(0, Input{Kind: Put, Key: "k", Value: "a"}, 0)
	took.Record(1, Input{Kind: Get, Key: "k"}, Output{Found: true, Value: "a"}, 50, 60)
	if res := check(t, took); res != porcupine.Ok {
		t.Fatalf("unknown write observed: got %s, want Ok", res)
	}

	didNot := NewRecorder()
	didNot.RecordUnknown(0, Input{Kind: Put, Key: "k", Value: "a"}, 0)
	didNot.Record(1, Input{Kind: Get, Key: "k"}, Output{}, 50, 60)
	if res := check(t, didNot); res != porcupine.Ok {
		t.Fatalf("unknown write not observed: got %s, want Ok", res)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	r := NewRecorder()
	r.Record(0, Input{Kind: Put, Key: "x", Value: "1"}, Output{}, 0, 10)
	r.Record(1, Input{Kind: Get, Key: "y"}, Output{}, 20, 30)
	r.Record(1, Input{Kind: Get, Key: "x"}, Output{Found: true, Value: "1"}, 40, 50)
	if res := check(t, r); res != porcupine.Ok {
		t.Fatalf("two keys: got %s, want Ok", res)
	}
}
