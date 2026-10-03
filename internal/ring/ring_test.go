package ring

import (
	"math"
	"strconv"
	"testing"
)

const testKeys = 100000

func key(i int) string { return "key-" + strconv.Itoa(i) }

// maxDeviation returns how far the most unevenly loaded shard is from the
// mean, as a fraction of the mean.
func maxDeviation(r *Ring, shards int) float64 {
	counts := make([]int, shards)
	for i := 0; i < testKeys; i++ {
		counts[r.Lookup(key(i))]++
	}
	mean := float64(testKeys) / float64(shards)
	worst := 0.0
	for _, c := range counts {
		worst = math.Max(worst, math.Abs(float64(c)-mean)/mean)
	}
	return worst
}

func TestLookupIsDeterministic(t *testing.T) {
	a, b := NewN(6, DefaultVNodes), NewN(6, DefaultVNodes)
	for i := 0; i < 1000; i++ {
		if a.Lookup(key(i)) != b.Lookup(key(i)) {
			t.Fatalf("rings built from the same parameters disagree on %q", key(i))
		}
	}
}

func TestEveryShardOwnsKeys(t *testing.T) {
	r := NewN(6, DefaultVNodes)
	seen := make(map[int]bool)
	for i := 0; i < testKeys; i++ {
		s := r.Lookup(key(i))
		if s < 0 || s >= 6 {
			t.Fatalf("Lookup(%q) = %d, want a shard in [0, 6)", key(i), s)
		}
		seen[s] = true
	}
	if len(seen) != 6 {
		t.Fatalf("only %d of 6 shards own keys", len(seen))
	}
}

func TestVirtualNodesBalanceLoad(t *testing.T) {
	if dev := maxDeviation(NewN(6, DefaultVNodes), 6); dev > 0.2 {
		t.Fatalf("with %d vnodes the busiest shard is %.0f%% off the mean, want <= 20%%",
			DefaultVNodes, dev*100)
	}
}

func TestMoreVirtualNodesReduceImbalance(t *testing.T) {
	one := maxDeviation(NewN(6, 1), 6)
	many := maxDeviation(NewN(6, DefaultVNodes), 6)
	if many >= one {
		t.Fatalf("imbalance with %d vnodes (%.2f) is not below imbalance with 1 vnode (%.2f)",
			DefaultVNodes, many, one)
	}
}

func TestAddingShardMovesOnlyItsShare(t *testing.T) {
	before, after := NewN(6, DefaultVNodes), NewN(7, DefaultVNodes)
	moved := 0
	for i := 0; i < testKeys; i++ {
		a, b := before.Lookup(key(i)), after.Lookup(key(i))
		if a == b {
			continue
		}
		moved++
		if b != 6 {
			t.Fatalf("%q moved from shard %d to %d; keys may only move to the new shard 6", key(i), a, b)
		}
	}
	// Ideally 1/7 of the keys move.
	if frac := float64(moved) / testKeys; frac < 0.10 || frac > 0.20 {
		t.Fatalf("adding a 7th shard moved %.1f%% of keys, want about %.1f%%", frac*100, 100.0/7)
	}
}

func TestEmptyRing(t *testing.T) {
	if s := New(nil, DefaultVNodes).Lookup("k"); s != -1 {
		t.Fatalf("Lookup on an empty ring = %d, want -1", s)
	}
}
