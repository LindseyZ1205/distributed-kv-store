// Package ring implements consistent hashing with virtual nodes.
//
// The ring maps keys to shards. Clients and servers build identical rings
// from the same parameters, so they agree on where every key lives without
// any coordination. Each shard places many points ("virtual nodes") on a
// 64-bit hash ring and owns the arc that ends at each of its points. Many
// small arcs per shard even out the load, and adding a shard takes keys
// only from the arcs its new points split, about 1/(n+1) of the keyspace.
package ring

import (
	"sort"
	"strconv"
)

// DefaultVNodes is the number of points each shard places on the ring.
const DefaultVNodes = 128

type point struct {
	hash  uint64
	shard int
}

// Ring is an immutable consistent-hash ring. It is safe for concurrent use.
type Ring struct {
	vnodes int
	shards []int
	points []point // sorted by hash
}

// New builds a ring in which every shard owns vnodes points.
func New(shards []int, vnodes int) *Ring {
	if vnodes < 1 {
		vnodes = 1
	}
	r := &Ring{
		vnodes: vnodes,
		shards: append([]int(nil), shards...),
		points: make([]point, 0, len(shards)*vnodes),
	}
	for _, s := range shards {
		for v := 0; v < vnodes; v++ {
			label := "shard-" + strconv.Itoa(s) + "-vnode-" + strconv.Itoa(v)
			r.points = append(r.points, point{hash: Hash(label), shard: s})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		// Break (astronomically unlikely) ties the same way everywhere.
		return r.points[i].shard < r.points[j].shard
	})
	return r
}

// NewN builds a ring for shards 0..n-1.
func NewN(n, vnodes int) *Ring {
	shards := make([]int, n)
	for i := range shards {
		shards[i] = i
	}
	return New(shards, vnodes)
}

// Lookup returns the shard that owns key: the owner of the first point at
// or after the key's hash, wrapping around at the top of the ring. It
// returns -1 for an empty ring.
func (r *Ring) Lookup(key string) int {
	if len(r.points) == 0 {
		return -1
	}
	h := Hash(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0
	}
	return r.points[i].shard
}

// Shards returns the shards on the ring.
func (r *Ring) Shards() []int {
	return append([]int(nil), r.shards...)
}

// VNodes returns the number of points per shard.
func (r *Ring) VNodes() int {
	return r.vnodes
}

// Hash is 64-bit FNV-1a followed by the splitmix64 finalizer. FNV-1a on its
// own maps strings that differ only in their last bytes, such as
// consecutive virtual node labels, to values that share their high bits,
// which would bunch those points together on the ring. The finalizer
// spreads every input bit across the whole output.
func Hash(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	x := uint64(offset64)
	for i := 0; i < len(s); i++ {
		x ^= uint64(s[i])
		x *= prime64
	}
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
