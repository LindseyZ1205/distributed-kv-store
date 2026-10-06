package node

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/LindseyZ1205/distributed-kv-store/client"
	"github.com/LindseyZ1205/distributed-kv-store/gen/kvpb"
	lin "github.com/LindseyZ1205/distributed-kv-store/internal/linearizability"
)

// testCluster runs real nodes in one process, talking gRPC over loopback.
type testCluster struct {
	t      *testing.T
	shards int
	addrs  []string
	dirs   []string
	nodes  []*Node
	client *client.Cluster
}

func newTestCluster(t *testing.T, n, shards int) *testCluster {
	c := &testCluster{t: t, shards: shards, nodes: make([]*Node, n)}
	listeners := make([]net.Listener, n)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		c.addrs = append(c.addrs, lis.Addr().String())
		c.dirs = append(c.dirs, t.TempDir())
	}
	// Registered after TempDir, so nodes stop before their data goes.
	t.Cleanup(c.stopAll)
	for i, lis := range listeners {
		c.startOn(i, lis)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cl, err := client.Dial(ctx, c.addrs, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	c.client = cl
	t.Cleanup(func() { cl.Close() })
	return c
}

func (c *testCluster) startOn(i int, lis net.Listener) {
	n, err := Start(Config{
		ID:            i,
		Peers:         c.addrs,
		DataDir:       c.dirs[i],
		Shards:        c.shards,
		VNodes:        32,
		SnapshotEvery: 200,
	}, lis)
	if err != nil {
		c.t.Fatalf("starting node %d: %v", i, err)
	}
	c.nodes[i] = n
}

// start restarts a stopped node on its old address and data directory.
func (c *testCluster) start(i int) {
	lis, err := net.Listen("tcp", c.addrs[i])
	if err != nil {
		c.t.Fatalf("relisten on %s: %v", c.addrs[i], err)
	}
	c.startOn(i, lis)
}

func (c *testCluster) stop(i int) {
	if c.nodes[i] != nil {
		c.nodes[i].Stop()
		c.nodes[i] = nil
	}
}

func (c *testCluster) stopAll() {
	for i := range c.nodes {
		c.stop(i)
	}
}

// isolate cuts node i off from the other nodes, or reconnects it. Clients
// can still reach it, so it can be asked to serve requests it must refuse.
func (c *testCluster) isolate(i int, cut bool) {
	for j := range c.nodes {
		if j == i {
			continue
		}
		if c.nodes[i] != nil {
			c.nodes[i].Peers().SetBlocked(j, cut)
		}
		if c.nodes[j] != nil {
			c.nodes[j].Peers().SetBlocked(i, cut)
		}
	}
}

// waitConverged waits until every node reports the same applied index and
// digest for every shard.
func (c *testCluster) waitConverged(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	var detail string
	for time.Now().Before(deadline) {
		detail = ""
		var first *kvpb.StatusResponse
		for i := range c.nodes {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			st, err := c.client.Node(i).Status(ctx, &kvpb.StatusRequest{})
			cancel()
			if err != nil {
				detail = fmt.Sprintf("node %d: %v", i, err)
				break
			}
			if first == nil {
				first = st
				continue
			}
			for s, a := range first.Shards {
				b := st.Shards[s]
				if a.AppliedIndex != b.AppliedIndex || a.Digest != b.Digest {
					detail = fmt.Sprintf("shard %d: node 0 at %d/%016x, node %d at %d/%016x",
						s, a.AppliedIndex, a.Digest, i, b.AppliedIndex, b.Digest)
				}
			}
		}
		if detail == "" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	c.t.Fatalf("replicas did not converge: %s", detail)
}

func TestPutGetDelete(t *testing.T) {
	c := newTestCluster(t, 3, 4)
	s := c.client.NewSession()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		if err := s.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put k%d: %v", i, err)
		}
	}
	for i := 0; i < 100; i += 2 {
		if err := s.Delete(ctx, fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("delete k%d: %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		v, found, err := s.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil {
			t.Fatalf("get k%d: %v", i, err)
		}
		want := i%2 == 1
		if found != want || (found && string(v) != fmt.Sprintf("v%d", i)) {
			t.Fatalf("get k%d = (%q, %v), want found=%v", i, v, found, want)
		}
	}

	// The keys are spread over every shard, and every replica holds them.
	c.waitConverged(10 * time.Second)
	st, err := c.client.Node(0).Status(ctx, &kvpb.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, sh := range st.Shards {
		if sh.Keys == 0 {
			t.Errorf("shard %d holds no keys", sh.Shard)
		}
	}
}

func TestDataSurvivesFullRestart(t *testing.T) {
	c := newTestCluster(t, 3, 2)
	s := c.client.NewSession()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Enough writes to cross the snapshot threshold, so recovery uses both
	// a snapshot and the log after it.
	for i := 0; i < 600; i++ {
		if err := s.Put(ctx, fmt.Sprintf("k%d", i%50), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	c.stopAll()
	for i := range c.nodes {
		c.start(i)
	}
	for k := 0; k < 50; k++ {
		v, found, err := s.Get(ctx, fmt.Sprintf("k%d", k))
		if err != nil {
			t.Fatalf("get k%d after restart: %v", k, err)
		}
		if want := fmt.Sprintf("v%d", 550+k); !found || string(v) != want {
			t.Fatalf("get k%d after restart = (%q, %v), want %q", k, v, found, want)
		}
	}
}

func TestLinearizableUnderPartitionsAndRestarts(t *testing.T) {
	const (
		clients = 6
		keys    = 4
	)
	c := newTestCluster(t, 3, 3)
	rec := lin.NewRecorder()
	stop := time.Now().Add(8 * time.Second)

	var wg sync.WaitGroup
	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			s := c.client.NewSession()
			rng := rand.New(rand.NewPCG(uint64(cl), 7))
			for n := 0; time.Now().Before(stop); n++ {
				in := lin.Input{Key: fmt.Sprintf("k%d", rng.IntN(keys))}
				switch r := rng.IntN(10); {
				case r < 5:
					in.Kind = lin.Get
				case r < 9:
					in.Kind, in.Value = lin.Put, fmt.Sprintf("c%d-%d", cl, n)
				default:
					in.Kind = lin.Delete
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				call := rec.Now()
				out, err := runOp(ctx, s, in)
				ret := rec.Now()
				cancel()
				switch {
				case err == nil:
					rec.Record(cl, in, out, call, ret)
				case in.Kind != lin.Get:
					rec.RecordUnknown(cl, in, call)
				}
			}
		}(cl)
	}

	rng := rand.New(rand.NewPCG(1, 2))
	for round := 0; time.Now().Before(stop); round++ {
		victim := rng.IntN(len(c.nodes))
		if round%2 == 0 {
			c.isolate(victim, true)
			time.Sleep(time.Second)
			c.isolate(victim, false)
		} else {
			c.stop(victim)
			time.Sleep(300 * time.Millisecond)
			c.start(victim)
		}
		time.Sleep(500 * time.Millisecond)
	}
	wg.Wait()

	final := c.client.NewSession()
	for k := 0; k < keys; k++ {
		in := lin.Input{Kind: lin.Get, Key: fmt.Sprintf("k%d", k)}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		call := rec.Now()
		out, err := runOp(ctx, final, in)
		ret := rec.Now()
		cancel()
		if err != nil {
			t.Fatalf("final read of %s: %v", in.Key, err)
		}
		rec.Record(clients, in, out, call, ret)
	}

	if res, _ := rec.Check(time.Minute); res != porcupine.Ok {
		t.Fatalf("history of %d operations is not linearizable (%s)", rec.Len(), res)
	}
	t.Logf("%d operations, linearizable", rec.Len())
	c.waitConverged(30 * time.Second)
}

func runOp(ctx context.Context, s *client.Session, in lin.Input) (lin.Output, error) {
	switch in.Kind {
	case lin.Get:
		v, found, err := s.Get(ctx, in.Key)
		return lin.Output{Found: found, Value: string(v)}, err
	case lin.Put:
		return lin.Output{}, s.Put(ctx, in.Key, []byte(in.Value))
	default:
		return lin.Output{}, s.Delete(ctx, in.Key)
	}
}
