package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// testCluster runs n replicas over a memNet, each with its own WAL in a
// temporary directory. A collector goroutine per replica plays the state
// machine: it checks that entries arrive in order and that every replica
// applies the same command at the same index, and it takes a snapshot every
// snapEvery entries.
type testCluster struct {
	t         *testing.T
	n         int
	snapEvery int
	net       *memNet
	dirs      []string

	mu          sync.Mutex
	nodes       []*Node
	stops       []chan struct{} // closed to stop a replica's collector
	dones       []chan struct{} // closed when that collector has exited
	lastApplied []int
	snapshots   []int          // InstallSnapshot or startup snapshots delivered, per replica
	committed   map[int]string // index -> command, as first applied anywhere
}

func newTestCluster(t *testing.T, n, snapEvery int) *testCluster {
	c := &testCluster{
		t:           t,
		n:           n,
		snapEvery:   snapEvery,
		net:         newMemNet(n),
		dirs:        make([]string, n),
		nodes:       make([]*Node, n),
		stops:       make([]chan struct{}, n),
		dones:       make([]chan struct{}, n),
		lastApplied: make([]int, n),
		snapshots:   make([]int, n),
		committed:   make(map[int]string),
	}
	for i := 0; i < n; i++ {
		c.dirs[i] = t.TempDir()
	}
	// Registered after TempDir, so replicas stop before their directories go.
	t.Cleanup(c.shutdown)
	for i := 0; i < n; i++ {
		c.start(i)
	}
	return c
}

func (c *testCluster) start(i int) {
	ch := make(chan ApplyMsg)
	nd, err := New(Config{
		ID:                 i,
		Peers:              c.n,
		Transport:          &memTransport{net: c.net, from: i},
		DataDir:            c.dirs[i],
		ApplyCh:            ch,
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatalf("starting replica %d: %v", i, err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	c.mu.Lock()
	c.nodes[i] = nd
	c.stops[i], c.dones[i] = stop, done
	c.lastApplied[i] = 0
	c.mu.Unlock()
	c.net.setNode(i, nd)
	go c.collect(i, nd, ch, stop, done)
}

// crash stops replica i. Its WAL stays behind for start to recover from.
// It waits for the replica's collector to exit, so a restarted replica
// never shares the bookkeeping with messages from the old one.
func (c *testCluster) crash(i int) {
	c.mu.Lock()
	nd, stop, done := c.nodes[i], c.stops[i], c.dones[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if nd == nil {
		return
	}
	c.net.setNode(i, nil)
	nd.Stop()
	close(stop)
	<-done
}

func (c *testCluster) shutdown() {
	for i := 0; i < c.n; i++ {
		c.crash(i)
	}
}

func (c *testCluster) node(i int) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *testCluster) collect(i int, nd *Node, ch <-chan ApplyMsg, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		var msg ApplyMsg
		select {
		case msg = <-ch:
		case <-stop:
			return
		}
		c.mu.Lock()
		if msg.SnapshotValid {
			c.snapshots[i]++
			c.lastApplied[i] = msg.SnapshotIndex
			c.mu.Unlock()
			continue
		}
		if msg.CommandIndex != c.lastApplied[i]+1 {
			c.t.Errorf("replica %d applied index %d right after %d", i, msg.CommandIndex, c.lastApplied[i])
		}
		c.lastApplied[i] = msg.CommandIndex
		cmd := string(msg.Command)
		if prev, ok := c.committed[msg.CommandIndex]; ok && prev != cmd {
			c.t.Errorf("replica %d applied %q at index %d, another replica applied %q",
				i, cmd, msg.CommandIndex, prev)
		}
		c.committed[msg.CommandIndex] = cmd
		snap := c.snapEvery > 0 && msg.CommandIndex%c.snapEvery == 0
		c.mu.Unlock()
		if snap {
			nd.Snapshot(msg.CommandIndex, []byte(fmt.Sprintf("state@%d", msg.CommandIndex)))
		}
	}
}

// leader waits until the connected replicas agree on exactly one leader in
// the newest term, and returns it.
func (c *testCluster) leader() int {
	c.t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		time.Sleep(100 * time.Millisecond)
		leaders := make(map[int][]int)
		newest := -1
		for i := 0; i < c.n; i++ {
			nd := c.node(i)
			if nd == nil || !c.net.isUp(i) {
				continue
			}
			if term, ok := nd.GetState(); ok {
				leaders[term] = append(leaders[term], i)
				if term > newest {
					newest = term
				}
			}
		}
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
		}
		if newest >= 0 {
			return leaders[newest][0]
		}
	}
	c.t.Fatal("no leader was elected")
	return -1
}

// appliedCount returns how many live replicas have applied index with cmd.
func (c *testCluster) appliedCount(index int, cmd string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if got, ok := c.committed[index]; !ok || got != cmd {
		return 0
	}
	count := 0
	for i := 0; i < c.n; i++ {
		if c.nodes[i] != nil && c.lastApplied[i] >= index {
			count++
		}
	}
	return count
}

// one submits cmd to whichever connected replica accepts it, retrying until
// at least expected replicas have applied it, and returns its index.
func (c *testCluster) one(cmd string, expected int) int {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		index := -1
		for i := 0; i < c.n; i++ {
			nd := c.node(i)
			if nd == nil || !c.net.isUp(i) {
				continue
			}
			if idx, _, ok := nd.Start([]byte(cmd)); ok {
				index = idx
				break
			}
		}
		if index == -1 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for wait := time.Now().Add(2 * time.Second); time.Now().Before(wait); {
			if c.appliedCount(index, cmd) >= expected {
				return index
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	c.t.Fatalf("%q was never applied on %d replicas", cmd, expected)
	return -1
}

func (c *testCluster) snapshotsDelivered(i int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshots[i]
}

func TestElectsOneLeader(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	first := c.leader()
	time.Sleep(time.Second)
	// With a healthy network the leader keeps its job.
	if again := c.leader(); again != first {
		t.Fatalf("leadership moved from %d to %d without any failure", first, again)
	}
}

func TestReelectsAfterLeaderIsolated(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	old := c.leader()
	c.net.isolate(old)
	if next := c.leader(); next == old {
		t.Fatalf("isolated replica %d is still the only leader", old)
	}
	c.net.connect(old)
	c.leader()
	c.one("after-heal", 3)
}

func TestReplicatesToEveryReplica(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	for i := 0; i < 50; i++ {
		c.one(fmt.Sprintf("cmd-%d", i), 3)
	}
}

func TestMinorityCannotCommit(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	c.one("before", 3)
	leader := c.leader()
	c.net.isolate((leader + 1) % 3)
	c.net.isolate((leader + 2) % 3)

	index, _, ok := c.node(leader).Start([]byte("no-majority"))
	if !ok {
		t.Fatalf("leader %d rejected Start", leader)
	}
	time.Sleep(time.Second)
	if n := c.appliedCount(index, "no-majority"); n > 0 {
		t.Fatalf("an entry stored only on the leader was applied on %d replicas", n)
	}

	c.net.connect((leader + 1) % 3)
	c.net.connect((leader + 2) % 3)
	c.one("after", 3)
}

func TestFollowerCatchesUp(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	c.one("start", 3)
	lagging := (c.leader() + 1) % 3
	c.net.isolate(lagging)
	for i := 0; i < 30; i++ {
		c.one(fmt.Sprintf("while-isolated-%d", i), 2)
	}
	c.net.connect(lagging)
	c.one("end", 3)
}

func TestRecoversFromWALAfterFullRestart(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	for i := 0; i < 20; i++ {
		c.one(fmt.Sprintf("before-%d", i), 3)
	}
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}
	// Each replica re-applies its log on restart; the collector checks the
	// replayed entries match what was applied before.
	for i := 0; i < 10; i++ {
		c.one(fmt.Sprintf("after-%d", i), 3)
	}
}

func TestSnapshotInstallsOnLaggingFollower(t *testing.T) {
	c := newTestCluster(t, 3, 10)
	c.one("start", 3)
	lagging := (c.leader() + 1) % 3
	c.net.isolate(lagging)
	for i := 0; i < 50; i++ {
		c.one(fmt.Sprintf("while-isolated-%d", i), 2)
	}
	c.net.connect(lagging)
	c.one("end", 3)
	if c.snapshotsDelivered(lagging) == 0 {
		t.Fatalf("replica %d caught up without receiving a snapshot", lagging)
	}

	// It recovers from its own snapshot and WAL after a restart too.
	c.crash(lagging)
	c.start(lagging)
	c.one("after-restart", 3)
}

func TestReadIndexNeedsMajority(t *testing.T) {
	c := newTestCluster(t, 3, 0)
	index := c.one("x", 3)
	leader := c.leader()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	got, err := c.node(leader).ReadIndex(ctx)
	cancel()
	if err != nil {
		t.Fatalf("ReadIndex on a healthy leader: %v", err)
	}
	if got < index {
		t.Fatalf("ReadIndex = %d, want at least %d (the last committed write)", got, index)
	}

	c.net.isolate(leader)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	_, err = c.node(leader).ReadIndex(ctx)
	cancel()
	if err == nil {
		t.Fatal("a leader cut off from the majority confirmed a read")
	}
}

func TestUnreliableNetworkWithCrashes(t *testing.T) {
	c := newTestCluster(t, 3, 25)
	c.net.setUnreliable(0.1, 20*time.Millisecond)

	stop := time.Now().Add(6 * time.Second)
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; time.Now().Before(stop); i++ {
				for r := 0; r < c.n; r++ {
					if nd := c.node(r); nd != nil {
						nd.Start([]byte(fmt.Sprintf("w%d-%d", w, i)))
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	rng := rand.New(rand.NewSource(2))
	for round := 0; time.Now().Before(stop); round++ {
		victim := rng.Intn(c.n)
		if round%2 == 0 {
			c.net.isolate(victim)
			time.Sleep(700 * time.Millisecond)
			c.net.connect(victim)
		} else {
			c.crash(victim)
			time.Sleep(300 * time.Millisecond)
			c.start(victim)
		}
		time.Sleep(400 * time.Millisecond)
	}
	wg.Wait()

	c.net.setUnreliable(0, 0)
	c.one("final", 3)
}
