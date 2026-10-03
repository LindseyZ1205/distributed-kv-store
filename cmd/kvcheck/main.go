// Command kvcheck checks a running cluster for linearizability and
// replica convergence. It is meant to run while faults are injected; see
// scripts/fault-test.sh.
//
// Concurrent clients issue random gets, puts and deletes on a small set of
// keys, so operations contend, and every operation is recorded with its
// call and return times. After the run, kvcheck reads every key once more,
// checks the whole history with Porcupine, and then waits until every
// replica of every shard has applied the same log index with the same data
// digest.
//
// Exit status: 0 if the history is linearizable and the replicas converge,
// 1 otherwise.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/LindseyZ1205/distributed-kv-store/client"
	"github.com/LindseyZ1205/distributed-kv-store/gen/kvpb"
	lin "github.com/LindseyZ1205/distributed-kv-store/internal/linearizability"
)

func main() {
	addrs := flag.String("addrs", "localhost:7000", "comma-separated node addresses")
	clients := flag.Int("clients", 16, "concurrent clients")
	keys := flag.Int("keys", 8, "number of keys the clients contend on")
	duration := flag.Duration("duration", 60*time.Second, "how long to run")
	opTimeout := flag.Duration("op-timeout", 5*time.Second, "give up on a single operation after this long")
	settle := flag.Duration("settle", 60*time.Second, "how long to wait for the final reads and for replicas to converge")
	checkTimeout := flag.Duration("check-timeout", 3*time.Minute, "time limit for the Porcupine check")
	viz := flag.String("viz", "", "write a Porcupine visualization of the history to this file")
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx := context.Background()
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	cluster, err := client.Dial(dctx, strings.Split(*addrs, ","), client.Options{})
	cancel()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer cluster.Close()

	rec := lin.NewRecorder()
	var acked, unknown, failedReads, slow atomic.Int64
	var slowest atomic.Int64 // nanoseconds
	stop := time.Now().Add(*duration)
	log.Printf("running %d clients on %d keys for %s", *clients, *keys, *duration)

	var wg sync.WaitGroup
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			s := cluster.NewSession()
			rng := rand.New(rand.NewPCG(uint64(c), uint64(time.Now().UnixNano())))
			for n := 0; time.Now().Before(stop); n++ {
				in := lin.Input{Key: fmt.Sprintf("key-%d", rng.IntN(*keys))}
				switch r := rng.IntN(10); {
				case r < 5:
					in.Kind = lin.Get
				case r < 9:
					in.Kind, in.Value = lin.Put, fmt.Sprintf("c%d-%d", c, n)
				default:
					in.Kind = lin.Delete
				}
				octx, cancel := context.WithTimeout(ctx, *opTimeout)
				call := rec.Now()
				out, err := run(octx, s, in)
				ret := rec.Now()
				cancel()
				// Operations caught by a fault stall until the cluster recovers.
				if took := ret - call; took > int64(time.Second) {
					slow.Add(1)
					for {
						cur := slowest.Load()
						if took <= cur || slowest.CompareAndSwap(cur, took) {
							break
						}
					}
				}
				switch {
				case err == nil:
					rec.Record(c, in, out, call, ret)
					acked.Add(1)
				case in.Kind == lin.Get:
					// A failed read changed nothing; leave it out.
					failedReads.Add(1)
				default:
					rec.RecordUnknown(c, in, call)
					unknown.Add(1)
				}
			}
		}(c)
	}
	wg.Wait()
	log.Printf("run finished: %d operations acknowledged, %d writes with unknown outcome, %d failed reads",
		acked.Load(), unknown.Load(), failedReads.Load())
	log.Printf("%d operations took longer than 1s; the slowest took %s",
		slow.Load(), time.Duration(slowest.Load()).Round(time.Millisecond))

	// Read every key once the faults are over, so the history ends with
	// the state the cluster settled on. Every acknowledged write must be
	// reflected in it.
	final := cluster.NewSession()
	deadline := time.Now().Add(*settle)
	for k := 0; k < *keys; k++ {
		in := lin.Input{Kind: lin.Get, Key: fmt.Sprintf("key-%d", k)}
		for {
			octx, cancel := context.WithTimeout(ctx, *opTimeout)
			call := rec.Now()
			out, err := run(octx, final, in)
			ret := rec.Now()
			cancel()
			if err == nil {
				rec.Record(*clients, in, out, call, ret)
				break
			}
			if time.Now().After(deadline) {
				log.Fatalf("FAIL: final read of %s never succeeded: %v", in.Key, err)
			}
		}
	}

	log.Printf("checking %d operations with Porcupine", rec.Len())
	res, info := rec.Check(*checkTimeout)
	if *viz != "" {
		if err := porcupine.VisualizePath(lin.Model, info, *viz); err != nil {
			log.Printf("visualization: %v", err)
		}
	}
	switch res {
	case porcupine.Ok:
		log.Print("history is linearizable")
	case porcupine.Illegal:
		log.Print("FAIL: history is not linearizable")
		os.Exit(1)
	default:
		log.Printf("FAIL: the check did not finish within %s", *checkTimeout)
		os.Exit(1)
	}

	if err := waitConverged(ctx, cluster, time.Until(deadline)+30*time.Second); err != nil {
		log.Printf("FAIL: %v", err)
		os.Exit(1)
	}
	log.Print("every replica of every shard converged to the same data")
}

func run(ctx context.Context, s *client.Session, in lin.Input) (lin.Output, error) {
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

// waitConverged polls every node until, for every shard, all replicas
// report the same applied index and the same data digest.
func waitConverged(ctx context.Context, c *client.Cluster, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		detail, ok := converged(ctx, c)
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("replicas did not converge: %s", detail)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func converged(ctx context.Context, c *client.Cluster) (string, bool) {
	var statuses []*kvpb.StatusResponse
	for i := 0; i < c.Nodes(); i++ {
		sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		st, err := c.Node(i).Status(sctx, &kvpb.StatusRequest{})
		cancel()
		if err != nil {
			return fmt.Sprintf("node %d: %v", i, err), false
		}
		statuses = append(statuses, st)
	}
	for s, a := range statuses[0].Shards {
		for i, st := range statuses[1:] {
			if s >= len(st.Shards) {
				return fmt.Sprintf("node %d reports %d shards", i+1, len(st.Shards)), false
			}
			b := st.Shards[s]
			if a.AppliedIndex != b.AppliedIndex || a.Digest != b.Digest {
				return fmt.Sprintf("shard %d: node 0 applied %d (digest %016x), node %d applied %d (digest %016x)",
					s, a.AppliedIndex, a.Digest, i+1, b.AppliedIndex, b.Digest), false
			}
		}
	}
	return "", true
}
