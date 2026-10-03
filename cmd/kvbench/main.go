// Command kvbench is a closed-loop load generator. Each of -clients
// goroutines has its own session and issues one operation at a time on
// keys chosen uniformly at random, reading with probability -read-ratio
// and writing otherwise. It reports throughput and latency percentiles for
// the measurement window, which starts after -warmup.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LindseyZ1205/distributed-kv-store/client"
)

type result struct {
	reads, writes []time.Duration
	errors        int
}

func main() {
	addrs := flag.String("addrs", "localhost:7000", "comma-separated node addresses")
	clients := flag.Int("clients", 500, "concurrent clients")
	duration := flag.Duration("duration", 30*time.Second, "measurement window")
	warmup := flag.Duration("warmup", 5*time.Second, "time to run before measuring")
	keys := flag.Int("keys", 10000, "number of distinct keys")
	valueSize := flag.Int("value-size", 100, "bytes per value")
	readRatio := flag.Float64("read-ratio", 0.9, "fraction of operations that are reads")
	opTimeout := flag.Duration("op-timeout", 5*time.Second, "per-operation timeout")
	flag.Parse()
	log.SetFlags(log.Ltime)

	ctx := context.Background()
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	cluster, err := client.Dial(dctx, strings.Split(*addrs, ","), client.Options{})
	cancel()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer cluster.Close()

	keyNames := make([]string, *keys)
	for i := range keyNames {
		keyNames[i] = fmt.Sprintf("key-%07d", i)
	}
	value := bytes.Repeat([]byte("v"), *valueSize)

	log.Printf("loading %d keys", *keys)
	if err := preload(ctx, cluster, keyNames, value, *opTimeout); err != nil {
		log.Fatalf("preload: %v", err)
	}

	log.Printf("running %d clients, %.0f%% reads, %s warmup + %s", *clients, *readRatio*100, *warmup, *duration)
	start := time.Now()
	measureFrom := start.Add(*warmup)
	end := measureFrom.Add(*duration)
	results := make([]result, *clients)
	var wg sync.WaitGroup
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			s := cluster.NewSession()
			rng := rand.New(rand.NewPCG(uint64(c), uint64(start.UnixNano())))
			res := &results[c]
			for {
				t0 := time.Now()
				if !t0.Before(end) {
					return
				}
				key := keyNames[rng.IntN(len(keyNames))]
				read := rng.Float64() < *readRatio
				octx, cancel := context.WithTimeout(ctx, *opTimeout)
				var err error
				if read {
					_, _, err = s.Get(octx, key)
				} else {
					err = s.Put(octx, key, value)
				}
				cancel()
				lat := time.Since(t0)
				if t0.Before(measureFrom) {
					continue
				}
				switch {
				case err != nil:
					res.errors++
				case read:
					res.reads = append(res.reads, lat)
				default:
					res.writes = append(res.writes, lat)
				}
			}
		}(c)
	}
	wg.Wait()

	var reads, writes []time.Duration
	errs := 0
	for _, r := range results {
		reads = append(reads, r.reads...)
		writes = append(writes, r.writes...)
		errs += r.errors
	}
	secs := duration.Seconds()
	total := float64(len(reads)+len(writes)) / secs
	log.Printf("throughput %.0f ops/s (%.0f reads/s, %.0f writes/s), %d errors",
		total, float64(len(reads))/secs, float64(len(writes))/secs, errs)
	log.Printf("read latency  %s", describe(reads))
	log.Printf("write latency %s", describe(writes))

	fmt.Println()
	fmt.Println("| workload | clients | throughput | read p50 | read p99 | write p50 | write p99 | errors |")
	fmt.Println("|---|---:|---:|---:|---:|---:|---:|---:|")
	fmt.Printf("| %.0f%% reads, %d keys, %d B values | %d | %.0f ops/s | %s | %s | %s | %s | %d |\n",
		*readRatio*100, *keys, *valueSize, *clients, total,
		ms(percentile(reads, 0.50)), ms(percentile(reads, 0.99)),
		ms(percentile(writes, 0.50)), ms(percentile(writes, 0.99)), errs)
}

// preload writes every key once, so reads find data.
func preload(ctx context.Context, c *client.Cluster, keys []string, value []byte, timeout time.Duration) error {
	const workers = 64
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s := c.NewSession()
			for i := w; i < len(keys); i += workers {
				octx, cancel := context.WithTimeout(ctx, timeout)
				err := s.Put(octx, keys[i], value)
				cancel()
				if err != nil {
					errs <- fmt.Errorf("put %s: %w", keys[i], err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func percentile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	if !sort.SliceIsSorted(d, func(i, j int) bool { return d[i] < d[j] }) {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	}
	i := int(math.Ceil(q*float64(len(d)))) - 1
	if i < 0 {
		i = 0
	}
	return d[i]
}

func describe(d []time.Duration) string {
	if len(d) == 0 {
		return "n/a"
	}
	return fmt.Sprintf("p50 %s  p90 %s  p99 %s  p99.9 %s  max %s (%d samples)",
		ms(percentile(d, 0.50)), ms(percentile(d, 0.90)), ms(percentile(d, 0.99)),
		ms(percentile(d, 0.999)), ms(percentile(d, 1)), len(d))
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.2f ms", float64(d)/float64(time.Millisecond))
}
