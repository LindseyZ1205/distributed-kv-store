// Command kvnode runs one storage node.
//
//	kvnode -id 0 -peers node1:7000,node2:7000,node3:7000 -listen :7000 -data /data
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/LindseyZ1205/distributed-kv-store/internal/node"
	"github.com/LindseyZ1205/distributed-kv-store/internal/ring"
)

func main() {
	id := flag.Int("id", 0, "this node's index in -peers")
	peers := flag.String("peers", "localhost:7000", "comma-separated addresses of all nodes, in id order")
	listen := flag.String("listen", ":7000", "address to serve on")
	data := flag.String("data", "data", "directory for the write-ahead logs and snapshots")
	shards := flag.Int("shards", 6, "number of shards (Raft groups)")
	vnodes := flag.Int("vnodes", ring.DefaultVNodes, "hash ring points per shard")
	snapshotEvery := flag.Int("snapshot-every", 10000, "applied entries between snapshots")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix(fmt.Sprintf("node %d ", *id))

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	n, err := node.Start(node.Config{
		ID:            *id,
		Peers:         strings.Split(*peers, ","),
		DataDir:       *data,
		Shards:        *shards,
		VNodes:        *vnodes,
		SnapshotEvery: *snapshotEvery,
		Logf:          log.Printf,
	}, lis)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("serving on %s with %d shards; peers %s", lis.Addr(), *shards, *peers)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Print("shutting down")
	n.Stop()
}
