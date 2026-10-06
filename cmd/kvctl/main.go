// Command kvctl talks to the store from the command line.
//
//	kvctl -addr node1:7000 put greeting hello
//	kvctl -addr node1:7000 get greeting
//	kvctl -addr node1:7000 del greeting
//	kvctl -addr node1:7000 status     # this node's view of every shard
//	kvctl -addr node1:7000 health     # exit 0 once every shard has a leader
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"

	"github.com/LindseyZ1205/distributed-kv-store/client"
	"github.com/LindseyZ1205/distributed-kv-store/gen/kvpb"
	"github.com/LindseyZ1205/distributed-kv-store/internal/transport"
)

func main() {
	addr := flag.String("addr", "localhost:7000", "address of any node")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl [-addr host:port] get KEY | put KEY VALUE | del KEY | status | health")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var err error
	switch args[0] {
	case "status":
		err = status(ctx, *addr)
	case "health":
		err = health(ctx, *addr)
	case "get", "put", "del":
		err = data(ctx, *addr, args)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvctl:", err)
		os.Exit(1)
	}
}

func data(ctx context.Context, addr string, args []string) error {
	c, err := client.Dial(ctx, []string{addr}, client.Options{})
	if err != nil {
		return err
	}
	defer c.Close()
	s := c.NewSession()
	switch {
	case args[0] == "get" && len(args) == 2:
		v, ok, err := s.Get(ctx, args[1])
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%q not found", args[1])
		}
		fmt.Println(string(v))
		return nil
	case args[0] == "put" && len(args) == 3:
		return s.Put(ctx, args[1], []byte(args[2]))
	case args[0] == "del" && len(args) == 2:
		return s.Delete(ctx, args[1])
	}
	return fmt.Errorf("bad arguments: %s", strings.Join(args, " "))
}

func nodeStatus(ctx context.Context, addr string) (*kvpb.StatusResponse, error) {
	conn, err := grpc.NewClient(addr, transport.DialOptions()...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return kvpb.NewKVClient(conn).Status(ctx, &kvpb.StatusRequest{})
}

func status(ctx context.Context, addr string) error {
	st, err := nodeStatus(ctx, addr)
	if err != nil {
		return err
	}
	fmt.Printf("node %d\n", st.NodeId)
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "SHARD\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAST\tSNAPSHOT\tKEYS\tDIGEST")
	for _, s := range st.Shards {
		fmt.Fprintf(w, "%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%016x\n", s.Shard, s.Role, s.Term, s.Leader,
			s.CommitIndex, s.AppliedIndex, s.LastIndex, s.SnapshotIndex, s.Keys, s.Digest)
	}
	return w.Flush()
}

func health(ctx context.Context, addr string) error {
	st, err := nodeStatus(ctx, addr)
	if err != nil {
		return err
	}
	for _, s := range st.Shards {
		if s.Leader < 0 {
			return fmt.Errorf("shard %d has no leader", s.Shard)
		}
	}
	return nil
}
