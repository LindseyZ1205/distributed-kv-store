# distributed-kv-store

[![ci](https://github.com/LindseyZ1205/distributed-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/LindseyZ1205/distributed-kv-store/actions/workflows/ci.yml)

A sharded, fault-tolerant key-value store in Go. Keys are partitioned with
consistent hashing, every shard is replicated across three nodes by its own
Raft group, nodes and clients talk gRPC, and every replica makes its state
durable in a write-ahead log before acknowledging it.

- **Raft** with leader election, log replication with fast backup,
  snapshots and InstallSnapshot. It started as my
  [MIT 6.5840 Lab 3 solution](https://github.com/LindseyZ1205/6.5840-distributed-systems)
  and was rebuilt to run as a service: a pluggable transport, a WAL
  instead of whole-state re-encoding, one pipelined replication loop per
  follower, and background fsync with group commit on the leader.
- **Linearizable reads and writes.** Writes go through the log, and reads
  use ReadIndex to confirm leadership with a majority instead of
  appending to the log. A leader that loses its majority steps down
  (CheckQuorum).
- **Exactly-once writes.** Client sessions with sequence numbers make
  retries after a timeout or a failover safe.
- **Consistent hashing** with 128 virtual nodes per shard. Each shard is
  its own Raft group, and their leaders are spread over the nodes.
- **gRPC + Protocol Buffers** for the client API and for Raft traffic
  between nodes.
- **Write-ahead log** with CRC32C-framed records, torn-tail recovery, and
  atomic compaction and snapshots (fsync, rename, fsync the directory).
- **Tested for linearizability under failures.** In-process tests and a
  Docker Compose cluster in CI record concurrent histories while nodes are
  partitioned, killed, paused and restarted, and check them with
  [Porcupine](https://github.com/anishathalye/porcupine).

## Results

Numbers from [CI run 37161416004](https://github.com/LindseyZ1205/distributed-kv-store/actions/runs/37161416004).
Everything ran on one GitHub-hosted runner (4 vCPUs, AMD EPYC 7763, 15 GB):
three nodes with six shards each, plus the load generator. Each load test
used 500 clients on 10,000 keys with 100-byte values, measured for 30 s
after a 5 s warmup. A write is acknowledged only after a majority has
fsynced it.

| Load | Reads / writes | Throughput | Read p50 | Read p99 | Write p50 | Write p99 | Errors |
|---|---|---:|---:|---:|---:|---:|---:|
| Closed loop, as fast as it goes | 90 / 10 | 24,586 ops/s | 18.2 ms | 55.9 ms | 19.0 ms | 57.5 ms | 0 |
| Closed loop, as fast as it goes | 50 / 50 | 21,978 ops/s | 20.1 ms | 59.6 ms | 21.1 ms | 60.6 ms | 0 |
| Paced to 12,000 ops/s | 90 / 10 | 12,000 ops/s | 4.6 ms | 23.7 ms | 5.3 ms | 24.6 ms | 0 |

- Closed-loop throughput depends a lot on the shared runner: an earlier run
  of the same benchmark measured 39,346 ops/s for the 90/10 mix.
  Closed-loop latency is mostly queueing, since 500 clients that each wait
  for their previous request see about 500 / throughput of delay on average.
- Paced latency is measured from when each request was scheduled to start,
  so time spent waiting behind a backlog counts.

**Fault injection.** In the same run, `kvcheck` recorded 438,211 operations
from 16 clients on 8 keys over 80 s. During that time node1 was cut off
from the network for 15 s, node2 was killed with SIGKILL and restarted,
node3 was frozen for 8 s, and then all three nodes were killed and
restarted. Porcupine found the history linearizable, and afterwards every
replica of every shard reported the same applied index and data digest.

**Tests.** Every Go test runs with the race detector on every push. The
whole suite also ran 12 times in a row (3 runs on each of 4 runners)
without a failure.

## Architecture

```
                       client (Go)
              hash ring: key -> shard -> leader
                          │ gRPC  KV.Put / Get / Delete
          ┌───────────────┼──────────────────┐
          ▼               ▼                  ▼
     ┌─────────┐     ┌─────────┐        ┌─────────┐
     │ node 0  │     │ node 1  │        │ node 2  │
     │ shard 0 │◀───▶│ shard 0 │◀──────▶│ shard 0 │   one Raft group per shard,
     │ shard 1 │◀───▶│ shard 1 │◀──────▶│ shard 1 │   a replica on every node
     │   ...   │     │   ...   │        │   ...   │
     └─────────┘     └─────────┘        └─────────┘
       WAL + snapshot per shard replica, fsynced before acknowledging
```

A **write** goes to the leader of the key's shard, is appended to its Raft
log, replicated to a majority, applied, and acknowledged. A **read** goes to
the same leader, which confirms with a round of heartbeats that it is still
the leader and serves the value once its state machine has caught up with
the commit index.

[`docs/design.md`](docs/design.md) covers the reasoning: the replication
pipeline, group commit, ReadIndex, the WAL format and recovery, failure
handling, and what is not implemented.

## Running it

Everything runs in Docker; no local Go toolchain is needed.

```bash
docker build -f deploy/Dockerfile -t distributed-kv-store:local .
docker compose -f deploy/docker-compose.yml up -d --wait

# command-line client inside the cluster network
alias kvctl='docker compose -f deploy/docker-compose.yml run --rm tools -addr node1:7000'
kvctl put greeting hello
kvctl get greeting
kvctl status            # every shard's role, term, leader, commit and applied index

make bench              # 500-client load test
make fault-test         # partitions, crashes and pauses under a linearizability check
make down               # stop the cluster and delete its volumes
```

With Go 1.25+ and protoc:

```bash
make generate           # protobuf code (also committed under gen/)
go test -race ./...
```

## Layout

```
cmd/
  kvnode/     storage node
  kvctl/      command-line client and status tool
  kvbench/    closed-loop load generator
  kvcheck/    linearizability and convergence checker for a live cluster
client/       Go client: topology discovery, routing, leader tracking, retries
internal/
  raft/       Raft: election, replication, ReadIndex, snapshots
  wal/        write-ahead log and snapshot files
  kv/         per-shard state machine, sessions, Raft group wrapper
  ring/       consistent hashing with virtual nodes
  transport/  gRPC transport for Raft
  node/       assembles shards, transport and gRPC services
  linearizability/  Porcupine model and history recorder
proto/        gRPC service definitions
deploy/       Dockerfile and three-node Docker Compose file
scripts/      fault-injection test
docs/         design notes
```

## Limitations

The number of nodes and shards is fixed at startup: there are no membership
changes and no shard migration. There is no PreVote, so a node rejoining
after a partition forces a brief re-election. Reads are always served by
leaders. There is no TLS or authentication. See
[`docs/design.md`](docs/design.md#8-not-implemented).

## License

MIT
