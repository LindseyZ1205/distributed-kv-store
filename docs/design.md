# Design notes

How the store works, why it is built that way, and what it does not do.

## 1. Shape of the system

```
                       client (Go)
              hash ring: key -> shard -> leader
                          │ gRPC  KV.Put / Get / Delete
          ┌───────────────┼──────────────────┐
          ▼               ▼                  ▼
     ┌─────────┐     ┌─────────┐        ┌─────────┐
     │ node 0  │     │ node 1  │        │ node 2  │
     │ shard 0 │◀───▶│ shard 0 │◀──────▶│ shard 0 │   one Raft group per shard,
     │ shard 1 │◀───▶│ shard 1 │◀──────▶│ shard 1 │   with a replica on every node
     │   ...   │     │   ...   │        │   ...   │
     │ WAL per │     │ WAL per │        │ WAL per │
     │ shard   │     │ shard   │        │ shard   │
     └─────────┘     └─────────┘        └─────────┘
            gRPC Raft.AppendEntries / RequestVote / InstallSnapshot
```

- **Shards.** The keyspace is split into a fixed number of shards (six by
  default). Each shard is an independent Raft group with one replica on
  every node, its own leader, its own log and its own write-ahead log.
- **Placement.** A consistent-hash ring with virtual nodes maps each key to
  a shard (section 6). Clients and servers build the same ring from the
  same parameters, so a client sends each request straight to the right
  shard.
- **Leaders are spread out.** Replica `s mod n` of shard `s` uses half the
  usual election timeout, so it normally wins the first election. With six
  shards on three nodes each node starts out leading two shards, which
  spreads the write load. After failures leadership can drift, and nothing
  moves it back (no leadership transfer).
- **One connection between two nodes** carries the Raft traffic for all
  shards. Each request names its group.

## 2. The request path

**Write** (`Put`, `Delete`):

1. The client hashes the key to a shard and sends the request to the node
   it last saw leading that shard (initially the preferred leader).
2. A node that is not the leader answers `CODE_WRONG_LEADER` with a hint;
   the client follows it or tries the next node.
3. The leader encodes the command, calls `Start`, and registers a waiter
   for the log index it got.
4. The entry is replicated (section 3). When it is applied, the waiter is
   released if the entry at that index still carries the term the leader
   proposed it in; otherwise a new leader replaced it and the client is
   told to retry.

**Read** (`Get`) does not touch the log. The leader runs ReadIndex
(section 3.4) to confirm it is still the leader, waits until its state
machine has applied the returned index, and reads its local map.

**Exactly once.** Every client session has a random 64-bit id and numbers
its writes. A session has one write outstanding at a time, and the state
machine applies a write only if its sequence number is higher than the last
one applied for that client. A write retried after a timeout or a leader
change therefore takes effect once, even if the first attempt did commit.
The session table is part of every snapshot; without it a replica restored
from a snapshot would apply a retry a second time.

## 3. Raft

The consensus core started as my solution to MIT 6.5840 Lab 3
([repo](https://github.com/LindseyZ1205/6.5840-distributed-systems)):
leader election, log replication with fast backup, persistence and
snapshots. Running it as a service required the following changes.

### 3.1 Transport

Raft talks to its peers through a `Transport` interface. Production uses
gRPC (`internal/transport`). Tests use an in-memory network that can
isolate replicas and drop or delay messages. Handlers never modify their
arguments, because the in-memory network passes the sender's structs
straight through.

### 3.2 Replication: one loop per follower

The lab version sent a burst of AppendEntries on every heartbeat and every
`Start`. Under load that puts many RPCs in flight per follower, each
carrying overlapping entries. Here, the leader runs one goroutine per
follower with **at most one AppendEntries in flight**. When a reply comes
back and there is more to send, the next RPC goes out at once and carries
everything that arrived in the meantime, up to 1024 entries. With nothing
pending, the loop sends a heartbeat every 50 ms. Batching comes from this
structure rather than a timer, so a lone write is never delayed to wait
for company.

A follower also gets an RPC when the leader's commit index moves, so it
learns about commits promptly, and when a read is waiting for
confirmation.

### 3.3 Durability and group commit

All Raft state goes to a write-ahead log (section 4). The rules:

| Event | When the fsync happens |
|---|---|
| Term or vote changes | Immediately, before replying to anyone |
| Follower appends entries | Before acknowledging the AppendEntries |
| Leader appends (a client write) | In the background |

The leader writes the entry to the WAL buffer inside `Start` and wakes a
background loop that fsyncs. Every write that arrives before an fsync
starts is covered by it, so under load one fsync serves many writes. The
leader counts its own copy toward a majority (`matchIndex[me]`) only once
the fsync that covers it returns. Followers may receive and store the
entry before that; the leader not having fsynced yet only delays the
commit. This is the same arrangement etcd's Raft uses.

### 3.4 Linearizable reads: ReadIndex

A read must not return data older than any write that finished before the
read started. A leader that has been partitioned away may not know it was
replaced, so it cannot just read its local state. ReadIndex (section 6.4
of Ongaro's dissertation) makes it check first:

1. Wait until an entry from the current term has committed. Each new
   leader appends a no-op for this, so the wait only happens right after
   an election. Until then, the leader's commit index may lag behind
   entries its predecessor committed.
2. Take a read sequence number and send heartbeats.
3. Once a majority, counting the leader, has replied to an RPC sent
   **after** the read arrived, no other leader can have been elected in
   the meantime. The leader's commit index at that point covers every
   write that completed before the read started.
4. The service waits until its state machine has applied that index, then
   reads.

Confirmations are batched. Every RPC carries the read sequence number that
was current when it was sent, and one round of replies confirms all the
reads waiting on it.

### 3.5 CheckQuorum

A leader that has not heard from a majority within the maximum election
timeout (600 ms) steps down. A new leader may already exist on the other
side of a partition. Without this, clients would keep sending to a leader
that can neither commit writes nor confirm reads, and wait out their
timeouts.

### 3.6 Snapshots

The state machine snapshots every 10,000 applied entries (configurable).
Raft saves the snapshot, drops the log up to that index, and rewrites the
WAL to hold only the rest. A follower whose `nextIndex` falls inside the
compacted prefix is sent the snapshot with InstallSnapshot. Snapshots are
delivered to the state machine by the same goroutine that delivers
entries, so the two stay ordered.

## 4. Write-ahead log

Each shard replica has a directory holding `wal.log` and `snapshot.bin`.

```
wal.log record:  | length u32 | crc32c u32 | type u8 | body |
  hardState  term, vote                     the last one on disk wins
  entry      index, term, data              an entry at an existing index
                                            replaces it and everything after
  snapshot   index, term                    first record after compaction
```

- **Conflict truncation without a truncation record.** When a follower
  overwrites a conflicting suffix, it just appends the new entries with
  their indexes. Replay applies the same rule: an entry at an index that
  is already present cuts the log back to that point.
- **Torn writes.** A crash can leave a partial record at the tail. Replay
  stops at the first record that is short or fails its checksum, and the
  file is truncated there before new writes. Corruption in the middle of
  the file is treated the same way, which is the usual assumption that
  only the tail can be torn.
- **Compaction.** The new log (snapshot marker, hard state, remaining
  entries) is written to `wal.log.tmp`, fsynced, renamed over `wal.log`,
  and the directory is fsynced. A crash leaves either the old file or the
  new one.
- **Snapshot before compaction.** `snapshot.bin` is replaced the same way,
  and before the log is compacted. If a crash comes in between, startup
  finds a snapshot newer than the log's marker and reconciles the two.
- **Failure policy.** Any storage error other than "closed" stops the
  process. A replica that cannot persist must not keep acknowledging
  writes; Raft tolerates a crashed replica, but not one that forgets what
  it promised.

## 5. Failure handling, end to end

| Failure | What happens |
|---|---|
| Leader partitioned from the rest | It cannot commit or confirm reads; CheckQuorum makes it step down within 600 ms; the majority elects a new leader after an election timeout; clients follow `WRONG_LEADER` hints or move on after a timeout |
| Follower crashes | The leader keeps a majority with the other follower. On restart, the follower replays its WAL and catches up from the leader, by log or by snapshot |
| Whole cluster crashes | Every replica replays snapshot + WAL; acknowledged writes were fsynced on a majority, so they survive |
| Write times out at the client | The client retries with the same sequence number at whichever node now leads; the session table makes the retry a no-op if the first attempt committed |
| Slow or paused node | Treated like a partition while it is unresponsive |

## 6. Consistent hashing

Each shard places 128 points on a 64-bit ring at the hash of
`shard-<s>-vnode-<v>`, and a key belongs to the shard owning the first
point at or after the key's hash. The hash is FNV-1a followed by the
splitmix64 finalizer. FNV-1a alone maps strings that differ only in their
last characters, like consecutive virtual-node labels, to values that
share their high bits, which bunches points together on the ring.

The unit tests check the properties that matter, on 100,000 keys:

- With 6 shards and 128 virtual nodes each, the busiest shard is within
  20% of the mean. In this configuration it is 13.4% off. With one point
  per shard it is 92% off.
- Going from 6 to 7 shards moves 13.8% of the keys (1/7 is 14.3%), and
  only onto the new shard.

**Limitation:** the shard count is fixed when the cluster starts. The ring
would keep data movement small if a shard were added, but moving that data
between Raft groups (shard migration) is not implemented.

## 7. Testing

| Layer | What it checks | Where |
|---|---|---|
| Ring | Determinism, balance, minimal movement | `internal/ring` |
| WAL | Replay, suffix overwrite, torn tail, checksum, compaction, atomic snapshots | `internal/wal` |
| Raft | Election, re-election after isolation, replication, no commit without a majority, catch-up, full restart from the WAL, InstallSnapshot, ReadIndex refused without a majority, and agreement under 10% message loss with random isolation and crash/restart | `internal/raft` |
| Service | Put/Get/Delete across shards, full restart with snapshots, **linearizability with Porcupine** while nodes are partitioned and restarted, replica convergence | `internal/node` |
| Cluster | `kvcheck` with Porcupine on a Docker Compose cluster while a node is partitioned, one is killed, one is paused, and then the whole cluster is killed | `scripts/fault-test.sh` |
| Performance | 500-client closed-loop load test | `cmd/kvbench` |

All Go tests run with the race detector in CI.

**What the Docker test cannot show.** `docker kill` ends the process but
leaves the kernel's page cache in place, so data that was written but not
yet fsynced survives it. The fsync discipline in section 3.3 protects
against power loss and kernel crashes, which this setup does not simulate.

## 8. Not implemented

- **Membership changes.** The set of nodes is fixed at startup.
- **Shard migration or rebalancing** (see section 6).
- **PreVote.** A node that rejoins after a partition with a higher term
  forces an election in the groups it is part of. That is safe but
  causes a short unavailability.
- **Reads from followers.** Every read is served by the shard's leader.
- **Streaming snapshots.** A snapshot travels in one gRPC message
  (limit 64 MB).
- **TLS and authentication.**
