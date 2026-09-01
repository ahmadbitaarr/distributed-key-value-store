# ARCHITECTURE.md

## 1. Architectural Style

Each of the three Java processes runs the same node application but receives a different node configuration:

- `node-1`: configured leader
- `node-2`: follower
- `node-3`: follower

The application is divided into small components so that network transport, persistence, replication, commit decisions, and state-machine mutation are independently testable.

## 2. Main Components

### `NodeServer`

Top-level process lifecycle.

Responsibilities:

- load cluster/node configuration
- initialize storage
- run recovery before accepting traffic
- start gRPC services
- start leader/follower background coordination
- stop cleanly

### `ClientKvService`

External client-facing gRPC service.

Responsibilities:

- validate PUT/GET/DELETE requests
- reject or redirect requests received by followers
- pass mutations to the leader command pipeline
- return application-level result codes and leader hints

It does **not** implement persistence or replication itself.

### `ReplicationService`

Internal node-to-node gRPC service.

Responsibilities:

- accept durable log-entry replication from the configured leader
- accept commit-index advancement
- report follower replication status
- truncate only an uncommitted suffix when commanded during reconciliation

### `LeaderCoordinator`

Owns the leader-side command lifecycle.

Responsibilities:

- serialize mutation admission
- perform request-ID deduplication
- allocate log indexes
- append locally
- invoke replication
- wait for majority durability
- advance commit
- apply committed commands
- complete client requests

### `ReplicationManager`

Leader-side replication logic.

Responsibilities:

- send entries to followers in parallel
- track durable acknowledgments
- enforce replication deadlines
- detect follower lag
- catch followers up
- propagate commit advancement

It does **not** decide client semantics or mutate the key-value map.

### `CommitManager`

Responsibilities:

- track current `commitIndex`
- decide when an entry has enough durable replicas to commit
- durably record commit advancement
- expose committed prefix to the state-machine applier

For a fixed 3-node cluster, quorum size is 2.

### `PersistentLog`

Logical replicated-log abstraction.

Responsibilities:

- expose ordered `LogEntry` records by index
- append entries
- retrieve entries for follower catch-up
- expose `lastLogIndex`
- truncate an uncommitted suffix during recovery/reconciliation

### `WalStore`

Physical persistence implementation beneath `PersistentLog` and commit metadata.

Responsibilities:

- append framed WAL records
- force records to durable storage
- scan records during startup
- validate checksums
- identify record/file offsets
- safely truncate an invalid or uncommitted tail

### `KvStateMachine`

Deterministic in-memory state machine.

Responsibilities:

- apply committed PUT commands
- apply committed DELETE commands
- serve GET from the applied map

It never decides whether an entry is committed.

### `StateMachineApplier`

Responsibilities:

- apply committed log entries strictly in index order
- maintain `appliedIndex`
- ensure `appliedIndex <= commitIndex`
- prevent an entry from being applied twice

### `RequestDeduplicator`

Responsibilities:

- track committed request IDs reconstructed from the committed log
- track active in-flight request IDs in memory
- return the original committed log index for an exact retry
- reject request-ID reuse with different command contents

### `RecoveryManager`

Responsibilities:

- scan and validate WAL
- recover `commitIndex`
- discard incomplete/uncommitted tail where required
- rebuild request deduplication state
- replay committed commands into `KvStateMachine`
- set `appliedIndex`
- perform leader/follower reconciliation before marking node ready

### `ClusterConfig`

Contains:

- local node ID
- configured leader ID
- node addresses
- RPC port
- WAL path
- replication timeout settings

Membership is fixed for the project.

## 3. Component Boundaries

The important dependency direction is:

```text
ClientKvService
      |
      v
LeaderCoordinator
   |      |       |
   v      v       v
PersistentLog  ReplicationManager  CommitManager
   |                          |          |
   v                          |          v
WalStore <--------------------+    StateMachineApplier
                                      |
                                      v
                                KvStateMachine
```

Rules:

- RPC classes do not directly edit the map.
- The state machine does not perform network calls.
- The WAL does not make quorum decisions.
- The replication manager does not decide whether a client request is a duplicate.
- Commit advancement is centralized.

## 4. Node Roles

### Leader

The configured leader:

- accepts client PUT/DELETE
- serves client GET
- allocates log indexes
- replicates entries
- decides majority commit
- drives follower catch-up

### Follower

A follower:

- does not accept successful client mutations
- does not serve successful client GETs
- persists replicated entries
- advances its local commit index only when instructed by the configured leader
- applies committed entries in order
- reports replication status

There is no candidate state in the baseline because there is no automatic election.

## 5. Client / Router Behavior

A small client library or router knows all three configured node addresses.

### Normal request

1. Client sends a request to a known node.
2. If it reaches the leader, the leader handles it.
3. If it reaches a follower, the follower returns `NOT_LEADER` plus the configured leader address.
4. The router retries against the leader.

### Leader unavailable

The router may attempt the configured leader a bounded number of times with backoff.

If the leader remains unavailable:

- the request fails as unavailable
- the router does not choose a new leader

For PUT/DELETE retries, the router always reuses the same `request_id`.

## 6. Write Path

Example: `PUT("user:42", value)`.

### Step 1: Admission

The leader:

- validates key/value size and request ID
- computes a command fingerprint
- checks committed and in-flight request IDs

If the request is an exact duplicate of a committed mutation, return its existing commit index.

If another identical request is currently in flight, attach the caller to that operation.

### Step 2: Serialize and allocate index

The leader serializes mutation admission and allocates:

```text
index = lastLogIndex + 1
```

A `LogEntry` is created containing the command and request metadata.

### Step 3: Leader durable append

The leader appends the entry to its WAL and forces it to durable storage.

Only after durable completion does the leader count itself as one replication acknowledgment.

### Step 4: Replicate in parallel

`ReplicationManager` sends the entry to node-2 and node-3 concurrently.

Each follower:

- verifies sender is the configured leader
- validates the expected log position
- appends the record to its WAL
- forces the append to durable storage
- returns acknowledgment

### Step 5: Majority decision

For three nodes:

```text
quorum = 2 durable copies
```

The leader therefore needs its own durable append plus at least one follower durable acknowledgment.

If quorum is not achieved before the mutation deadline, the entry remains uncommitted and the client does not receive success.

### Step 6: Durable commit

After quorum:

- `CommitManager` advances `commitIndex` to the entry index
- the new commit index is durably recorded in the leader WAL as a commit record

The client is still not answered yet.

### Step 7: Apply

`StateMachineApplier` applies every committed but unapplied entry in index order until:

```text
appliedIndex == commitIndex
```

### Step 8: Complete client operation

The leader returns success with:

- request ID
- committed log index

### Step 9: Propagate commit

The leader sends the new commit index to followers.

This propagation may occur best-effort after the leader has made the commit durable; a lagging follower can learn the commit index later during status synchronization.

## 7. Read Path

### Request reaches follower

Follower returns:

```text
NOT_LEADER + leader hint
```

It does not return its local value.

### Request reaches leader

The leader:

1. verifies recovery is complete
2. ensures `appliedIndex == commitIndex`
3. reads the key from `KvStateMachine`
4. returns either `FOUND(value)` or `NOT_FOUND`

Because all successful reads come from the leader's applied committed state, a lagging follower cannot expose a stale successful read.

## 8. Failure and Recovery Paths

## Follower failure during write

```text
leader durable append
    |
    +---- node-2 durable ACK
    |
    +---- node-3 unavailable

=> 2/3 durable copies => commit allowed
```

The failed follower later catches up.

## Loss of quorum

```text
leader durable append
    |
    +---- node-2 unavailable
    +---- node-3 unavailable

=> 1/3 => no commit
```

The state machine is not mutated by that entry.

## Leader crash

No follower promotes itself.

```text
leader down
   |
   +--> client PUT/DELETE unavailable
   +--> client GET unavailable
```

After restart:

1. scan WAL
2. validate record framing/checksums
3. recover durable `commitIndex`
4. discard incomplete or uncommitted local suffix as required
5. rebuild deduplication table from committed entries
6. replay committed entries into the map
7. query follower status
8. truncate only follower uncommitted suffixes that conflict with the leader's recovered committed prefix
9. resend missing entries
10. send current commit index
11. mark leader ready

## Follower restart / lag recovery

1. follower recovers its WAL and applied state
2. follower reports `lastLogIndex`, `commitIndex`, and `appliedIndex`
3. leader compares follower status with leader log
4. if follower has an uncommitted suffix beyond the leader-authoritative point, leader instructs follower to truncate only that uncommitted suffix
5. leader sends missing entries in order
6. follower durably stores them
7. leader sends current commit index
8. follower applies through that commit index

## 9. Text Data-Flow Diagram

```text
                        CLIENT / ROUTER
                              |
                    PUT / GET / DELETE
                              |
                              v
                    +-------------------+
                    |      LEADER       |
                    |  ClientKvService  |
                    +---------+---------+
                              |
                     mutation | GET
                              |----------------------+
                              v                      |
                    +-------------------+            |
                    | LeaderCoordinator |            |
                    +----+---------+----+            |
                         |         |                 |
             append/fsync|         |replicate        |
                         v         v                 |
                 +-----------+  +----------------+   |
                 |Persistent |  | Replication    |   |
                 |Log / WAL  |  | Manager        |   |
                 +-----------+  +-------+--------+   |
                                        |            |
                              gRPC      | gRPC       |
                               +--------+--------+    |
                               |                 |    |
                               v                 v    |
                        +-------------+     +-------------+
                        | FOLLOWER 2  |     | FOLLOWER 3  |
                        | WAL + State |     | WAL + State |
                        +-------------+     +-------------+
                               |                 |
                               +------ACK--------+
                                      |
                                      v
                              +---------------+
                              | CommitManager |
                              +-------+-------+
                                      |
                             commit WAL record
                                      |
                                      v
                           +---------------------+
                           | StateMachineApplier |
                           +----------+----------+
                                      |
                                      v
                              +---------------+
                              | KvStateMachine|<----------+
                              +---------------+
                                      |
                                   response
                                      |
                                      v
                                    CLIENT
```

## 10. Concurrency Model

Keep the first version simple:

- one serialized leader mutation pipeline
- parallel follower RPCs for each entry
- multiple GETs may execute concurrently once they observe an applied state
- one state-machine apply order
- follower replication service serializes mutations to its WAL

This gives deterministic log ordering without introducing an unnecessary multi-writer concurrency problem.

A later performance phase can pipeline multiple log entries, but correctness comes first.

## References

1. Raft paper: https://raft.github.io/raft.pdf
2. Raft overview: https://raft.github.io/
3. gRPC core concepts and deadlines: https://grpc.io/docs/what-is-grpc/core-concepts/
4. Docker Compose networking: https://docs.docker.com/compose/how-tos/networking/
