# PROJECT_SCOPE.md

## 1. Project Summary

Build a **Java, Raft-inspired distributed key-value store** that runs as a fixed **3-node cluster**.

The store supports:

- `PUT key value`
- `GET key`
- `DELETE key`

The system uses a **single configured leader** for all client-visible operations in the baseline version. Mutations are ordered by the leader, appended to a replicated log, durably written to a write-ahead log (WAL), replicated to followers over gRPC, committed after a majority of the three nodes durably acknowledge the entry, and then applied to an in-memory key-value state machine.

This project intentionally borrows the **leader + replicated-log + majority-commit + replicated-state-machine** structure associated with Raft, while not claiming full Raft consensus.

## 2. Primary Goal

The goal is to demonstrate strong distributed-systems engineering fundamentals:

- leader-based request ordering
- durable append-only logging
- majority-based replication
- explicit commit/apply boundaries
- deterministic state-machine execution
- retry-safe mutation handling
- node crash/restart recovery
- follower catch-up
- failure-oriented integration testing
- clear separation of RPC, replication, persistence, commit, and state-machine logic

The priority is **correctness, observability, and testability**, not maximum feature count.

## 3. Included in the Baseline

### Cluster

- Exactly 3 configured nodes.
- One node is configured as leader at startup.
- Two nodes are followers.
- Fixed cluster membership.
- Docker Compose starts all three nodes with persistent storage volumes.

### Client operations

- PUT
- GET
- DELETE

### Write path

- Writes are accepted only by the configured leader.
- Each mutation receives a monotonically increasing log index.
- The leader appends the command to its local WAL and forces it to durable storage before counting its own acknowledgment.
- The leader replicates the entry to both followers using gRPC.
- A follower acknowledges replication only after durably appending the entry to its own WAL.
- In a 3-node cluster, an entry reaches replication quorum when it is durable on any 2 nodes, including the leader.
- The leader then durably records the new commit index.
- The committed command is applied to the leader state machine.
- The leader sends commit advancement to followers.
- The client receives success only after the leader has durably recorded the commit and applied the command.

### Read path

- Successful GETs are served only by the configured leader.
- A follower receiving a client GET returns a `NOT_LEADER` result with a leader hint.
- Before answering, the leader ensures its state machine has applied through its current commit index.

### Persistence and recovery

- Append-only WAL per node.
- Persisted log entries.
- Persisted commit markers / commit index.
- In-memory key-value state rebuilt by replaying the committed WAL prefix.
- Detection of incomplete WAL tail records caused by crashes.
- Safe truncation of uncommitted suffixes during recovery/reconciliation.
- Follower catch-up after temporary downtime.

### Retry safety

- Each PUT and DELETE carries a client-generated `request_id`.
- Reusing the same request ID with the same mutation does not create a second committed mutation.
- Reusing the same request ID with different mutation contents is rejected.
- Concurrent duplicate requests join the same in-flight operation instead of creating duplicate log entries.

### Testing

- JUnit unit tests.
- In-process / local integration tests.
- Three-node Docker Compose tests.
- Crash, restart, lag, quorum-loss, duplicate retry, and stale-read tests.

## 4. Explicitly Excluded from the Baseline

The baseline is **not a full Raft implementation**.

The following are excluded unless added in a later, explicitly scoped extension:

- automatic leader election
- randomized election timeouts
- Raft terms
- `RequestVote`
- voting rules
- Raft log-matching safety rules based on `(term, index)`
- automatic leadership transfer
- dynamic cluster membership
- joint consensus
- snapshots
- log compaction
- multi-key transactions
- compare-and-swap
- secondary indexes
- follower-served linearizable reads
- leases
- authentication / authorization
- TLS configuration
- Byzantine fault tolerance
- cross-region replication
- arbitrary cluster sizes
- production-grade operational tooling

## 5. Supported Failure Cases

### One follower crashes

Expected behavior:

- The leader and remaining follower still form a 2-of-3 majority.
- Writes may continue.
- The failed follower falls behind.
- When it restarts, it catches up from the leader before being considered synchronized.

### Both followers are unreachable

Expected behavior:

- The leader cannot form a majority.
- New PUT/DELETE operations are not committed.
- The client receives a retryable no-quorum / unavailable result.
- Uncommitted commands are never applied to the state machine.

### Leader crashes

Expected baseline behavior:

- No automatic election occurs.
- Client-visible reads and writes become unavailable.
- Followers do not promote themselves.
- When the configured leader restarts, it recovers its committed state from disk, reconciles uncommitted suffixes, catches followers up as needed, and then resumes service.

This is intentionally **safe unavailability instead of unsafe fake failover**.

### Follower becomes slow or temporarily partitioned

Expected behavior:

- The leader does not wait for all three nodes when one follower plus the leader have formed a majority.
- The slow follower may lag.
- The lagging follower does not serve client reads.
- It later catches up using replication status and missing-log replay.

### Crash during an in-flight mutation

Possible cases:

1. The entry was not committed: the client may receive failure/timeout, and the uncommitted suffix is discarded during recovery.
2. The entry became committed but the client did not receive the response: after restart, retrying the same `request_id` returns the already-committed result without committing the mutation twice.

The client must treat timeout as an **unknown outcome** and retry with the same request ID.

### Partial WAL tail after process/container crash

Expected behavior:

- Recovery scans record boundaries and checksums.
- An incomplete uncommitted tail is discarded.
- Corruption inside the committed prefix is treated as a data-loss/startup failure rather than silently ignored.

## 6. Consistency Guarantees We Claim

Within the supported baseline assumptions:

### Single leader ordering

All successful client-visible operations are routed to one configured leader, giving the leader one serialization point for operations.

### Majority durability for acknowledged mutations

A successful PUT/DELETE means:

- its log entry was durably stored by at least 2 of the 3 nodes, and
- the leader durably recorded that the entry is committed before returning success.

### Committed-prefix application

Only committed log entries are applied to the key-value state machine.

### Read-after-write behavior

A GET served by the leader after a completed mutation observes a state machine that has applied through the leader's commit index.

### No successful stale follower reads

Followers do not return successful client GET results in the baseline, preventing an intentionally lagging follower from exposing stale state.

### Retry idempotency for mutations

Retries using the same request ID and same command do not create multiple committed effects.

### Intended linearizable single-key API under the static-leader model

Because operations are serialized by one leader, successful mutations are committed before acknowledgement, and reads come from the applied leader state, the API is designed to behave linearly for supported single-key operations while that single-leader assumption holds.

This is an engineering guarantee for the constrained project model, **not a formal proof of Raft linearizability under arbitrary leadership changes**.

## 7. Guarantees We Do Not Claim

We do not claim:

- full Raft safety
- full Raft liveness
- automatic failover
- availability when the configured leader is down
- safety if multiple nodes are incorrectly configured as leader
- linearizable reads from followers
- exactly-once execution across arbitrary external clients forever
- tolerance of two simultaneous node failures for writes
- tolerance of Byzantine or malicious nodes
- correctness under arbitrary membership changes
- zero data loss if storage acknowledged an `fsync` but the underlying platform later loses durable data
- production readiness

## 8. Availability Model

For mutations:

- Leader + at least one follower reachable: available.
- Leader only: unavailable for new commits.
- Two followers reachable but leader down: unavailable in baseline.

For reads:

- Configured leader available and recovered: available.
- Leader unavailable: unavailable in baseline.

This deliberately favors **clear safety semantics over maximum availability**.

## 9. Portfolio Positioning

Recommended description:

> A Java Raft-inspired distributed key-value store with leader-based log replication, majority commit, durable WAL recovery, request deduplication, follower catch-up, gRPC/Protocol Buffers, Docker Compose, and failure-injection tests.

Avoid descriptions such as:

- "implemented production Raft"
- "fully fault-tolerant consensus system"
- "zero-downtime leader failover"

unless those features are actually implemented and tested later.

## References

1. Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*: https://raft.github.io/raft.pdf
2. Raft project overview: https://raft.github.io/
3. gRPC core concepts: https://grpc.io/docs/what-is-grpc/core-concepts/
4. gRPC retry guidance: https://grpc.io/docs/guides/retry/
