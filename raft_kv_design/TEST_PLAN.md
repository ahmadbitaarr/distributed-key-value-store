# TEST_PLAN.md

## 1. Testing Philosophy

This project should be judged by failure behavior, not only by whether `PUT` and `GET` work when all three nodes are healthy.

Every implementation phase must add tests before the next subsystem is considered complete.

Important invariants to test repeatedly:

```text
appliedIndex <= commitIndex <= lastLogIndex
```

and:

- no mutation is applied before commit
- follower replication ACK means durable append completed
- successful client mutation means leader durable commit completed
- duplicate retries do not create duplicate committed effects
- followers never return successful stale client reads

## 2. Test Layers

### Layer A — Unit tests

Fast tests for individual Java components with network/storage dependencies replaced by test doubles where appropriate.

### Layer B — Component integration tests

Real WAL + real protobuf messages + real service/component interaction in a temporary local environment.

### Layer C — Multi-node integration tests

Three node servers communicating with real gRPC on local ports.

### Layer D — Docker Compose failure tests

Three containers, independent persistent volumes, real process termination/restart, and network interruption where practical.

## 3. Unit Tests

## `KvStateMachineTest`

Test:

- PUT new key
- PUT overwrite existing key
- GET present key
- GET missing key
- DELETE present key
- DELETE missing key
- deterministic replay of the same command sequence

## `WalStoreTest`

Test:

- append ENTRY
- append COMMIT
- reopen and recover
- multiple entries and commit records
- checksum validation
- incomplete final record
- corrupt uncommitted tail
- committed-region corruption causes recovery failure
- truncate to a known safe record boundary

Use temporary directories so tests do not depend on developer machine state.

## `PersistentLogTest`

Test:

- contiguous index allocation
- entry lookup by index
- ordered range retrieval
- reject accidental index gaps
- uncommitted suffix truncation
- never truncate committed entries

## `CommitManagerTest`

For a 3-node cluster:

- leader only = no commit
- leader + follower-2 = commit
- leader + follower-3 = commit
- all three = commit
- duplicate ACK does not count twice
- late ACK after timeout does not violate state

## `StateMachineApplierTest`

Test:

- applies only through commit index
- applies in order
- does not apply same index twice
- catches up when commit index advances multiple entries
- maintains `appliedIndex <= commitIndex`

## `RequestDeduplicatorTest`

Test:

- first request admitted
- exact committed retry returns original log index
- same request ID + different command -> conflict
- concurrent exact duplicate joins in-flight operation
- uncommitted request is not reconstructed as committed after restart

## `LeaderCoordinatorTest`

With fake replication/storage collaborators:

- local durable append happens before local ACK counts
- client success waits for majority
- no state-machine apply without commit
- client success waits for durable COMMIT record
- failure to reach quorum returns no-quorum result

## `FollowerReplicationTest`

Test:

- accepts configured leader
- rejects incorrect sender
- ACK occurs only after durable append
- duplicate identical entry is idempotently acknowledged
- unexpected index is rejected
- commit cannot move beyond durable last log index

## 4. Protobuf / API Contract Tests

Test:

- required semantic validation for request ID/key
- PUT value round-trip, including arbitrary bytes
- result-code handling
- leader hint population on follower responses
- request ID preserved in mutation response
- unknown/default enum values handled defensively

## 5. Local gRPC Integration Tests

Start multiple node servers on ephemeral ports.

Test:

### Healthy cluster mutation

1. PUT to leader
2. verify response `OK`
3. verify leader commit index advanced
4. verify at least one follower durably stored entry
5. eventually verify both followers apply after commit propagation
6. GET from leader returns value

### Follower routing

1. send PUT to follower
2. receive `NOT_LEADER` with hint
3. router retries leader with same request ID
4. operation commits once

### GET from follower

1. write value
2. send GET directly to follower
3. verify `NOT_LEADER`
4. router retries leader
5. verify correct value

## 6. Docker Compose Cluster Tests

Compose should start:

```text
node-1  configured LEADER
node-2  FOLLOWER
node-3  FOLLOWER
```

Each node receives its own persistent volume.

Minimum Docker smoke test:

1. `docker compose up`
2. wait until all nodes report ready
3. PUT several keys
4. GET keys
5. DELETE one key
6. inspect cluster status
7. restart containers
8. verify committed state recovered

Docker service-to-service communication should use Compose service names rather than fixed container IPs.

## 7. Leader Crash Test

This test demonstrates **safe unavailability and recovery**, not automatic failover.

### Setup

- healthy 3-node cluster
- PUT several committed keys
- record final successful commit index

### Failure

- hard-stop leader container/process

### Assertions while leader is down

- follower does not promote itself
- direct follower PUT returns `NOT_LEADER`
- router cannot complete mutation because configured leader is unavailable
- follower GET does not return a successful possibly stale value

### Recovery

- restart leader using same persistent volume
- wait for recovery/reconciliation

### Assertions

- committed keys still exist
- leader recovers expected commit index
- no uncommitted entry was applied
- followers eventually converge to same committed prefix
- new writes can commit again

## 8. Leader Crash During Mutation Test

Inject crash points around:

1. before local WAL append
2. after local ENTRY fsync, before follower ACK
3. after one follower durable ACK, before leader COMMIT record
4. after leader COMMIT fsync, before state-machine apply
5. after apply, before client response

Expected outcomes:

- stages 1-3: operation may be absent after recovery because it was not durably committed by leader
- stages 4-5: operation must recover as committed
- if client did not receive response, retry with same request ID must not double-apply it

This is one of the strongest portfolio tests in the project.

## 9. Follower Lag Test

### Setup

- healthy cluster
- stop node-3

### During lag

- issue multiple writes
- leader + node-2 form quorum
- verify writes succeed
- node-3 remains behind

### Recovery

- restart node-3

### Assertions

- leader detects lag
- missing entries are sent in order
- node-3 WAL catches up
- commit index advances to leader's current committed point
- state-machine contents eventually match leader

## 10. Restart Recovery Test

### Single node recovery unit/integration

1. write ENTRY records
2. write commit records through index N
3. leave entries greater than N uncommitted
4. close/crash process
5. reopen

Assertions:

- commit index == N
- only entries 1..N applied
- uncommitted tail not visible in GET state
- dedup table contains only committed request IDs

### Full cluster restart

1. commit dataset
2. stop all three nodes
3. restart all with same volumes
4. verify same leader state
5. verify followers converge

## 11. Duplicate Retry Test

### Exact duplicate after success

1. PUT with request ID R
2. receive success at commit index N
3. resend exact PUT with R

Assertions:

- success returned
- commit index is still N for that logical request
- no extra log entry is committed for R

### Retry after unknown outcome

1. send PUT with R
2. crash/interrupt client connection after server commit but before client observes response
3. retry with R

Assertions:

- committed effect appears once
- server recognizes committed R
- response maps to original commit index

### Conflicting reuse

1. PUT key=A, value=1 with R
2. PUT key=A, value=2 with same R

Assertion:

- second operation receives `REQUEST_ID_CONFLICT`

## 12. Stale Read Prevention Test

### Setup

1. stop or delay node-3
2. commit several newer writes using leader + node-2
3. node-3 is stale

### Assertion

Direct GET against node-3 must return `NOT_LEADER`, not its stale local value.

Router then contacts leader and returns current committed value.

This test directly supports the read-consistency claim.

## 13. Quorum-Loss Test

### Setup

- leader healthy
- make both followers unavailable

### Mutation

- send PUT

Assertions:

- local leader ENTRY may exist as uncommitted storage
- mutation does not receive `OK`
- commit index does not advance
- state machine does not apply command
- GET does not expose that uncommitted mutation

After recovery/reconciliation, the uncommitted suffix is safely discarded or reconciled according to storage rules.

## 14. One-Follower Failure Test

- stop node-3
- send writes
- verify leader + node-2 commit
- restart node-3
- verify catch-up

This demonstrates the intended 2-of-3 majority behavior.

## 15. WAL Corruption Tests

### Partial tail

Create/truncate final bytes to simulate torn append.

Expected:

- recover to last valid safe record

### Corruption after committed boundary

Expected:

- discard uncommitted corrupted suffix if safe

### Corruption inside committed prefix

Expected:

- node refuses to become ready
- exposes data-loss/startup failure

## 16. Timing / Deadline Tests

Use deterministic fake follower delays where possible.

Test:

- fast follower + slow follower => commit after majority, without waiting for all 3
- both followers slower than deadline => no-quorum/timeout
- retry preserves request ID
- late follower ACK cannot make a previously failed client request double-commit on retry

Avoid flaky wall-clock assertions. Give broad time budgets and test state transitions rather than microsecond timing.

## 17. Metrics and Benchmark Tests

These are engineering measurements, not production performance claims.

Record at least:

- successful mutation count
- failed/no-quorum mutation count
- replication attempts
- replication retry count
- follower lag in log entries
- WAL recovery duration
- request latency p50/p95 from a controlled benchmark run
- approximate writes/second for healthy 3-node cluster

Recommended benchmark scenarios:

1. single client, sequential PUTs
2. several concurrent clients with serialized leader mutation pipeline
3. one follower down
4. follower restart/catch-up
5. restart recovery with increasing WAL sizes

Do not fail normal CI based on strict throughput numbers. Performance runs should produce reports for comparison, not flaky correctness gates.

## 18. Phase-by-Phase Test Gates

### Phase 1 — Domain model + state machine

Must pass:

- command model tests
- state-machine tests
- API validation tests

### Phase 2 — WAL + recovery

Must pass:

- append/reopen
- commit recovery
- truncated-tail recovery
- committed corruption failure
- replay/dedup reconstruction

### Phase 3 — gRPC contracts

Must pass:

- client service contract tests
- follower replication service tests
- deadlines/error mapping tests

### Phase 4 — leader replication + commit

Must pass:

- 2-of-3 commit
- no-quorum rejection
- one follower lag
- duplicate replication delivery
- no apply before commit

### Phase 5 — full recovery/reconciliation

Must pass:

- leader restart
- follower restart/catch-up
- uncommitted suffix cleanup
- duplicate retry across restart

### Phase 6 — Docker Compose failure suite

Must pass:

- healthy cluster CRUD
- follower crash
- leader crash + safe unavailability
- leader restart recovery
- full cluster restart
- stale follower read prevention

Only after these gates pass should performance tuning or optional Raft election work begin.

## 19. Definition of Done

The baseline project is complete when:

- all five design documents match implementation behavior
- 3-node Docker Compose cluster is reproducible
- committed writes survive restarts
- one follower may fail without preventing writes
- loss of majority prevents new commits
- leader crash causes safe unavailability
- leader restart restores committed state
- follower lag/catch-up works
- duplicate retries do not double-apply
- followers do not serve successful stale reads
- automated tests demonstrate these claims
- README clearly states that the project is Raft-inspired rather than full Raft

## References

1. JUnit User Guide: https://docs.junit.org/5.13.1/user-guide/index.html
2. gRPC retry guidance: https://grpc.io/docs/guides/retry/
3. gRPC status codes: https://grpc.io/docs/guides/status-codes/
4. Docker Compose networking: https://docs.docker.com/compose/how-tos/networking/
5. Raft paper: https://raft.github.io/raft.pdf
