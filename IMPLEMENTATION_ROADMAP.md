# IMPLEMENTATION_ROADMAP.md

## 0. Purpose and Build Strategy

This roadmap turns the completed design into a staged implementation plan for a **Java, Raft-inspired distributed key-value store** running as a fixed 3-node cluster.

The baseline deliberately uses **one configured leader**. There is no automatic election in v1. If the configured leader is unavailable, the system becomes safely unavailable for client-visible reads and writes instead of promoting a follower with an incomplete consensus protocol.

The implementation order is intentionally bottom-up:

1. deterministic local behavior
2. logical command/log model
3. durable persistence
4. network contracts
5. role-aware servers
6. replication
7. majority commit
8. safe reads
9. catch-up
10. recovery
11. deduplication
12. containerized cluster
13. failure validation
14. observability/performance
15. documentation polish

Every phase must end with tests that prove a concrete property before the next distributed concern is added.

### Core client-success invariant

```text
ACK to client
    = majority durable replication
    + durable leader commit
    + leader application
```

A successful mutation response must never be produced earlier than this point.

### Baseline read rule

```text
Follower GET -> NOT_LEADER
Leader GET   -> read only from state applied through commitIndex
```

Followers may maintain committed state for recovery and convergence, but they do not successfully serve baseline client GET requests.

---

# 1. Repository Structure

## 1.1 Proposed top-level layout

```text
raft-kv-store/
├── README.md
├── pom.xml                         # created when implementation begins
├── compose.yaml                    # Phase 12
├── Dockerfile                      # Phase 12
├── .dockerignore
├── .gitignore
│
├── docs/
│   ├── PROJECT_SCOPE.md
│   ├── ARCHITECTURE.md
│   ├── API_DESIGN.md
│   ├── STORAGE_DESIGN.md
│   ├── TEST_PLAN.md
│   ├── IMPLEMENTATION_ROADMAP.md
│   ├── FAILURE_MODEL.md            # optional documentation-polish artifact
│   └── BENCHMARKS.md               # Phase 14/15 results
│
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/example/raftkv/
│   │   │       ├── client/
│   │   │       ├── server/
│   │   │       ├── rpc/
│   │   │       ├── replication/
│   │   │       ├── log/
│   │   │       ├── wal/
│   │   │       ├── commit/
│   │   │       ├── statemachine/
│   │   │       ├── deduplication/
│   │   │       ├── config/
│   │   │       ├── metrics/
│   │   │       └── errors/
│   │   │
│   │   ├── proto/
│   │   │   ├── client_api.proto
│   │   │   └── replication_api.proto
│   │   │
│   │   └── resources/
│   │       └── logback.xml         # only if structured logging configuration is used
│   │
│   └── test/
│       ├── java/
│       │   └── com/example/raftkv/
│       │       ├── client/
│       │       ├── server/
│       │       ├── rpc/
│       │       ├── replication/
│       │       ├── log/
│       │       ├── wal/
│       │       ├── commit/
│       │       ├── statemachine/
│       │       ├── deduplication/
│       │       ├── recovery/
│       │       ├── integration/
│       │       └── testsupport/
│       └── resources/
│
├── docker/
│   └── config/
│       ├── node-1.properties
│       ├── node-2.properties
│       └── node-3.properties
│
└── scripts/
    ├── cluster-up.sh
    ├── cluster-down.sh
    ├── smoke-test.sh
    ├── kill-leader.sh
    ├── restart-node.sh
    └── benchmark.sh
```

`com.example.raftkv` is a temporary package root for planning. Replace `com.example` with a stable reverse-domain/package namespace **before creating the first production Java classes**, so the repository does not require a package-wide rename later.

## 1.2 Why one Maven module in v1

Use one Maven module initially rather than splitting `client`, `server`, and `storage` into separate Maven modules.

Reasons:

- lower build complexity
- easier protobuf/gRPC code generation
- simpler JUnit integration testing
- fewer dependency-boundary problems while the architecture is still evolving
- package boundaries are enough to enforce responsibilities for a student-scale system

A multi-module Maven build can be a later cleanup only if the project becomes large enough to justify it.

## 1.3 Proto location

Keep hand-written protocol definitions in:

```text
src/main/proto/
```

Expected files:

```text
client_api.proto
replication_api.proto
```

Generated Java/protobuf/gRPC sources must go to the build-generated source directory, not into hand-written source control folders.

Do **not** manually edit generated classes.

## 1.4 Docker layout

Use:

- one `Dockerfile` to build/run the node application
- one `compose.yaml` defining the 3 node services
- separate persistent volume per node
- separate config per node
- Docker Compose service names for node-to-node addresses

Conceptually:

```text
node-1 -> configured LEADER
node-2 -> FOLLOWER
node-3 -> FOLLOWER
```

Do not hard-code container IP addresses.

## 1.5 Test folders

Tests should primarily **mirror the production package they exercise** instead of placing every test in one generic test package.

Examples:

```text
src/test/java/.../wal/WalStoreTest.java
src/test/java/.../commit/CommitManagerTest.java
src/test/java/.../statemachine/KvStateMachineTest.java
```

Cross-component tests belong in:

```text
integration/
```

Reusable test-only infrastructure belongs in:

```text
testsupport/
```

Examples of test-support responsibilities:

- temporary node configurations
- free-port allocation
- fake follower clients
- fault injection hooks
- WAL inspection helpers
- eventually/assertion helpers

Test support must not leak into production behavior.

## 1.6 Scripts folder

Scripts are optional until Phase 12, but useful once container failure testing begins.

Scripts should automate **repeatable developer actions**, not hide system logic.

Good uses:

- start/stop cluster
- hard-kill leader
- restart follower
- run smoke workload
- run benchmark workload

Bad use:

- repairing WAL state manually
- deciding cluster commit status outside the Java process
- simulating leader election

---

# 2. Build Tool Decision

## 2.1 Choice: Maven

Use **Maven** for v1.

### Why Maven fits this project

This project benefits more from predictable conventions than from a highly customized build language.

Maven gives us:

- conventional Java directory layout
- deterministic lifecycle stages such as compile/test/package
- straightforward JUnit integration
- mature protobuf/gRPC generation plugins
- dependency scopes that clearly separate production and test dependencies
- easy execution in CI and Docker builds
- a build file recruiters and reviewers can understand quickly

For a portfolio distributed-systems project, build clarity is more valuable than build-script cleverness.

## 2.2 Expected dependencies at a high level

Do not add all of these at once. Add them only in the phase that needs them.

### Runtime / implementation

- gRPC Java runtime
- Protocol Buffers Java runtime
- generated gRPC stub support
- SLF4J logging API
- one logging backend

### Testing

- JUnit Jupiter
- assertion library only if standard JUnit assertions become cumbersome
- gRPC in-process testing utilities where useful

### Optional later

- Micrometer core or a very small custom metrics abstraction

Do not add a database, Spring Boot, Netty configuration framework, dependency-injection framework, or serialization library unless the implemented design actually requires it.

## 2.3 Expected Maven plugins at a high level

- Java compiler plugin
- Surefire for unit tests
- Failsafe if integration tests are separated into Maven's integration-test/verify lifecycle
- protobuf code-generation plugin
- gRPC Java code-generation plugin
- JAR/application packaging plugin as needed

Optional later:

- JaCoCo for coverage visibility

Coverage should support finding missing tests, not become the project goal.

## 2.4 Build expectations

By the end of the project, these concepts should be straightforward:

```text
mvn test
mvn verify
mvn package
```

The exact POM is intentionally deferred until implementation begins.

---

# 3. Java Package Design

## 3.1 `client`

Purpose:

- client-facing Java wrapper/router
- request-ID creation for mutations
- leader-hint handling
- bounded retry/backoff
- preserve request ID across mutation retries

Must not:

- decide commit
- inspect WAL files
- pick a new leader when configured leader is down

## 3.2 `server`

Purpose:

- application lifecycle
- node readiness
- role-aware composition of services
- dependency wiring

Must not become a “god package” containing replication or storage algorithms.

## 3.3 `rpc`

Purpose:

- adapters between generated protobuf/gRPC types and internal domain objects
- gRPC service implementations
- gRPC peer clients
- transport error mapping

Must not:

- mutate the state machine directly
- own quorum logic
- implement WAL framing

## 3.4 `proto/generated` boundary

Generated protobuf/gRPC types are build output, not hand-written domain classes.

Recommended boundary:

```text
.proto schema
    -> generated message/service types
    -> rpc adapters/mappers
    -> internal domain model
```

Do not allow protobuf-generated objects to become the internal representation everywhere in the codebase. Keeping an internal command/log model reduces coupling between protocol schema and core logic.

## 3.5 `replication`

Purpose:

- leader-to-follower entry transmission
- follower replication progress
- deadlines/retries to peers
- catch-up orchestration
- commit propagation

Must not:

- decide request-ID semantics
- apply commands directly
- physically encode WAL records

## 3.6 `log`

Purpose:

- logical ordered replicated log
- log indexes
- `LogEntry` domain object
- append/query/range/truncate abstraction

Must not know about gRPC.

## 3.7 `wal`

Purpose:

- physical durable record storage
- ENTRY records
- COMMIT records
- framing
- checksums
- fsync/force
- scan/replay metadata
- safe tail truncation

Must not decide quorum.

## 3.8 `commit`

Purpose:

- durable-ack quorum tracking
- monotonic `commitIndex`
- durable commit recording
- expose newly committed range

Must never mutate key/value state directly.

## 3.9 `statemachine`

Purpose:

- deterministic PUT/DELETE application
- GET from applied state
- ordered application of committed entries
- `appliedIndex`

Must not:

- replicate
- perform fsync
- decide leader/follower role

## 3.10 `deduplication`

Purpose:

- request-ID conflict detection
- exact retry recognition
- committed duplicate reconstruction
- in-flight duplicate joining

Must not become a permanent unrelated client-session store.

## 3.11 `config`

Purpose:

- node ID
- configured leader ID
- role
- peer addresses
- ports
- WAL directory
- deadlines
- size limits

Configuration parsing and validation belong here.

## 3.12 `metrics`

Purpose:

- counters/gauges/timers exposed by components
- replication lag
- commit latency
- WAL latency
- request counts
- error counts

Metrics must observe system behavior, never determine correctness.

## 3.13 `errors`

Purpose:

- internal typed exceptions / error categories
- corruption/data-loss distinction
- invariant failures
- not-ready/precondition failures

Do not duplicate the protobuf `ResultCode` enum as a giant exception hierarchy.

## 3.14 Tests

Tests mirror production packages.

Add dedicated:

```text
integration/
testsupport/
```

Testing should be able to replace network peers, clocks/deadlines, and persistence boundaries where necessary without exposing production internals broadly.

---

# 4. Main Classes and Interfaces

Method names below are architectural contracts, not implementation code. Exact Java types may change when the corresponding phase is implemented.

## 4.1 Domain / log layer

### `Command`

**Package:** `log`

**Responsibility:** Immutable semantic mutation representation.

Represents:

- PUT
- DELETE
- key
- optional value

**Important methods:**

- `type()`
- `key()`
- `value()`
- deterministic equality/fingerprint input

**Must not:**

- know its log index
- perform network or disk I/O
- apply itself directly to storage

### `LogEntry`

**Package:** `log`

**Responsibility:** Immutable replicated-log entry.

Contains conceptually:

- index
- request ID
- command
- command fingerprint

**Important methods:**

- `index()`
- `requestId()`
- `command()`
- `fingerprint()`

**Must not:**

- contain `term` in v1
- decide whether it is committed

### `PersistentLog`

**Package:** `log`

**Responsibility:** Logical ordered log abstraction over durable WAL storage.

**Important methods:**

- `append(LogEntry entry)`
- `get(long index)`
- `entries(long fromInclusive, long toInclusive)`
- `lastLogIndex()`
- `truncateAfter(long index)`

**Must not:**

- perform quorum decisions
- contact followers
- apply commands

## 4.2 WAL layer

### `WalStore`

**Package:** `wal`

**Responsibility:** Durable append-only WAL operations and recovery scanning.

**Important methods:**

- `appendEntry(...)`
- `appendCommit(...)`
- `force()` or an append operation whose contract includes durable force
- `scan()`
- `truncateToOffset(...)`
- `close()`

The durability contract should be explicit: callers must know whether completion means bytes were merely written or forced to durable storage.

**Must not:**

- expose client semantics
- count acknowledgments
- advance state-machine indexes

### `WalRecordCodec`

**Package:** `wal`

**Responsibility:** Encode/decode framed ENTRY and COMMIT WAL records.

**Important methods:**

- `encodeEntry(...)`
- `encodeCommit(...)`
- `decode(...)`
- checksum verification

**Must not:**

- perform file I/O
- decide whether a record is safe to commit

### `WalRecoveryResult`

**Package:** `wal`

**Responsibility:** Immutable description of recovered durable WAL state.

Conceptually exposes:

- valid entries
- recovered commit index
- last safe byte offset
- corruption/tail status

**Must not:**

- mutate the state machine itself

## 4.3 State machine layer

### `KvStateMachine`

**Package:** `statemachine`

**Responsibility:** Deterministic in-memory key/value state.

**Important methods:**

- `applyPut(key, value)`
- `applyDelete(key)`
- `get(key)`
- `clear()` for controlled recovery/test use

**Must not:**

- decide commit
- read the WAL
- make RPCs

### `StateMachineApplier`

**Package:** `statemachine`

**Responsibility:** Apply committed entries in strict log-index order.

**Important methods:**

- `applyThrough(long commitIndex)`
- `appliedIndex()`

**Must enforce:**

```text
appliedIndex <= commitIndex
```

**Must not:**

- advance commitIndex
- apply an entry twice
- skip indexes

## 4.4 Commit layer

### `CommitManager`

**Package:** `commit`

**Responsibility:** Track durable replica acknowledgments and advance the leader's commit index.

**Important methods:**

- `recordDurableAck(index, nodeId)`
- `hasQuorum(index)`
- `commit(index)`
- `commitIndex()`

`commit(index)` must not publish an in-memory committed index until the corresponding commit metadata has been durably recorded according to the storage design.

**Must not:**

- apply the command
- call client response observers
- fabricate follower ACKs

### `AckTracker`

**Package:** `commit`

**Responsibility:** Per-index unique-node acknowledgment accounting.

**Important methods:**

- `acknowledge(nodeId)`
- `count()`
- `hasMajority()`

**Must not:**

- treat duplicate ACKs as additional replicas
- own WAL persistence

## 4.5 Deduplication layer

### `CommandFingerprint`

**Package:** `deduplication`

**Responsibility:** Produce deterministic fingerprint input/result for a semantic mutation.

**Important methods:**

- `of(Command command)`
- equality/comparison

**Must not:**

- include transport-only fields that would make the same logical retry look different

### `RequestDeduplicator`

**Package:** `deduplication`

**Responsibility:** Coordinate first-seen, in-flight, committed duplicate, and request-ID conflict behavior.

**Important methods:**

- `check(requestId, fingerprint)`
- `registerInFlight(...)`
- `markCommitted(...)`
- `completeFailed(...)`
- `rebuildFromCommittedLog(...)`

**Must not:**

- append log entries itself
- commit commands

### `DeduplicationDecision`

**Package:** `deduplication`

**Responsibility:** Explicit result of deduplication check.

Possible conceptual outcomes:

- NEW
- JOIN_IN_FLIGHT
- ALREADY_COMMITTED
- CONFLICT

This avoids boolean-heavy calling code.

## 4.6 Replication layer

### `FollowerClient`

**Package:** `replication`

**Responsibility:** Interface used by leader replication logic to communicate with one follower.

**Important methods:**

- `replicateEntry(...)`
- `advanceCommit(...)`
- `getStatus()`
- `truncateUncommittedSuffix(...)`

**Must not:**

- know client API semantics
- make commit decisions

A gRPC implementation belongs in `rpc`; unit tests can provide a fake implementation.

### `ReplicationManager`

**Package:** `replication`

**Responsibility:** Leader-side parallel replication and follower progress tracking.

**Important methods:**

- `replicate(LogEntry entry)`
- `propagateCommit(long commitIndex)`
- `catchUpFollower(nodeId)`
- `progress(nodeId)`

**Must not:**

- allocate log indexes
- apply the state machine
- decide whether request IDs conflict

### `FollowerProgress`

**Package:** `replication`

**Responsibility:** Track leader-side view of follower durable/log progress.

Conceptual fields:

- node ID
- last persisted index
- known commit index
- availability/last-contact state

**Must not:**

- be treated as authoritative persistent state

### `FollowerReplicationHandler`

**Package:** `replication`

**Responsibility:** Core follower-side semantics behind replication RPCs.

**Important methods:**

- `replicateEntry(...)`
- `advanceCommit(...)`
- `status()`
- `truncateUncommittedSuffix(...)`

**Must guarantee:**

- sender is configured leader
- durable append before positive replication ACK
- committed entries are never truncated
- commit does not exceed durable last log index

**Must not:**

- expose gRPC-specific response observers

## 4.7 Leader command orchestration

### `LeaderCoordinator`

**Package:** `server` or `replication`

**Recommended package:** `server`

Reason: it coordinates the end-to-end leader request lifecycle rather than being only a replication component.

**Responsibility:** Orchestrate one mutation from admission through client-visible completion.

**Important methods:**

- `executePut(...)`
- `executeDelete(...)`
- internal `executeMutation(...)`

Conceptual ordering:

```text
deduplicate
-> allocate entry
-> local durable append
-> replicate
-> majority durable ACK
-> durable commit
-> apply
-> mark dedup committed
-> return success
```

**Must not:**

- encode WAL bytes
- implement gRPC transport
- directly manipulate the backing map

### `MutationResult`

**Package:** `server`

**Responsibility:** Internal client-operation outcome independent of protobuf response classes.

Conceptual outcomes:

- committed success
- no quorum
- request-ID conflict
- not ready

**Must not:**

- contain gRPC transport objects

## 4.8 RPC layer

### `ClientKvGrpcService`

**Package:** `rpc`

**Responsibility:** Implement generated client-facing gRPC service base class.

**Important methods:**

- `put(...)`
- `get(...)`
- `delete(...)`

**Must:**

- validate wire request
- enforce role/readiness gating
- map internal results to protobuf responses/statuses

**Must not:**

- append WAL records directly
- count follower ACKs

### `ReplicationGrpcService`

**Package:** `rpc`

**Responsibility:** Implement generated internal replication service.

**Important methods:**

- `replicateEntry(...)`
- `advanceCommit(...)`
- `getReplicationStatus(...)`
- `truncateUncommittedSuffix(...)`

It should delegate semantic work to `FollowerReplicationHandler`.

**Must not:**

- duplicate follower persistence rules inside RPC glue

### `GrpcFollowerClient`

**Package:** `rpc`

**Responsibility:** gRPC transport implementation of `FollowerClient`.

**Important methods:**

- implementations of follower RPC calls
- deadline application
- protobuf/domain mapping

**Must not:**

- retry indefinitely
- decide quorum

### `ProtoMapper`

**Package:** `rpc`

**Responsibility:** Translate internal domain objects to/from generated protobuf messages.

**Important methods:**

- command/log-entry mappings
- result mappings

**Must not:**

- contain business rules

## 4.9 Server lifecycle / recovery

### `NodeServer`

**Package:** `server`

**Responsibility:** Top-level process lifecycle and dependency composition.

**Important methods:**

- `start()`
- `stop()`
- `awaitTermination()`

Startup conceptually:

```text
load config
-> open WAL
-> recover local state
-> role-specific reconciliation
-> mark ready
-> start/accept normal traffic
```

The exact point at which gRPC binds versus readiness becomes true may differ, but client operations must return `NOT_READY` until recovery/reconciliation is complete.

**Must not:**

- contain the implementation of WAL parsing or replication algorithms

### `NodeReadiness`

**Package:** `server`

**Responsibility:** Explicit readiness state used by RPC gates and tests.

Conceptual states may include:

- STARTING
- RECOVERING
- RECONCILING
- READY
- FAILED

**Must not:**

- be confused with leader/follower role

### `RecoveryManager`

**Package:** `server`

**Responsibility:** Restore local state from WAL before request service.

**Important methods:**

- `recoverLocalState()`
- `reconcileAsLeader()`
- `reconcileAsFollower()` if needed

Recovery responsibilities:

- validate WAL
- recover committed prefix
- remove invalid/uncommitted local suffix where allowed
- replay committed commands
- rebuild dedup state
- restore indexes

**Must not:**

- claim full Raft log reconciliation

## 4.10 Configuration

### `ClusterConfig`

**Package:** `config`

**Responsibility:** Immutable validated cluster configuration.

Conceptual fields:

- node ID
- configured leader ID
- node role
- all peer addresses
- gRPC port
- WAL path
- operation deadlines
- request size limits

**Important methods:**

- accessors
- `leader()`
- `peers()`

**Must not:**

- dynamically change membership in v1

### `ConfigLoader`

**Package:** `config`

**Responsibility:** Load environment/properties into `ClusterConfig` and reject inconsistent configurations.

**Important methods:**

- `load()`
- `validate(...)`

Important validation:

- exactly 3 unique node IDs
- exactly one configured leader in deployment configuration
- local node exists in membership
- WAL path exists/is creatable
- peer addresses are present

## 4.11 Client

### `KvClient`

**Package:** `client`

**Responsibility:** Simple Java client API.

**Important methods:**

- `put(key, value)`
- `get(key)`
- `delete(key)`

Mutation methods generate one request ID per logical operation and pass it to the router.

**Must not:**

- generate a new request ID on transport retry

### `ClusterRouter`

**Package:** `client`

**Responsibility:** Route/retry requests to configured nodes and follow leader hints.

**Important methods:**

- `put(request)`
- `get(request)`
- `delete(request)`

**Must not:**

- elect a leader
- promote a follower
- retry forever

## 4.12 Metrics

### `MetricsRegistry`

**Package:** `metrics`

**Responsibility:** Small abstraction for counters/gauges/timers.

**Important metrics:**

- client PUT/GET/DELETE totals
- successful/failed mutations
- quorum failures
- WAL append latency
- mutation commit latency
- replication RPC latency
- follower last-persisted index
- follower lag = leader lastLogIndex - follower lastPersistedIndex
- commitIndex
- appliedIndex
- duplicate retry count

**Must not:**

- alter control flow based on metric backend availability

---

# 5. Implementation Phases

# Phase 1 — Local Single-Node KV State Machine

## Goal

Prove deterministic PUT/GET/DELETE behavior without network, WAL, replication, or commit logic.

## Build

- `Command`
- operation type domain enum
- `KvStateMachine`
- simple value-copy/immutability policy for `byte[]`

## Deliberately absent

- gRPC
- WAL
- commitIndex
- appliedIndex orchestration
- request IDs

## Exit condition

A deterministic sequence of commands produces exactly the expected map state every time.

---

# Phase 2 — Command Model and Replicated Log Abstraction

## Goal

Introduce ordered immutable log entries and a testable logical log API before persistence.

## Build

- `LogEntry`
- log index rules
- in-memory implementation/test double of log behavior
- `PersistentLog` interface/contract
- range retrieval and suffix truncation rules

## Key rule

Indexes are contiguous and monotonic within the configured leader's active history.

## Exit condition

Tests prove append ordering, lookup, ranges, and safe uncommitted suffix behavior without involving files.

---

# Phase 3 — WAL Append and Replay

## Goal

Make log entries and commit metadata durable independently of distributed replication.

## Build

- WAL record framing
- ENTRY record encoding
- COMMIT record encoding
- checksum
- append + durable force contract
- scan/replay
- tail detection
- safe physical truncation
- WAL-backed `PersistentLog`

## Important restraint

At this phase, a COMMIT record can be exercised by tests/recovery utilities, but distributed quorum logic does not exist yet.

## Exit condition

A process can append entries/commit markers, close, reopen, and reconstruct exactly the expected durable log and committed prefix.

---

# Phase 4 — Protobuf and gRPC Service Definitions

## Goal

Freeze the network contracts before implementing distributed behavior.

## Build

- `client_api.proto`
- `replication_api.proto`
- Maven protobuf/gRPC code generation
- `ProtoMapper`
- API validation tests

Client RPCs:

- Put
- Get
- Delete

Internal RPCs:

- ReplicateEntry
- AdvanceCommit
- GetReplicationStatus
- TruncateUncommittedSuffix

## Important restraint

Generated service methods may initially be unimplemented/minimal adapters. Do not put replication behavior into this phase merely because RPC stubs exist.

## Exit condition

Schemas compile, generated Java sources build, and message round-trip/contract tests pass.

---

# Phase 5 — Leader/Follower Server Roles

## Goal

Run a node process with explicit configured role and readiness behavior before replication is enabled.

## Build

- `ClusterConfig`
- `ConfigLoader`
- `NodeReadiness`
- `NodeServer`
- `ClientKvGrpcService`
- role gating

Baseline behavior:

- follower PUT -> `NOT_LEADER`
- follower DELETE -> `NOT_LEADER`
- follower GET -> `NOT_LEADER`
- leader can serve GET from its current applied state when ready

Writes may still be disabled/not-ready until distributed mutation pipeline exists.

## Exit condition

Two differently configured node processes expose correct role behavior without pretending followers can become leaders.

---

# Phase 6 — Replication RPCs

## Goal

Durably copy one leader log entry to followers.

## Build

- `FollowerClient`
- `GrpcFollowerClient`
- `ReplicationGrpcService`
- `FollowerReplicationHandler`
- `ReplicationManager`
- follower progress tracking

Follower positive ACK rule:

```text
validate entry
-> append to WAL
-> force durable
-> reply accepted
```

## Not yet included

- client mutation success
- majority commit advancement

## Exit condition

Leader-side tests can replicate an entry to one or two real/fake followers and prove follower ACK occurs only after durable append.

---

# Phase 7 — Majority ACK Tracking and Commit Index

## Goal

Implement the complete safe mutation commit pipeline.

## Build

- `AckTracker`
- `CommitManager`
- `LeaderCoordinator`
- durable leader COMMIT record
- `StateMachineApplier`
- client mutation result mapping

Required ordering:

```text
1. admit command
2. allocate log index
3. append leader ENTRY durably
4. replicate to followers
5. obtain 2-of-3 durable copies
6. append leader COMMIT durably
7. advance in-memory commitIndex
8. apply through commitIndex
9. return client success
```

If step 5 fails:

- do not commit
- do not apply
- do not return success

## Exit condition

A healthy 3-node local test can PUT/DELETE successfully with only two durable replicas, while loss of quorum prevents client success and state-machine application.

---

# Phase 8 — Committed-State Read Path

## Goal

Make GET semantics explicitly tied to committed/applied leader state.

## Build

Leader GET gate:

- node must be configured leader
- node must be READY
- ensure state machine is applied through current commitIndex
- read from `KvStateMachine`

Follower behavior remains:

```text
GET -> NOT_LEADER + leader hint
```

## Exit condition

Tests prove a follower never successfully returns stale application data and a leader GET after a successful mutation sees that mutation.

---

# Phase 9 — Follower Lag and Catch-Up

## Goal

Allow a follower that missed entries to converge without blocking healthy writes that already have quorum.

## Build

- `GetReplicationStatus`
- follower progress comparison
- missing-range retrieval from `PersistentLog`
- sequential/bounded catch-up replication
- commit propagation after missing entries exist durably
- safe uncommitted suffix truncation for reconciliation cases allowed by the design

Simplify v1:

- no streaming replication required
- no batching required initially
- prioritize correctness and observable progress

## Exit condition

Stop one follower, commit multiple entries with leader + other follower, restart the lagging follower, and prove it converges to the same committed prefix/state.

---

# Phase 10 — Restart Recovery

## Goal

Make process/container restart a normal tested lifecycle rather than a special case.

## Build

- `RecoveryManager`
- startup WAL scan
- committed-prefix replay
- `commitIndex` restoration
- `appliedIndex` reconstruction
- local uncommitted tail handling
- leader/follower reconciliation before readiness

Startup safety rule:

```text
No normal request service before WAL replay and required reconciliation complete.
```

## Exit condition

Hard-stop/restart any node using the same storage and prove committed state returns without applying uncommitted data.

---

# Phase 11 — Duplicate Request Handling

## Goal

Make mutation retries safe when clients cannot know whether a timed-out request committed.

## Build

- `CommandFingerprint`
- `RequestDeduplicator`
- committed request reconstruction from committed log
- in-flight duplicate joining
- request-ID conflict handling
- router preservation of request IDs across retries

Required cases:

1. first request -> normal pipeline
2. same request ID + same command while in flight -> join existing operation
3. same request ID + same command after commit -> return original committed result
4. same request ID + different command -> reject conflict

## Exit condition

A client can retry after an unknown outcome and the logical mutation is applied exactly once within the defined deduplication model.

---

# Phase 12 — Docker Compose 3-Node Cluster

## Goal

Move from local-process tests to reproducible independent node processes with persistent storage.

## Build

- `Dockerfile`
- `compose.yaml`
- node-specific configuration
- one persistent volume per node
- health/readiness signal
- cluster scripts

Networking:

```text
node-1
node-2
node-3
```

Use Compose DNS/service names rather than fixed IP addresses.

## Exit condition

One command starts the cluster, smoke tests can write/read/delete through the configured leader, and restart preserves committed state.

---

# Phase 13 — Integration and Failure Tests

## Goal

Prove the portfolio-worthy distributed-system behaviors under faults.

## Build/automate

- healthy 3-node mutation/read scenario
- one-follower failure
- no-quorum scenario
- leader crash
- leader restart
- follower lag/catch-up
- crash during mutation
- duplicate retry after uncertain response
- stale follower read prevention
- WAL-tail corruption/truncation scenarios

Tests should inspect both API behavior and internal observable indexes when possible.

## Exit condition

Failure tests are automated, deterministic enough for repeated local execution, and document exactly which guarantee each scenario proves.

---

# Phase 14 — Metrics and Benchmarks

## Goal

Measure behavior without changing correctness semantics.

## Build

Metrics:

- request totals by operation/outcome
- mutation commit latency
- WAL durable append latency
- replication RPC latency
- quorum failure count
- follower lag
- lastLogIndex
- commitIndex
- appliedIndex
- duplicate retry count

Benchmarks should answer practical questions such as:

- healthy sequential write latency
- healthy concurrent write throughput
- read latency from leader
- performance with one slow follower while quorum is still available
- follower catch-up duration for a known backlog
- restart/replay duration for increasing WAL sizes

## Rules

- record hardware/environment
- warm up before measuring if appropriate
- report median and tail latency where useful
- never call the result a production benchmark
- correctness tests remain separate from performance tests

## Exit condition

`BENCHMARKS.md` contains reproducible methodology and results with limitations.

---

# Phase 15 — Documentation Polish

## Goal

Make the repository understandable to another engineer without needing a verbal explanation.

## Final documentation should cover

- what “Raft-inspired” means here
- what is deliberately not Raft
- architecture diagram
- write timeline
- read semantics
- durability semantics
- quorum model
- failure model
- recovery behavior
- duplicate retry semantics
- how to run tests
- how to run Docker cluster
- benchmark methodology
- known limitations

## README claim discipline

Good wording:

> A Java Raft-inspired replicated key-value store with a statically configured leader, durable WAL, 2-of-3 replication quorum, committed-state reads, follower catch-up, restart recovery, and retry deduplication.

Avoid wording such as:

- “production-grade Raft”
- “fully fault tolerant”
- “highly available consensus database”
- “exactly-once distributed execution”

unless later implementation genuinely earns those claims.

## Exit condition

A reviewer can determine architecture, guarantees, limitations, run instructions, and test evidence from the repository alone.

---

# 6. Test Plan Per Phase

## Phase 1 tests — State machine

### Unit

- PUT new key
- PUT overwrite
- GET existing
- GET missing
- DELETE existing
- DELETE missing
- arbitrary byte value handling
- repeated deterministic command sequence

### Integration

None required yet.

### Failure

None required yet.

### Proves

- state mutation semantics are deterministic
- GET behavior is independent from replication concerns

---

## Phase 2 tests — Log abstraction

### Unit

- first append gets expected index contract
- contiguous append
- get by index
- range retrieval preserves order
- reject index gap
- truncate only allowed suffix
- cannot truncate protected/committed range once that concept is introduced into the log contract

### Integration

- state-machine test can consume entries in log order using an in-memory log implementation

### Failure

- invalid index operations rejected

### Proves

- ordered history is modeled independently of storage/network

---

## Phase 3 tests — WAL

### Unit

- encode/decode ENTRY
- encode/decode COMMIT
- checksum passes valid record
- checksum rejects corruption
- append/reopen/replay
- multiple commit records recover highest valid commit index
- incomplete final record handling
- safe tail truncation
- committed-prefix corruption fails loudly

### Integration

- WAL-backed `PersistentLog` reconstructs entries correctly after reopen

### Failure

- torn final record
- corrupt uncommitted tail
- corruption affecting committed state

### Proves

- durability format survives restart and distinguishes recoverable tail damage from data loss

---

## Phase 4 tests — Protobuf/gRPC contracts

### Unit

- protobuf message round-trip
- bytes values preserved
- enum handling
- `ProtoMapper` domain conversion
- invalid client request validation

### Integration

- generated gRPC service/stub can make a minimal in-process call

### Failure

- malformed request maps to appropriate gRPC status

### Proves

- wire contract is stable before distributed behavior depends on it

---

## Phase 5 tests — Roles/readiness

### Unit

- config rejects invalid membership
- follower client API decisions return `NOT_LEADER`
- leader/follower hints are correct
- not-ready node rejects normal operations

### Integration

- start leader and follower on local ports
- GET to follower -> `NOT_LEADER`
- client mutation to follower -> `NOT_LEADER`

### Failure

- invalid config prevents startup

### Proves

- roles are configured, not elected
- followers cannot accidentally behave as leaders

---

## Phase 6 tests — Replication

### Unit

- follower rejects sender other than configured leader
- follower rejects unexpected/gapped index
- follower identical retry is idempotently accepted
- follower ACK happens only after mocked durable append completes
- replication manager contacts followers independently
- slow/unavailable follower is recorded without blocking forever

### Integration

- real gRPC leader-side follower client sends entry to real follower service
- follower WAL contains entry after positive ACK

### Failure

- follower unavailable
- follower deadline exceeded
- duplicate ReplicateEntry

### Proves

- a positive follower ACK means durable replicated storage, not merely receipt over the network

---

## Phase 7 tests — Majority commit

### Unit

- leader ACK only -> no quorum
- leader + follower-2 -> quorum
- leader + follower-3 -> quorum
- all three -> quorum
- duplicate ACK does not increase count
- commitIndex monotonic
- commit cannot exceed durable/log bounds
- applier cannot exceed commitIndex
- client success waits for commit persistence
- client success waits for leader application

### Integration

- 3 local nodes commit with all healthy
- commit succeeds with one follower unavailable
- commit fails with both followers unavailable

### Failure

- quorum timeout leaves entry unapplied
- commit-WAL force failure prevents success
- state application failure is treated as serious server failure, not success

### Proves

```text
success => majority durable + durable commit + applied leader state
```

---

## Phase 8 tests — Read path

### Unit

- follower GET always returns `NOT_LEADER`
- leader GET blocked while not ready
- leader GET reads applied state only
- appliedIndex must reach current commitIndex before normal response

### Integration

- PUT success followed by leader GET returns new value
- DELETE success followed by GET returns not found
- direct GET to lagging follower does not expose stale value

### Failure

- leader not ready during replay -> `NOT_READY`

### Proves

- baseline reads cannot accidentally come from stale followers
- acknowledged writes are visible to subsequent leader reads

---

## Phase 9 tests — Catch-up

### Unit

- calculate missing range from follower status
- no-op for already synchronized follower
- missing entries sent in order
- commit propagated only after required entries exist
- committed suffix truncation request rejected

### Integration

- pause/stop follower
- commit several entries using remaining quorum
- restart follower
- follower catches up entries
- follower receives commit advancement
- follower state eventually equals leader committed state

### Failure

- follower disappears mid-catch-up and later resumes
- follower reports unexpected suffix requiring safe uncommitted truncation

### Proves

- a lagging non-serving follower can converge without weakening healthy quorum behavior

---

## Phase 10 tests — Restart recovery

### Unit

- rebuild commitIndex from WAL
- rebuild applied state from committed prefix
- rebuild appliedIndex
- discard leader local uncommitted suffix as defined by design
- node cannot become READY before recovery finishes

### Integration

- commit data, stop server, restart same directory, verify state
- restart follower and verify catch-up
- restart entire local 3-node cluster and verify committed state

### Failure

Inject crash positions around:

- before ENTRY durable append
- after ENTRY durable append
- after follower durable ACK
- before durable COMMIT
- after durable COMMIT but before application
- after application but before client receives response

### Proves

- restart reconstructs committed truth and never promotes uncommitted entries into state

---

## Phase 11 tests — Deduplication

### Unit

- first ID -> NEW
- same ID/same command in flight -> JOIN_IN_FLIGHT
- same ID/same committed command -> ALREADY_COMMITTED
- same ID/different command -> CONFLICT
- committed dedup table rebuilds from WAL/log replay
- uncommitted request is not rebuilt as committed

### Integration

- send exact duplicate concurrently; only one logical log entry created
- retry after committed response loss; same commit index returned
- retry same ID with different value; conflict returned

### Failure

- crash after commit but before client response, restart, same request ID retry

### Proves

- unknown-outcome retries do not double-apply a committed logical mutation

---

## Phase 12 tests — Docker Compose

### Unit

No new algorithmic unit tests required.

### Integration

- build image
- start all 3 services
- readiness checks pass
- PUT/GET/DELETE smoke test
- node service-name connectivity
- independent volume verification

### Failure

- restart one container and prove volume-backed recovery

### Proves

- behavior works across independent processes/containers, not only in-process tests

---

## Phase 13 tests — Failure suite

### Integration/failure scenarios

#### Leader crash

- commit data
- hard-kill leader
- follower PUT/GET do not become successful
- no follower promotes itself
- restart configured leader
- recover committed state
- resume writes

#### One follower down

- kill follower
- writes still commit with leader + remaining follower
- restart follower
- follower catches up

#### Quorum loss

- stop both followers
- leader mutation fails/no quorum
- mutation remains unapplied

#### Follower lag

- deliberately delay follower
- healthy quorum continues
- lag metric increases
- catch-up restores convergence

#### Duplicate unknown-outcome retry

- commit operation
- suppress/drop client response
- retry same request ID
- return original committed result
- no duplicate logical effect

#### Stale read prevention

- lag follower intentionally
- direct follower GET returns `NOT_LEADER`, not old value

#### WAL damage

- incomplete uncommitted tail recovers safely
- committed-prefix corruption fails startup

### Proves

- the public claims in `PROJECT_SCOPE.md` are backed by executable evidence

---

## Phase 14 tests — Metrics/benchmarks

### Unit

- metric increments/gauges reflect component events
- metrics backend failure does not alter correctness

### Integration

- follower lag metric rises/falls during lag/catch-up
- commitIndex/appliedIndex metrics reflect actual internal state

### Benchmark

- sequential writes
- concurrent writes
- leader reads
- one slow follower
- backlog catch-up
- restart replay

### Proves

- system behavior can be observed and measured without relying on anecdotes

---

## Phase 15 tests — Documentation/reproducibility

### Verification

From a clean checkout:

- documented build command works
- documented test command works
- documented Docker command works
- documented smoke test works
- benchmark command works or clearly documents prerequisites

### Proves

- repository is reproducible for a reviewer

---

# 7. Invariants to Preserve

These should appear repeatedly in code comments near critical boundaries and, more importantly, in tests.

## 7.1 Never apply uncommitted entries

```text
StateMachineApplier may apply index N
only if N <= commitIndex.
```

An entry existing in the leader WAL or follower WAL does not make it committed.

## 7.2 Never return write success before majority durable ACK

In a fixed 3-node cluster:

```text
majority = 2 distinct durable copies
```

The leader's own copy counts only after its WAL append is durable.

A follower counts only after its WAL append is durable.

Network receipt alone is not an acknowledgment.

## 7.3 Durable commit before client success

After majority durable replication:

```text
append COMMIT(N)
-> force durable
-> advance commitIndex
-> apply
-> success
```

If durable commit recording fails, do not return success.

## 7.4 Never serve baseline GET successfully from follower

Follower client GET:

```text
NOT_LEADER + leader hint
```

This remains true even if the follower currently appears fully caught up.

## 7.5 Replay WAL before serving requests

A node must not become `READY` until local WAL recovery is complete and required role-specific reconciliation has completed.

## 7.6 Duplicate request IDs must not apply twice

Same request ID + same semantic mutation:

- join in-flight work, or
- return original committed result

Same request ID + different mutation:

- reject as conflict

## 7.7 `commitIndex` must never move backward

```text
newCommitIndex >= currentCommitIndex
```

Any attempt to regress it is an invariant violation.

## 7.8 `appliedIndex` must never exceed `commitIndex`

Always:

```text
0 <= appliedIndex <= commitIndex <= lastLogIndex
```

## 7.9 Application is ordered

The state machine applies:

```text
appliedIndex + 1
```

next. It may not skip ahead.

## 7.10 Committed entries are never truncated

Suffix truncation is allowed only beyond the local committed prefix.

If reconciliation asks a node to remove committed data, reject/fail loudly.

## 7.11 Positive follower replication ACK means durable storage

Do not acknowledge immediately after deserialization or file write buffering.

The follower's response contract represents successful durable append.

## 7.12 Configured leader is not discovered dynamically

No component may infer a new leader from:

- follower reachability
- highest log index
- lowest node ID
- timeout
- majority connectivity

The configured leader remains the leader until configuration changes outside the running v1 protocol.

## 7.13 Metrics do not participate in correctness

Metrics can fail, lag, or be disabled without changing replication/commit behavior.

---

# 8. Things Explicitly Out of Scope for v1

## Consensus / Raft features

- automatic/full Raft leader election
- election timers
- terms
- voting
- `RequestVote`
- candidate role
- Raft `(term, index)` log-matching protocol
- leader completeness proof/implementation
- formal Raft membership-change protocol
- leadership transfer

## Storage lifecycle

- snapshots
- snapshot installation
- log compaction
- automatic WAL rotation/retention system beyond what is minimally necessary to run tests

## Cluster topology

- dynamic membership
- node add/remove at runtime
- arbitrary quorum sizes
- multi-datacenter consensus
- sharding/partitioning

## Security

- TLS
- mTLS
- authentication
- authorization
- secret management

## Database features

- multi-key transactions
- compare-and-swap
- secondary indexes
- range scans
- SQL
- distributed transactions

## Advanced read protocols

- follower-served linearizable reads
- leader leases
- ReadIndex-style Raft reads

## Operational claims

Do not claim:

- production readiness
- production database reliability
- full Raft correctness
- automatic high availability
- exactly-once execution in the general distributed-systems sense
- Byzantine fault tolerance
- zero data loss under arbitrary storage failures

---

# 9. Recommended Phase Gates

Do not start the next phase merely because the current code compiles.

A phase is complete only when:

1. its responsibilities are isolated behind the intended interfaces
2. its unit tests pass
3. its relevant integration/failure tests pass
4. new invariants introduced in the phase are explicitly tested
5. documentation is updated if the implementation differs from the design
6. no out-of-scope feature was smuggled in to “make the demo work”

Particularly important gates:

### Before Phase 6

WAL durability semantics must already be trustworthy.

### Before Phase 7

Follower positive ACK must already mean durable storage.

### Before Phase 8

Commit/application ordering must already be tested.

### Before Phase 10

WAL replay and corruption behavior must already be tested independently.

### Before Phase 12

The local multi-node system must already work without Docker. Docker should package a working system, not become the debugging environment for unfinished core logic.

---

# 10. Suggested Development Rhythm

For each phase:

```text
1. Restate the phase invariant.
2. Define/adjust interfaces.
3. Write the smallest failing tests.
4. Implement only enough production behavior for the phase.
5. Add edge/failure tests.
6. Run all previous tests.
7. Review package boundaries.
8. Update docs if behavior changed.
9. Commit the phase as a coherent milestone.
```

This project will stand out more through **clear invariants, reproducible failure tests, and disciplined claims** than through adding many distributed-systems buzzwords.

---

# 11. Milestone View

## Milestone A — Deterministic durable core

Phases 1-3.

Result:

- correct state machine
- ordered log
- durable WAL/replay

No distributed-system claim yet.

## Milestone B — Replicated write pipeline

Phases 4-8.

Result:

- gRPC contracts
- configured leader/follower roles
- durable replication
- 2-of-3 commit
- applied leader reads

This is the first end-to-end distributed KV milestone.

## Milestone C — Failure recovery and idempotency

Phases 9-11.

Result:

- follower catch-up
- restart recovery
- retry deduplication

This is where the project becomes substantially stronger than a basic “three servers with gRPC” portfolio demo.

## Milestone D — Reproducible distributed validation

Phases 12-15.

Result:

- Docker Compose cluster
- automated failure scenarios
- metrics
- benchmarks
- polished documentation

This is the portfolio-ready milestone.

---

# 12. Reference Material

Use primary documentation while implementing:

- Apache Maven — build lifecycle: https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html
- Apache Maven — dependency mechanism: https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html
- gRPC Java generated code: https://grpc.io/docs/languages/java/generated-code/
- Protocol Buffers Java generated code: https://protobuf.dev/reference/java/java-generated/
- Docker Compose networking: https://docs.docker.com/compose/how-tos/networking/
- JUnit documentation: https://docs.junit.org/
- Raft paper/reference site for terminology and for understanding what this project intentionally does **not** fully implement: https://raft.github.io/

These references support implementation mechanics and terminology; the project's actual guarantees remain those stated in the design documents and proven by its tests.
