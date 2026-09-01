# API_DESIGN.md

## 1. API Principles

The API is split into two protobuf/gRPC surfaces:

1. **Client API** — PUT, GET, DELETE
2. **Internal replication API** — log replication, commit propagation, status, and safe uncommitted-suffix truncation

Transport/system errors use gRPC status codes. Expected distributed-system control outcomes such as `NOT_LEADER` are represented explicitly in application response messages so the router can make deterministic decisions.

## 2. Shared Data Types

## `NodeRole`

Values:

- `LEADER`
- `FOLLOWER`

No `CANDIDATE` value is needed in the baseline.

## `OperationType`

Values:

- `PUT`
- `DELETE`

GET is not logged because it does not mutate replicated state.

## `ResultCode`

Recommended values:

- `OK`
- `NOT_FOUND`
- `NOT_LEADER`
- `NO_QUORUM`
- `REQUEST_ID_CONFLICT`
- `NOT_READY`

Invalid wire requests should generally use gRPC `INVALID_ARGUMENT` instead of adding another application result.

## `LeaderHint`

Fields:

- `leader_id: string`
- `host: string`
- `port: int32`

## `LogEntry`

Fields:

- `index: int64`
- `request_id: string`
- `operation: OperationType`
- `key: string`
- `value: bytes` — populated only for PUT
- `command_fingerprint: bytes` — deterministic hash of semantic command contents

No Raft `term` field is present in the baseline because the system does not implement Raft elections/term rules.

## 3. Client Service

Conceptual service:

```text
KeyValueService
  Put(PutRequest)       -> MutationResponse
  Get(GetRequest)       -> GetResponse
  Delete(DeleteRequest) -> MutationResponse
```

This section specifies the schema without generating implementation code.

## PUT

### `PutRequest`

Fields:

- `request_id: string` — required UUID-like idempotency key
- `key: string` — required, non-empty
- `value: bytes` — required; empty byte array may be allowed as a valid value

### `MutationResponse`

Fields:

- `code: ResultCode`
- `request_id: string`
- `commit_index: int64` — valid for committed success / duplicate success
- `leader_hint: LeaderHint` — populated for `NOT_LEADER`
- `message: string` — human-readable diagnostics; clients must not parse it for behavior

### PUT semantics

`OK` means the mutation is committed and applied on the leader.

A retry with the same request ID and identical command returns `OK` with the original committed index.

## GET

### `GetRequest`

Fields:

- `key: string`

Optional later tracing field:

- `request_id: string`

A request ID is not required for correctness because GET is read-only.

### `GetResponse`

Fields:

- `code: ResultCode`
- `key: string`
- `value: bytes`
- `commit_index: int64` — leader commit index observed by the read
- `leader_hint: LeaderHint` — populated for `NOT_LEADER`
- `message: string`

Semantics:

- `OK`: key exists; value populated
- `NOT_FOUND`: leader successfully checked applied state and key does not exist
- `NOT_LEADER`: caller hit a follower
- `NOT_READY`: leader is still recovering/reconciling

## DELETE

### `DeleteRequest`

Fields:

- `request_id: string` — required
- `key: string` — required

Response uses `MutationResponse`.

DELETE is designed as an idempotent state mutation: committing deletion of a missing key is still a valid committed command. The API does not promise a previous-value return because that would complicate duplicate-result reconstruction unnecessarily.

## 4. Internal Replication Service

Conceptual service:

```text
ReplicationService
  ReplicateEntry(ReplicateEntryRequest)             -> ReplicateEntryResponse
  AdvanceCommit(AdvanceCommitRequest)               -> AdvanceCommitResponse
  GetReplicationStatus(ReplicationStatusRequest)    -> ReplicationStatusResponse
  TruncateUncommittedSuffix(TruncateSuffixRequest)  -> TruncateSuffixResponse
```

All internal calls are unary in the first implementation. This keeps failure behavior easy to reason about. Streaming or batching can be added later if profiling justifies it.

## `ReplicateEntryRequest`

Fields:

- `leader_id: string`
- `entry: LogEntry`
- `leader_commit_index: int64`

`leader_commit_index` lets a follower learn about an already-known commit position during normal traffic.

## `ReplicateEntryResponse`

Fields:

- `follower_id: string`
- `accepted: bool`
- `last_persisted_index: int64`
- `commit_index: int64`
- `message: string`

Follower sends success only after the new entry is durable.

The follower accepts an entry only when:

- sender is configured leader
- node has completed recovery
- entry is valid
- entry position is compatible with its current expected log position

A mismatch causes rejection and the leader switches to status/reconciliation logic instead of blindly appending.

## `AdvanceCommitRequest`

Fields:

- `leader_id: string`
- `commit_index: int64`

Follower behavior:

1. verify sender is configured leader
2. verify commit index does not exceed follower's durably stored last log index
3. durably persist commit advancement
4. apply entries through new commit index
5. reply

## `AdvanceCommitResponse`

Fields:

- `follower_id: string`
- `commit_index: int64`
- `applied_index: int64`

## `ReplicationStatusRequest`

Fields:

- `leader_id: string`

## `ReplicationStatusResponse`

Fields:

- `node_id: string`
- `role: NodeRole`
- `last_log_index: int64`
- `commit_index: int64`
- `applied_index: int64`
- `ready: bool`

This is used for startup reconciliation and follower catch-up.

## `TruncateSuffixRequest`

Fields:

- `leader_id: string`
- `truncate_after_index: int64`

Follower may truncate only entries with indexes greater than its own committed index.

If the request would remove a committed entry, the follower rejects it with a serious precondition/data-integrity error.

## `TruncateSuffixResponse`

Fields:

- `node_id: string`
- `last_log_index: int64`
- `commit_index: int64`

## 5. Error Model

## Application result codes

Use application codes when the RPC reached a healthy server and the server is intentionally communicating cluster/application state.

### `NOT_LEADER`

Used when client API reaches a follower.

Router behavior: use leader hint and retry the configured leader.

### `NO_QUORUM`

Used when the leader cannot get a durable majority within the operation deadline.

Router behavior: retry only with bounded backoff and the same mutation request ID.

### `REQUEST_ID_CONFLICT`

Used when the same request ID was previously associated with different mutation contents.

Router behavior: do not retry automatically; caller must generate a new request ID for a genuinely new mutation.

### `NOT_READY`

Used while a node is recovering or reconciling.

Router behavior: retry with bounded backoff.

## gRPC status codes

Recommended mapping:

- `INVALID_ARGUMENT` — malformed request, missing request ID, invalid key, oversize value
- `UNAVAILABLE` — process/network unavailable; internal replication peer unavailable
- `DEADLINE_EXCEEDED` — RPC deadline elapsed
- `FAILED_PRECONDITION` — dangerous internal operation rejected, such as attempting to truncate committed data
- `DATA_LOSS` — committed WAL corruption detected
- `INTERNAL` — invariant violation / unexpected implementation fault

Do not use `INTERNAL` as a generic substitute for expected cluster behavior.

## 6. Deadlines

Every RPC should have a finite deadline.

Suggested categories rather than hard-coded production promises:

- client mutation deadline: configurable, e.g. seconds
- follower replication RPC: shorter sub-deadline within mutation deadline
- status/catch-up RPC: configurable

The leader must not wait forever for a follower.

## 7. Retry Behavior

Keep retry policy visible in the client/router instead of hiding correctness inside aggressive automatic retry.

### Client PUT / DELETE

Retry only when:

- node returned `NOT_LEADER`
- node returned `NO_QUORUM` and retry budget allows
- RPC failed with retryable transient transport status such as `UNAVAILABLE`
- RPC deadline expired and caller accepts unknown-outcome retry

Requirements:

- preserve the exact same `request_id`
- use bounded attempts
- use exponential backoff
- do not retry forever

### Client GET

GET may safely retry because it has no mutation effect.

Retry on:

- `NOT_LEADER`
- `NOT_READY`
- transient `UNAVAILABLE`

### Internal replication

Replication manager may retry follower RPCs with bounded backoff.

A retry of `ReplicateEntry` is safe because the follower detects an already-durable identical entry and acknowledges it instead of appending a duplicate logical entry.

## 8. Request ID / Deduplication Design

## Client responsibility

For each logical PUT or DELETE, the client creates one unique request ID.

Example conceptual value:

```text
UUID
```

The same ID is reused for every retry of that logical operation.

## Fingerprint

The leader computes a deterministic fingerprint over:

- operation type
- key
- value bytes, if PUT

Request ID itself is not part of the semantic command fingerprint.

## Committed request table

Logical structure:

```text
request_id -> {
    fingerprint,
    committed_log_index
}
```

It is rebuilt on startup from committed log entries rather than maintained as a second authoritative database.

## In-flight request table

In memory only:

```text
request_id -> in-flight completion handle
```

If an identical retry arrives while the original request is still running, it joins the same completion rather than receiving a new log index.

## Conflict rule

If request ID matches but command fingerprint differs:

```text
REQUEST_ID_CONFLICT
```

This catches accidental idempotency-key reuse instead of silently applying an unexpected command.

## Recovery rule

Only entries at indexes `<= commitIndex` rebuild the committed deduplication table.

Uncommitted entries are not treated as completed client operations.

## 9. Protobuf Evolution Rules

Keep schemas easy to evolve:

- never renumber existing fields
- never reuse removed field numbers
- use enums with a zero/default `UNSPECIFIED` value in the actual schema
- add fields rather than changing existing field meaning
- keep external client messages separate from internal replication messages

## 10. Why Unary RPCs First

Unary RPCs are preferred initially because they make it easy to test:

- exactly when a follower durably acknowledges
- timeout handling
- duplicate delivery
- follower failure
- request/response correlation

A later optimization phase may batch entries or use streaming, but only after the correctness tests pass.

## References

1. gRPC core concepts / deadlines: https://grpc.io/docs/what-is-grpc/core-concepts/
2. gRPC status codes: https://grpc.io/docs/guides/status-codes/
3. gRPC retry guidance: https://grpc.io/docs/guides/retry/
4. Protocol Buffers Java basics: https://protobuf.dev/getting-started/javatutorial/
5. Protocol Buffers Java generated-code guide: https://protobuf.dev/reference/java/java-generated/
