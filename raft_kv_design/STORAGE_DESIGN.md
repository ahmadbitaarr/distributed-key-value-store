# STORAGE_DESIGN.md

## 1. Storage Goals

The storage layer must make these invariants testable:

1. A node never reports a replication acknowledgment before the entry is durably written.
2. An uncommitted entry is never applied to the key-value state machine.
3. A committed entry survives process restart if its WAL remains intact.
4. Recovery can distinguish committed state from an uncommitted crash tail.
5. A duplicate committed request can be recognized after restart.

The baseline intentionally avoids a separate embedded database. The durable source of truth is the WAL; the key-value map is rebuilt in memory from committed log entries.

## 2. Logical Replicated Log

Each mutation is represented by one `LogEntry`.

Fields:

- `index`
- `request_id`
- `operation`
- `key`
- `value` for PUT
- `command_fingerprint`

Indexes begin at 1 and are contiguous within the authoritative leader log.

Logical invariant:

```text
entry[n].index == n
```

The baseline has no Raft term field.

## 3. WAL Design

Each node owns one append-only WAL file in its persistent node storage directory.

Suggested conceptual record types:

- `ENTRY`
- `COMMIT`

A WAL record is framed so recovery can find boundaries and detect torn/corrupted tails.

Conceptual framing:

```text
+------------+-------------+----------+----------------+----------+
| magic/ver  | record type | length   | payload bytes  | checksum |
+------------+-------------+----------+----------------+----------+
```

The exact binary format is an implementation detail to define when coding begins.

### Checksum

Use a checksum such as CRC32C over the stable record contents.

Purpose:

- detect accidental record corruption
- distinguish a bad record from valid bytes
- make recovery tests more meaningful

This is corruption detection, not cryptographic integrity.

## 4. ENTRY Record

An ENTRY record contains the serialized logical `LogEntry`.

Leader sequence:

1. append ENTRY
2. force WAL to durable storage
3. count local durable acknowledgment

Follower sequence:

1. validate entry
2. append ENTRY
3. force WAL to durable storage
4. return durable replication acknowledgment

## 5. COMMIT Record

Instead of treating `commitIndex` as purely in-memory metadata, commit advancement is also recorded durably in the WAL.

Conceptual payload:

```text
commit_index: int64
```

Leader commit sequence:

1. achieve durable majority for entry N
2. append `COMMIT(N)`
3. force WAL
4. advance in-memory `commitIndex`
5. apply through N
6. return success to client

Follower commit sequence:

1. receive `AdvanceCommit(N)`
2. verify N is not beyond its durable log
3. append `COMMIT(N)`
4. force WAL
5. advance local `commitIndex`
6. apply through N

This makes committed state recoverable without introducing a second authoritative metadata store.

## 6. Commit Index

Definition:

```text
commitIndex = highest log index known locally to be committed
```

In the leader baseline, the leader is the authority that advances the cluster commit index.

Invariant:

```text
0 <= appliedIndex <= commitIndex <= lastLogIndex
```

Leader only advances commit after durable majority acknowledgment.

Client mutation success is returned only after the leader's `COMMIT` record is durable.

## 7. Applied Index

Definition:

```text
appliedIndex = highest committed entry already applied to the local key-value state machine
```

`appliedIndex` is **in memory only** in the baseline.

Reason:

- the state machine itself is in memory
- both can be rebuilt together on restart
- persisting `appliedIndex` adds another durability protocol without improving the baseline correctness model

Recovery initializes:

```text
appliedIndex = 0
```

and replays committed entries through recovered `commitIndex`.

## 8. State Machine

Baseline state machine:

```text
Map<String, byte[]>
```

Commands:

### PUT

```text
map[key] = value
```

### DELETE

```text
remove key if present
```

DELETE remains deterministic whether or not the key existed.

GET is not a log command. It reads the leader's applied map.

## 9. State-Machine Replay

Startup replay process:

1. scan WAL from beginning
2. validate record framing and checksum
3. reconstruct ordered ENTRY records
4. recover highest durable COMMIT index
5. validate `commitIndex <= lastLogIndex`
6. initialize empty key-value map
7. apply ENTRY 1 through `commitIndex` in order
8. set `appliedIndex = commitIndex`
9. rebuild committed request-ID table from entries 1 through `commitIndex`

This produces the same logical map for nodes that share the same committed log prefix.

## 10. Restart Recovery

## Clean restart

If WAL is intact:

- recover entries
- recover commit index
- replay committed prefix
- rebuild deduplication state
- resume role-specific reconciliation

## Torn/incomplete final record

If process termination leaves an incomplete record at the physical end of the WAL:

- if it is beyond the committed durable prefix, truncate the incomplete tail to the last valid record boundary
- continue recovery

## Checksum failure in uncommitted tail

Treat as an invalid uncommitted suffix and truncate to the last safe boundary, provided no committed record depends on it.

## Corruption in committed prefix

Fail startup loudly as data loss.

Do not silently skip a committed record because that would permit different state machines to reconstruct different state.

## 11. Uncommitted Suffix Handling

Because the baseline never elects another leader, the configured leader's recovered committed prefix is authoritative after restart.

Before accepting new writes, the leader discards its own uncommitted suffix beyond recovered `commitIndex`.

For each follower:

1. fetch status
2. ensure follower has not committed beyond leader's recovered commit index
3. if follower has extra uncommitted entries, truncate only indexes greater than follower `commitIndex`
4. send authoritative missing entries from leader
5. propagate current commit index

This is a **static-leader recovery rule**, not a substitute for Raft term/log-matching rules.

## 12. Follower Catch-Up

Follower status exposes:

- `lastLogIndex`
- `commitIndex`
- `appliedIndex`

Leader compares status with its own log.

### Follower behind

Example:

```text
leader lastLogIndex = 20
follower lastLogIndex = 16
```

Leader sends entries 17 through 20 in order.

After durable append, leader sends current commit index.

### Follower has extra uncommitted tail

Example:

```text
leader recovered commitIndex = 15
leader lastLogIndex = 15
follower commitIndex = 15
follower lastLogIndex = 17
```

Follower may safely truncate 16-17 because they are explicitly uncommitted under the single configured leader model.

### Dangerous mismatch

If a follower reports a committed index greater than the leader's recovered commit index, startup/reconciliation must stop with an invariant violation rather than guessing.

## 13. Request Deduplication Persistence

There is no separate request database in the baseline.

Committed request table is reconstructed from committed ENTRY records:

```text
request_id -> fingerprint + committed index
```

Advantages:

- one durable source of truth
- no cross-file transaction between request table and WAL
- simple recovery tests

In-flight duplicate tracking remains memory-only because unfinished requests are not guaranteed to commit.

## 14. What Is Persisted

Persisted per node:

- ENTRY WAL records
- COMMIT WAL records
- WAL format version information
- node storage directory / file itself on a persistent Docker volume

Configuration may come from environment/config files and does not need to be in the WAL.

## 15. What Is Only In Memory

- key-value map
- `appliedIndex`
- request in-flight table
- reconstructed committed request lookup table
- log index -> WAL file offset index
- follower health/lag status
- gRPC channels/stubs
- current retry state
- performance counters that do not need restart persistence

## 16. Durability Semantics

A node's replication acknowledgment means its WAL append completed and was forced to the storage API used by the process.

The project should describe this as **application-level durable WAL acknowledgment**, not as a guarantee against every possible hardware/controller/filesystem failure mode.

In Docker Compose, each node must use a persistent volume so container recreation does not automatically erase the WAL.

## 17. No Snapshot / Compaction in Baseline

The WAL grows monotonically during the project baseline.

Reasons to defer compaction:

- easier recovery reasoning
- easier failure tests
- deduplication reconstruction remains simple
- avoids snapshot/install-snapshot protocol complexity

A future extension can add snapshotting only after recovery and replication tests are stable.

## References

1. Raft paper — replicated logs and state machines: https://raft.github.io/raft.pdf
2. Protocol Buffers Java generated-code guide: https://protobuf.dev/reference/java/java-generated/
3. Docker Compose networking/persistence environment context: https://docs.docker.com/compose/how-tos/networking/
