# Phase 12 — Docker Compose deployment (static leader)

Source baseline: `5c2ec2bdef1b8583070a080de88236318512ac7f`.

## Prerequisites

Windows 10/11 with Docker Desktop (Linux containers), Docker Compose v2, Windows PowerShell 5.1+ or PowerShell 7, Java 21 + Maven for host regression runs. Docker Engine must be running. No WSL commands are required from the user. The image *itself* builds on Linux in the Docker builder.

Run from `raft-kv-store/`:

```powershell
docker version
docker compose version
docker compose config
.\scripts\cluster-up.ps1
.\scripts\cluster-status.ps1
.\scripts\smoke-test.ps1
.\scripts\deployment-acceptance.ps1
```

Cluster: `node-1` (`n1`, sole static leader), `node-2` (`n2`) and `node-3` (`n3`) followers. All containers bind `50051`, and **only** `127.0.0.1:50051` is published to the host. Within the private Compose network, DNS names `node-1`, `node-2`, and `node-3` resolve their peers. No dynamic elections or failover are implemented.

## Build and runtime

`Dockerfile` uses `maven:3.9.13-eclipse-temurin-21` and `eclipse-temurin:21-jre-jammy`; check/update digest pins for release hardening. Builder generates native Protobuf/gRPC sources on Linux, then runs Maven `package` without tests. The Maven Shade Plugin merges `META-INF/services` and creates a single executable `NodeMain` jar. The runtime contains only the JRE and shaded jar. Its foreground JVM gets Docker stop signals directly. UID/GID 10001 owns `/var/lib/raftkv`.

To inspect actual Linux/native protobuf execution (do **not** claim this has run until Docker logs establish it):

```powershell
docker compose build --no-cache --progress=plain
docker compose up -d
docker compose ps
docker compose images
```

**Do not** publish the follower ports or use a sidecar to inspect live WALs.

## Durability and ownership

Three *distinct named volumes* `raftkv-node1-data`, `raftkv-node2-data`, and `raftkv-node3-data` mount at `/var/lib/raftkv`, where each node exclusively owns `raftkv.wal` and its WAL lease. Docker image rebuild/replacement never deletes these volumes.

```powershell
.\scripts\restart-node.ps1 -Node node-3
.\scripts\kill-node.ps1 -Node node-1
# After a deliberate kill:
docker compose start node-1
.\scripts\cluster-down.ps1       # docker compose down, preserving named volumes
```

**DESTRUCTIVE OPERATION:** `docker compose down -v` deletes the named WAL volumes. This is never called by scripts. Only explicitly destroy volumes in a disposable test environment after backing up data. There is no automatic reconstruction of a lost leader WAL from surviving followers.

## Health versus write quorum

Compose health probes are separate read-only Java processes using the packaged gRPC classes. They do not open, scan, or lock WAL files. Leader health is a bounded valid-key `GET` returning `OK` or `NOT_FOUND` with a valid committed-state index. This means **read-ready**, not guaranteed write quorum. Follower health is a bounded `GetReplicationStatus` call validating node identity, FOLLOWER role, and `0 <= applied <= committed <= last`. CONTROL_ONLY is healthy; terminal FAILED/STOPPING/STOPPED cannot answer healthy status. The recovery-witness requirement is not bypassed; an unwitnessed recovered leader stays unready even if its TCP port is bound. Probes use local loopback gRPC; normal settings are interval 5s, timeout 5s, startup grace 15s and 4 retries.

Explicit `restart: "no"` avoids silently reviving killed nodes during deterministic fault tests. No circular `depends_on` health dependencies. Compose stop grace is 75s vs 20s configured node shutdown grace.

## Smoke client and deterministic IDs

Use the real protobuf RPC CLI packaged in the image, not HTTP. `--value-base64=` **is required for PUT** and represents an explicitly present empty byte array; for nonempty data encode the actual bytes first. `--expect`, `--expect-index`, and `--expect-value-base64` make the CLI return nonzero on mismatches. JSON output includes `code`, `commit_index`, `has_value`, `value_base64`, and replication-status indexes.

```powershell
docker compose exec -T node-1 java -cp /opt/raftkv/app.jar io.github.ahmadbitaarr.raftkv.client.DeploymentSmokeClient put --key=demo --request-id=demo-001 --value-base64=YWJj --expect=OK
docker compose exec -T node-1 java -cp /opt/raftkv/app.jar io.github.ahmadbitaarr.raftkv.client.DeploymentSmokeClient get --key=demo --expect=OK --expect-value-base64=YWJj
docker compose exec -T node-1 java -cp /opt/raftkv/app.jar io.github.ahmadbitaarr.raftkv.client.DeploymentSmokeClient retry-put --key=demo --request-id=demo-001 --value-base64=YWJj --expect=OK
```

For repeatable tests pass explicit request IDs, keys, and Base64 values. A reused ID with changed command must yield `REQUEST_ID_CONFLICT`. The included `scripts/smoke-test.ps1` uses unique keys by default, or accepts `-Key` and `-RequestId` for persistent volume checks. If those IDs already committed a *different* value, conflict is expected.

## Acceptance / recovery scenarios

Run `scripts/deployment-acceptance.ps1` **only** on your disposable local Compose test cluster. It builds and starts nodes, asserts health and non-root writable storage, runs real gRPC smoke, stops one follower and verifies majority writes/catch-up, gracefully restarts the leader, hard-kills the leader and verifies dedup persistence, checks NO_QUORUM and witnessless recovery, then recreates containers without destroying volumes and retries an exact committed request ID. Failures return nonzero and preserve the cluster/WAL for diagnostics.

Check diagnostics:

```powershell
.\scripts\cluster-logs.ps1 -Node node-1 -Tail 150
.\scripts\cluster-status.ps1
docker compose ps --format json
docker compose logs --since 5m
```

## Limitations

This is a **local plaintext gRPC demo**, not secure over untrusted networks. TLS/authentication, secret management, remote cloud deployment, leader election, snapshot/compaction, and full production metrics/fault frameworks are outside Phase 12. Native Linux image build, Docker persistence and Windows Maven execution must be verified separately on a machine with those tools. The Shade and Compose declarations alone do not constitute proof of execution.

## Phase 12 final Windows acceptance update — C05 and G01 (October 28, 2026)

The previously recorded successful all-scenario deployment acceptance used an existing image and `--no-build` after a temporary Docker Hub DNS error; that run reported 3/3 healthy, `initialIndex=7`, `majorityIndex=8`, `durableLastIndex=8`. **No image rebuild took place during that earlier acceptance run.**

**C05 — PASS in a separate later focused run.** Windows console evidence records a durable PUT at original commit index **9** on prior image `sha256:afae0c12956bc1f55207eaad9c1f31b184c7928a8ace4257ed8b4acd697ebb38`. The user then executed `docker compose build --no-cache` successfully, producing `sha256:35dd01867a91bcd10386ce88f36af8934729ae2861cb4b2fdbd3c1fdb95c58f1`, and ran `docker compose up --detach --no-build --force-recreate` without deleting named WAL volumes. The recreated leader's running image was checked to match the rebuilt image. A GET retrieved the pre-rebuild value, an exact request-ID retry returned index **9** without increasing `last_log_index`, and all three containers were healthy.

**G01 — PASS in a distinct later Windows one-command startup run.** Running the corrected `.\scripts\cluster-up.ps1` built `raftkv:phase12`, started all three containers healthy, and exited **0**. `docker image inspect` reported `sha256:05f15d3e9789c4f28d5bf88ac3ca039fa103f4ed42274582f0ad8435d433b674`; `git diff --check` produced no diagnostics. The G01 image ID is not the C05 image ID because the runs are separate build operations.

See the two Windows transcripts in the Planning deliverables package and the updated `PHASE12_DOCKER_BUILD_PROVENANCE_AND_C05.md`. The previous 595 passing Maven tests were executed separately on Windows. No new Java code/tests are modified for this documentation update; full Maven/Docker commands were **not** rerun by the packaging assistant.
