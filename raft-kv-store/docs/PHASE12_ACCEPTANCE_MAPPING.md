# Phase 12 — A01–H08 acceptance mapping

Approved baseline: `5c2ec2bdef1b8583070a080de88236318512ac7f`.

**Execution legend:** `WINDOWS MAVEN PASS` refers to the supplied Windows Maven transcript (595 executed / zero failures, errors or skips). `WINDOWS ACCEPTANCE REPORTED PASS` and `WINDOWS CLUSTER REPORTED PASS` refer to the user's successful Windows Docker run using a prebuilt image (`--no-build`); this report does not contain its full raw Docker transcript. `SEPARATE DOCKER BUILD REPORTED PASS` refers to an earlier independent image build, not the acceptance run. `STATIC REVIEW CHECKED` is not a runtime test. `UNRESOLVED` means no adequate execution evidence.

| ID | Scenario | Concrete test/assertion or command | Execution status |
|---|---|---|---|
| A01 | Linux Java 21 builder compilation | `Dockerfile: RUN mvn -DskipTests package; docker compose build --no-cache` | SEPARATE DOCKER BUILD REPORTED PASS — Docker build log/image ID not supplied here; not built during acceptance |
| A02 | Linux native Protobuf/gRPC generation | `Dockerfile: protobuf-maven-plugin generate; docker compose build --no-cache` | SEPARATE DOCKER BUILD REPORTED PASS — Docker build log/image ID not supplied here; not built during acceptance |
| A03 | Shaded executable includes runtime dependencies | `pom.xml:maven-shade-plugin; java -cp shaded JAR DeploymentHealthProbe --help` | WINDOWS MAVEN PACKAGE PASS — Shade JAR built; running container subsequently reported healthy |
| A04 | NodeMain launches from runtime stage | `docker compose up -d --build; docker compose ps` | WINDOWS CLUSTER REPORTED PASS — 3/3 healthy; acceptance used existing image (`--no-build`) |
| A05 | Netty/gRPC service provider works in JAR | `scripts/smoke-test.ps1 (real unary gRPC)` | WINDOWS CLUSTER REPORTED PASS — 3/3 healthy; acceptance used existing image (`--no-build`) |
| A06 | Runtime excludes Maven and development files | `scripts/deployment-acceptance.ps1: command -v mvn, /opt/raftkv/src check` | WINDOWS ACCEPTANCE REPORTED PASS — script checks runtime Maven absence/non-root/WAL writability |
| A07 | Nonroot runtime writes own WAL directory | `scripts/deployment-acceptance.ps1: id -u=10001, test -w` | WINDOWS ACCEPTANCE REPORTED PASS — script checks runtime Maven absence/non-root/WAL writability |
| A08 | Clean Docker build from approved tree | `docker compose build --no-cache (Dockerfile + .dockerignore)` | SEPARATE DOCKER BUILD REPORTED PASS — Docker build log/image ID not supplied here; not built during acceptance |
| B01 | Exactly three node services | `compose.yaml: services node-1/2/3; PyYAML parse` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| B02 | node-1 configured sole leader | `Phase12ComposeConfigTest.allThreeComposeNodeConfigurationsValidateAndAgree; Java 21 config check` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| B03 | node-2/node-3 followers | `Phase12ComposeConfigTest.allThreeComposeNodeConfigurationsValidateAndAgree; Java 21 config check` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| B04 | Identical cluster identity/membership | `Phase12ComposeConfigTest.allThreeComposeNodeConfigurationsValidateAndAgree; Java 21 topology digest check` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| B05 | Docker DNS on bridge network | `scripts/deployment-acceptance.ps1: real three-node writes` | WINDOWS CLUSTER REPORTED PASS — 3/3 healthy; acceptance used existing image (`--no-build`) |
| B06 | Valid bind ports/endpoints | `Phase12ComposeConfigTest.allThreeComposeNodeConfigurationsValidateAndAgree; Java 21 config check` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| B07 | No healthy dependency cycle | `compose.yaml: no depends_on; PyYAML parse` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| B08 | Loopback leader-only host port | `compose.yaml: node-1 ports; no follower ports; PyYAML parse` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| C01 | Three separate named WAL volumes | `compose.yaml: raftkv-node{1,2,3}-data; PyYAML parse` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| C02 | Nonroot can write named WAL volume | `scripts/deployment-acceptance.ps1: test -w /var/lib/raftkv + PUT` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| C03 | Normal stop/start preserves data | `scripts/deployment-acceptance.ps1: stop/restart node-1, GET` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| C04 | Full recreation persists data | `scripts/deployment-acceptance.ps1: down/up without -v, GET` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| C05 | Rebuild preserves WAL | Real Windows C05: `docker compose build --no-cache`; `docker compose up --detach --no-build --force-recreate`; original-index GET and retry + stable WAL last index | **WINDOWS C05 PASS — execution transcript**: index **9** preserved, no duplicate append, 3/3 healthy; prior image `afae0c12…`, rebuilt/running image `35dd0186…` |
| C06 | WAL lease exclusive ownership | `scripts/deployment-acceptance.ps1: second NodeMain process must fail lease` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| C07 | SIGKILL restart restores durable state | `scripts/deployment-acceptance.ps1: kill/start node-1, GET+original index` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| C08 | Cluster-down never removes WAL volumes | `scripts/cluster-down.ps1: docker compose down (without -v)` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| D01 | Leader fresh readiness accurate | `DeploymentHealthProbeTest.validLeaderNotFoundAndPresentEmptyValuesCountAsReadyReads; scripts/cluster-up.ps1` | WINDOWS CLUSTER REPORTED PASS — 3/3 healthy; acceptance used existing image (`--no-build`) |
| D02 | Witnessless recovery not READY | `scripts/deployment-acceptance.ps1: stop followers, restart leader, GET NOT_READY` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| D03 | Witness authorizes recovered leader | `scripts/deployment-acceptance.ps1: start node-2; Wait-NodeHealth node-1` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| D04 | Follower CONTROL_ONLY health | `DeploymentHealthProbeTest.followerControlPlaneAllowsDurableLagButRequiresCorrectIdentity; Compose health` | WINDOWS CLUSTER REPORTED PASS — 3/3 healthy; acceptance used existing image (`--no-build`) |
| D05 | Bound unready RPC fails probe | `DeploymentHealthProbeTest.leaderProbeNeverMistakesNotReadyOrStaleFollowerForReadiness` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| D06 | FAILED follower cannot pass probe | `ReplicationGrpcServiceTest.failedFollowerCannotAdvertiseHealthyReplicationStatus + stoppingFollowerCannotAdvertiseHealthyReplicationStatus` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| D07 | Probe never opens WAL | `DeploymentHealthProbe.probe(): only GET/GetReplicationStatus; source inspection` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| D08 | Bounded non-cyclic probes | `DeploymentHealthProbe parseArgs and deadline; compose healthcheck config` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| E01 | PUT through container leader | `scripts/smoke-test.ps1: Invoke-NodeRpc retry-put (Phase 11 client)` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| E02 | GET returns committed value | `scripts/smoke-test.ps1: get and base64 equality` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| E03 | DELETE then NOT_FOUND | `scripts/smoke-test.ps1: delete then get` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| E04 | Empty PUT has value presence | `scripts/smoke-test.ps1: value-base64=, nonnull empty GET` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| E05 | Exact ID returns original commit index | `scripts/smoke-test.ps1: retry-put / retry-delete indexes` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| E06 | Conflict has no new commit | `scripts/smoke-test.ps1: REQUEST_ID_CONFLICT + last_log unchanged` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| E07 | Follower GET returns NOT_LEADER | `scripts/smoke-test.ps1: get host=node-2` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| E08 | Smoke exit nonzero on failed assertions | `scripts/smoke-test.ps1: catch/exit 1; Invoke-NodeRpc checks exit` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| F01 | One follower outage: majority write | `scripts/deployment-acceptance.ps1: stop node-3, majority PUT` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| F02 | Restarted follower catch-up without new write | `scripts/deployment-acceptance.ps1: start node-3, Wait-Replicated` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| F03 | Both followers lost: no success | `scripts/deployment-acceptance.ps1: stop node-2/3, PUT!=OK` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| F04 | Leader clean restart recovers data/index | `scripts/deployment-acceptance.ps1: restart node-1, GET, duplicate index` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| F05 | Leader SIGKILL restart preserves state | `scripts/deployment-acceptance.ps1: SIGKILL/start node-1, GET` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| F06 | Full three-container recreation preserves history | `scripts/deployment-acceptance.ps1: compose down/up, GET and retry` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| F07 | Mandatory recovery witness before READY | `scripts/deployment-acceptance.ps1: stop both peers, leader restart, GET NOT_READY` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| F08 | No second committed duplicate after recovery | `scripts/deployment-acceptance.ps1: duplicate index+last_log after SIGKILL` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| G01 | One-command build+start | Real Windows: `.\scripts\cluster-up.ps1` | **WINDOWS G01 PASS — execution transcript**: Compose image build and 3/3 healthy; startup exit **0**; image `05f15d3e…`; `git diff --check` clean |
| G02 | Windows PowerShell without WSL | `scripts/*.ps1 use native docker compose/PowerShell` | WINDOWS POWERSHELL REPORTED PASS — native Compose/PowerShell; no WSL needed |
| G03 | Normal down preserves volumes | `scripts/cluster-down.ps1: docker compose down, no -v` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| G04 | Individual-node restart | `scripts/restart-node.ps1 -Node node-3` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| G05 | Individual-node SIGKILL | `scripts/kill-node.ps1 -Node node-3` | WINDOWS ACCEPTANCE REPORTED PASS — real Compose run used `--no-build`; see evidence/provenance report |
| G06 | Diagnostics identify node | `scripts/cluster-status.ps1 and cluster-logs.ps1` | WINDOWS CLUSTER REPORTED PASS — 3/3 healthy; acceptance used existing image (`--no-build`) |
| G07 | 75s Docker grace vs 20s config | `compose.yaml stop_grace_period and docker/config shutdown deadline` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| G08 | Destructive data/TLS limitations documented | `docs/PHASE12_DOCKER_DEPLOYMENT.md` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |
| H01 | Preserve 584 Phase 1–11 regressions | `mvn clean test (584 prior verified externally; corrected suite 595 authored)` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| H02 | Majority durability/quorum preserved | `Phase7RealThreeNodeTest + AckTrackerTest; mvn clean test` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| H03 | WAL force/rewrite unchanged | `FileWalStoreTest + WalRecoveryTest; mvn clean test` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| H04 | Read consistency and follower restriction | `Phase8RealThreeNodeTest + ClientKvGrpcServiceTest; mvn clean test` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| H05 | Catch-up and live-write priority | `Phase9RealThreeNodeCatchUpTest + ReplicationManagerCatchUpPriorityTest` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| H06 | Recovery certificates and empty-WAL | `Phase10RecoveredLeaderAdmissionTest + Phase10RealThreeNodeRecoveryTest` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| H07 | Dedup/retry ID preserved | `Phase11RealThreeNodeDedupTest + RequestDeduplicatorTest + RetryingMutationClientTest` | WINDOWS MAVEN PASS — 595 tests; 0 failures/errors/skips; see supplied Maven log |
| H08 | No elections/WAL redesign | `Source diff: only Docker, packaging, probe, CLI, one terminal status guard` | STATIC REVIEW CHECKED — source/Compose assertions, not separate operational execution |

## Verified versus reported evidence — October 28, 2026

- **Direct Maven console evidence:** `mvn clean test` compiled Java using release 21, `protoc` returned exit 0 (Windows host), and Surefire reported **595 run, 0 failed, 0 errors, 0 skipped**, `BUILD SUCCESS`. `mvn dependency:tree` succeeded. `mvn -DskipTests package` succeeded and the Shade plugin replaced the original JAR. These are separate Windows Maven runs; they do not establish Linux-native Protobuf execution.
- **User-reported Docker execution:** successful Docker image build earlier; 3/3 containers healthy; smoke test passed; full deployment acceptance passed with `initialIndex=7`, `majorityIndex=8`, `durableLastIndex=8`. The full Docker logs and image IDs are not attached here.
- **Build provenance:** due to a temporary Docker Hub DNS outage, the final successful acceptance used **`--no-build`** (existing `raftkv:phase12` image). The earlier successful Docker build and later acceptance are distinct operations. **No rebuild occurred during the successful acceptance.**
- **Scope:** Executed Windows results predate this final packaging-only script normalization. The changes in this review do not modify Java production/test code. The exact final script bytes were not executed again in this environment.

## Execution checklist

### Java-focused tests (Windows Maven ran previously; final script update not rerun)
```powershell
cd raft-kv-store
mvn "-Dtest=DeploymentHealthProbeTest,DeploymentSmokeClientTest,Phase12ComposeConfigTest,ReplicationGrpcServiceTest" test
mvn clean test
mvn dependency:tree
```

### Linux builder + Docker operational suite (separate build reported; final acceptance used --no-build)
```powershell
cd raft-kv-store
docker compose config
docker compose build --no-cache
.\scripts\deployment-acceptance.ps1
docker compose ps --format json
```

**Evidence required:** actual Maven count and failures, Linux protoc exit code, shaded artifact execution, Docker health JSON, RPC response JSON, durable indexes after restarts/recreation, failed-quorum behavior, and explicit script exit codes.

**Known limitations:** earlier 595-test Windows Maven run and earlier `--no-build` deployment acceptance have their own evidence; **C05 and G01 are now separately evidenced and accepted**. The current assistant environment did not rerun Maven/Docker, and full 160-file clean-checkout packaging is not verified until the included reconstruction tool runs on a host with the baseline repository.

## C05 and G01 final Windows execution evidence (October 28, 2026)

- **C05 — PASS:** user-provided PowerShell transcript shows `docker compose build --no-cache` PASS, then `docker compose up --detach --no-build --force-recreate` PASS on retained volumes; source write index 9, post-recreation GET matches, retry index 9 with no new log entry, 3/3 healthy. Old image `sha256:afae0c12956bc1f55207eaad9c1f31b184c7928a8ace4257ed8b4acd697ebb38`; rebuilt and running image `sha256:35dd01867a91bcd10386ce88f36af8934729ae2861cb4b2fdbd3c1fdb95c58f1`. See `PHASE12_C05_WINDOWS_EXECUTION_TRANSCRIPT.txt`.
- **G01 — PASS:** user-pasted transcript shows corrected `.\scripts\cluster-up.ps1` produced Compose image-build success and 3/3 healthy services; startup exit code 0; current image `sha256:05f15d3e9789c4f28d5bf88ac3ca039fa103f4ed42274582f0ad8435d433b674`; `git diff --check` produced no output. See `PHASE12_G01_WINDOWS_EXECUTION_TRANSCRIPT.txt`.
- **Provenance:** C05 and G01 are **distinct later runs**; previous successful deployment acceptance used `--no-build` and must not be recast as an image-build test.
