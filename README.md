# hatrie_cache
Experimental distributed memcache using HAT-Trie (a data structure designed by Dr Nikolas Askitis)

Slice/stack/queue values are stored behind compact HAT-trie indexes with a ring
deque backing store, so push/pop/shift stay O(1) and removed elements do not
retain old object references. Priority queue values use a flat binary heap with
stable insertion ordering for equal priorities, keeping push/pop O(log n), peek
O(1), and memory usage low without per-item node allocations.
Bloom filter values use packed bitsets plus double hashing for fast,
low-memory membership checks without storing inserted items.
Cuckoo filter values use compact fixed-size fingerprint buckets for fast,
low-memory membership checks with approximate delete support.
XOR filter values stage unique items once, then compile them into static 8-bit
fingerprint arrays for faster low-memory membership checks than Bloom filters
on read-heavy immutable sets.
Roaring bitmap values use adaptive sorted-array and packed-bitset containers for
exact uint32 sets with fast membership, remove, count, and sorted iteration.
Sparse bitset values use sorted 16-bit containers keyed by the upper 48 bits,
promoting dense ranges to packed bitsets for exact uint64 membership with low
memory overhead on sparse high-cardinality IDs.
Radix tree values use path-compressed string edges for exact nested key/value
indexes with fast lookup, sorted prefix scans, and low overhead for keys that
share long prefixes.
Count-Min Sketch values use compact uint32 counter grids plus double hashing
for approximate frequency counts without storing observed items.
HyperLogLog values use compact register arrays for approximate distinct counts
without retaining the observed items.
Top-K values use a bounded Space-Saving min-heap to track heavy hitters with
fixed memory and O(log k) updates.
Reservoir sample values keep a deterministic fixed-capacity stream sample using
hashed priorities, so representative samples stay bounded in memory without
retaining the full event history.
Quantile sketch values use a compact Greenwald-Khanna summary for approximate
p50/p95/p99-style numeric queries with bounded rank error and low memory use.
Fenwick tree values use a compact int64 array for point updates, point reads,
prefix sums, and range sums in O(log n) time without storing individual events.
Typed backing pools reuse deleted indexes through a compact bitset-backed stack
and trim freed tail slots, avoiding per-index hash-map overhead while keeping
reuse checks, allocation, and delete-heavy memory release fast. TTL expiration
uses a min-heap schedule plus an authoritative key map, so vacuuming pops due
keys instead of scanning every TTL entry. The map stores a compact heap index;
deadline updates repair one existing node in O(log n), and `Persist`/delete
remove it immediately, so stale schedule entries do not accumulate under churn.
Native HAT-trie hash buckets retain one additional append-sized capacity class
instead of reallocating their contiguous slot on every insertion. Empty slots
are released immediately and nonempty slots shrink below one-third utilization,
keeping churn fast without unbounded high-water retention. The isolated and
full-cache measurements are in
[BENCHMARK.md](BENCHMARK.md#adaptive-native-bucket-size-classes).
Long-running delete-heavy workloads can call `CompactMemory` to rebuild the C
trie, densely reindex in-memory typed pools, pack live strings into 256 KiB
chunks, and shrink Merkle and metadata backing. Normal string writes remain
zero-copy; packing occurs only during explicit compaction. Disk-spill indexes
remain stable so file ownership cannot alias.
Periodic compaction is available but remains off by default because the rebuild
temporarily retains a second generation and adds bounded scan-page pauses.
Map, slice, set, and priority queue APIs deep-copy nested JSON-style map/slice
values at storage and read boundaries so callers cannot mutate cached state
through shared nested references.

## Development

Run the Go wrapper tests:

```
make test
```

Run just the Go race detector suite:

```
make verify-race
```

Run the backing-store and compact-structure benchmarks:

```
make bench
make bench BENCH=RoaringBitmap
```

Run the serialization, snapshot, and LevelDB storage tradeoff benchmarks:

```
make bench-serialization
make bench-serialization BENCHTIME=20x
make bench-serialization SERIALIZATION_BENCH='BenchmarkLevelDB(Save|Load).*Structured' BENCHTIME=20x
make bench-structured-storage-codec BENCHTIME=1000x COUNT=7
make bench-startup-persistence BENCHTIME=1x COUNT=7
make bench-live-replication BENCHTIME=1x COUNT=7
make bench-merkle-maintenance BENCHTIME=1x COUNT=7
make bench-online-compaction BENCHTIME=1x COUNT=7
make bench-native-ahtable-allocator NATIVE_AHTABLE_KEYS=100000 NATIVE_AHTABLE_SLOTS=4096 COUNT=7
```

Run the architectural baseline for concurrent reads, retained per-key memory,
durable writes, snapshot pauses, anti-entropy, and unary/stream command transport:

```
make bench-big-wins BIG_WINS_KEYS=100000 BIG_WINS_OPS=100000 BENCHTIME=1x
```

Run command-feature benchmarks for the Redis/Tarantool comparison matrix:

```
make bench-command-features BENCHTIME=100x
make bench-hatrie-transport-features HATRIE_TRANSPORT_BENCH='^BenchmarkCommandTransportFeature/(HTTPJSON|HTTPProtobuf|GRPC|GRPCStream)/(StringSet|StringGet)$' BENCHTIME=100x
make bench-redis-command-features REDIS_START_DOCKER=1 REDIS_PORT=6380 REDIS_REQUESTS=10000
make bench-tarantool-command-features TARANTOOL_REQUESTS=10000 TARANTOOL_KEYSPACE=10000
make bench-hatrie-command-features BENCHMARK_ARTIFACT_DIR=build/benchmarks BENCHTIME=100x
make bench-redis-command-features BENCHMARK_ARTIFACT_DIR=build/benchmarks REDIS_START_DOCKER=1 REDIS_PORT=6380 REDIS_REQUESTS=10000
make bench-tarantool-command-features BENCHMARK_ARTIFACT_DIR=build/benchmarks TARANTOOL_REQUESTS=10000 TARANTOOL_KEYSPACE=10000
make bench-command-comparison BENCHMARK_ARTIFACT_DIR=build/benchmarks
make benchmark-md BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

When `BENCHMARK_ARTIFACT_DIR` is set, the HAT-trie, Redis, and Tarantool
command benchmark scripts write raw Markdown plus TSV rows and memory summaries.
`make bench-command-comparison` joins those TSV files into
`command-feature-comparison.md` with seconds-per-10k and speedup columns.
`make benchmark-md` refreshes the generated comparison and raw-result regions
in `BENCHMARK.md` from those artifacts.
[`IMPROVEMENT_REPORT.md`](IMPROVEMENT_REPORT.md) consolidates the shipped
feature commits, final before/after metrics, and measured tradeoffs.
The one-table summary for all measured earlier and final architecture
improvements is in
[`BENCHMARK.md`](BENCHMARK.md#measured-improvement-summary).

Run the benchmark smoke locally:

```
make bench-smoke
make bench-smoke BENCH_SMOKE_CHECK_THRESHOLDS=1
make bench-smoke BENCH_SMOKE_CHECK_THRESHOLDS=1 BENCH_SMOKE_MAX_COMMAND_NS_OP=100000 BENCH_SMOKE_MAX_B_OP=262144
make bench-smoke BENCH_SMOKE_ARTIFACT_DIR=build/benchmarks
make bench-smoke BENCH_SMOKE_ARTIFACT_DIR=build/current BENCH_SMOKE_BASELINE_JSON=build/main/benchmark-smoke.json
```

`BENCH_SMOKE_CHECK_THRESHOLDS=1` turns the smoke run into an optional
regression guard. It checks representative command, transport, and
serialization benchmark rows against `BENCH_SMOKE_MAX_COMMAND_NS_OP`,
`BENCH_SMOKE_MAX_TRANSPORT_NS_OP`,
`BENCH_SMOKE_MAX_SERIALIZATION_NS_OP`, `BENCH_SMOKE_MAX_B_OP`, and
`BENCH_SMOKE_MAX_ALLOCS_OP`. Set any max to `0` to disable that specific
limit.
Set `BENCH_SMOKE_ARTIFACT_DIR` to write raw output plus
`benchmark-smoke.json`, `benchmark-smoke.md`, and timestamped history
copies. Set `BENCH_SMOKE_BASELINE_JSON` to compare the current artifact
against a baseline from `origin/master` or a checked-in/downloaded artifact;
`BENCH_SMOKE_MAX_REGRESSION_PCT` controls the allowed `ns/op` regression,
and `BENCH_SMOKE_COMPARE_MEMORY=1` also compares `B/op` and `allocs/op`.
Artifacts remain in the selected local directory; timestamped copies make it
possible to retain and compare runs without a hosted workflow.

The Svelte MPA management UI lives in `svelte-mpa/`. Install and run it with:

```
make frontend-install
make frontend-dev
make frontend-smoke
make frontend-backend-smoke
```

`make frontend-smoke` builds the Svelte MPA, serves the production bundle with
Vite preview on a loopback port, verifies dashboard/keys/commands/admin HTML
entrypoints, and renders the Admin page with Chrome/Chromium when one is
available. Set `FRONTEND_SMOKE_REQUIRE_BROWSER=true` to fail when a browser is
not installed.
`make frontend-backend-smoke` starts the real `hatrie-cache` monitoring server
with temporary LevelDB, audit logging, replication status, and the built Svelte
MPA. It verifies real `/api/health`, `/api/storage`, `/api/storage/flush`,
`/api/replication`, `/api/audit`, and rendered `/admin.html` output.

The Admin page at `/admin.html` exposes LevelDB flush/compact controls and
replication queue/sync status.

Run the full local verification suite with `make verify` or the explicit alias
`make verify-local`. It checks deploy configuration, Go tests/race/coverage, C
tests, the Svelte MPA, operations smoke tests, and benchmark-document freshness.
Set `VERIFY_LOCAL_DOCKER_COMPOSE=1` to include Docker Compose config validation;
run `make docker-build` separately when an image build is required.
The C verifier automatically runs AddressSanitizer/UBSan leak and undefined
behavior checks when the local compiler supports them; use
`make verify-c SANITIZE_C=0` to skip that pass or `SANITIZE_C=1` to require it.
On hosts with `vm.overcommit_memory=2`, auto mode skips that sanitizer pass
because AddressSanitizer can reserve a large shadow-memory range that strict
commit accounting rejects. On this toolchain that reservation is expected around
`15392894357504` bytes, which can appear in repeated kernel
`__vm_enough_memory` messages. In those cases, auto mode still runs a
LeakSanitizer-only fallback when the compiler/runtime supports it, so leak
coverage remains available without the full AddressSanitizer reservation. Auto
mode also skips the AddressSanitizer/UBSan pass when `/proc/meminfo` shows
available commit headroom below AddressSanitizer's expected reservation. To
force that pass anyway, use
`make verify-c SANITIZE_C=1 SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=1` and, when
needed, `SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM=1`.

Run one-off commands through the Makefile/script wrapper:

```
make run CMD='go env GOMOD'
```

## Operations Manual

This section is the operator runbook for installing, running, backing up,
recovering, and joining nodes to a cluster. The examples use the Makefile
wrappers so the same flags and defaults are used in development and local
operations.

No hosted GitHub Actions workflow is checked in. Before pushing, run
`make verify-local`; use `make bench-smoke BENCH_SMOKE_CHECK_THRESHOLDS=1` for
the optional performance guard and `make docker-build` for the production image.

### Install And Build

Runtime requirements:

- Go 1.20 or newer.
- `make`, a POSIX shell, and a C toolchain for the bundled HAT-trie C code.
- Node.js plus `pnpm` or `bun` only when building the Svelte MPA web UI.

Build and verify the service:

```
make verify
make verify-ops
make run CMD='mkdir -p build'
make run CMD='go build -o build/hatrie-cache ./cmd/hatrie-cache'
make run CMD='go build -o build/hatrie-cli ./cmd/hatrie-cli'
```

`make verify-ops` runs executable smoke tests for snapshot+journal restore and
journal-pull catch-up. Keep it green when changing persistence, recovery,
replication, or CLI behavior.

Install the binaries wherever your service manager expects them. For a
frontend-enabled deployment, build the static web assets and point
`MONITORING_WEB_DIR` at the generated directory:

```
make frontend-install
make frontend-build
make monitoring-server MONITORING_WEB_DIR=svelte-mpa/dist
```

For API-only deployments, the web assets are optional. The HTTP monitoring/API
server is opt-in; `go run ./cmd/hatrie-cache` by itself does not bind HTTP or
gRPC unless `-monitoring-server` or `GRPC_ADDR` is configured through the
wrapper.

### Run As A Service

Use `make monitoring-server` while developing and run the compiled binary under
systemd, supervisord, Docker, or another process supervisor in production. A
durable single-node service normally enables a command journal plus either
snapshots or LevelDB persistence:

```
make monitoring-server \
  MONITORING_ADDR=0.0.0.0:8080 \
  SNAPSHOT_PATH=data/snapshot.hc \
  SNAPSHOT_INTERVAL=30s \
  JOURNAL_PATH=data/commands.journal
```

Example systemd unit for a compiled binary:

```
[Unit]
Description=HAT-trie cache
After=network-online.target
Wants=network-online.target

[Service]
User=hatrie-cache
Group=hatrie-cache
WorkingDirectory=/var/lib/hatrie-cache
ExecStart=/usr/local/bin/hatrie-cache \
  -monitoring-server \
  -monitoring-addr 0.0.0.0:8080 \
  -monitoring-web-dir /usr/share/hatrie-cache/svelte-mpa/dist \
  -snapshot-path /var/lib/hatrie-cache/snapshot.hc \
  -snapshot-interval 30s \
  -journal-path /var/lib/hatrie-cache/commands.journal
Restart=on-failure
RestartSec=2s
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

Bind to localhost or a private network unless the API is protected by another
network layer. For direct TLS/HTTP2, set `MONITORING_TLS_CERT` and
`MONITORING_TLS_KEY`; for bearer-token API protection across HTTP and native
gRPC APIs, set `MONITORING_AUTH_TOKEN` on the server and pass `-token` to
`make cli` for HTTP CLI calls; for native protobuf clients, set `GRPC_ADDR` and
send the same token as gRPC metadata.

### Persistence Model

The server restores data in this order:

1. Load the configured persistent store when `DB_PATH` is set.
2. Load the snapshot when `SNAPSHOT_PATH` is set, replacing the current
   in-memory key set.
3. Replay journal entries newer than the snapshot checkpoint when
   `JOURNAL_PATH` is set.

That means `SNAPSHOT_PATH + JOURNAL_PATH` is the usual point-in-time recovery
pair, while `DB_PATH` is a full key/value persistence store. You can enable
both, but keep in mind the snapshot is loaded after the persistent store and
therefore wins for overlapping keys.

Recommended durability profiles:

| Profile | Configuration | Use case |
| --- | --- | --- |
| Fast local persistence | `DB_PATH=data/cache.leveldb DB_BACKEND=auto DB_FORMAT=binary DB_SYNC_INTERVAL=30s DB_COMPACT_INTERVAL=10m DB_HOT_LOAD=true` | Pebble for a new path, legacy LevelDB auto-detection, periodic sync, compaction, and memory-efficient cold starts. |
| Point-in-time recovery | `SNAPSHOT_PATH=data/snapshot.hc SNAPSHOT_INTERVAL=30s JOURNAL_PATH=data/commands.journal` | Compact snapshots plus replayable mutations after the latest snapshot. |
| Replica catch-up | `JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080` | Pull another node's journal tail during bootstrap or after downtime. |

### Backup Runbook

For snapshot+journal deployments:

1. Trigger an online snapshot.
2. Copy the data directory to a fresh backup directory.
3. Keep file ownership and permissions when copying.

The active journal and `<JOURNAL_PATH>.segments/` are one WAL set. `make backup`
copies the containing data directory, so keep both below `DATA_DIR`. For a raw
copy outside the supplied workflow, stop the process or use one filesystem
snapshot so rotation cannot move a segment between the two copies. Server-side
atomic backup bundles do not need archived segments: they contain either a
point-in-time snapshot or a native Pebble checkpoint plus a journal checkpoint
at the same sequence.

```
make cli ARGS='snapshot'
make backup DATA_DIR=data BACKUP_DIR=backup/run-001
```

For LevelDB deployments, prefer stopping the process or using filesystem-level
snapshots before copying the data directory:

```
make backup DATA_DIR=data BACKUP_DIR=backup/run-001
```

Use a fresh `BACKUP_DIR` by default. Set `BACKUP_OVERWRITE=true` only when you
intentionally want to copy into an existing backup directory.

The safest operational pattern is to store cache data under one directory such
as `/var/lib/hatrie-cache`, keep snapshots/journals/LevelDB on the same durable
volume, and back up that directory with your normal host snapshot tooling.

Deployable templates are available in `deploy/`: systemd at
`deploy/systemd/hatrie-cache.service`, topology examples at
`deploy/topology/full-replica.json` and `deploy/topology/sharded.json`, and a
two-node local compose example at `deploy/docker-compose.yml`. A hardened
single-node container template is available at
`deploy/docker-compose.production.yml`.

Build the production container image with the same Makefile+script convention:

```
make docker-build DOCKER_IMAGE=hatrie-cache:latest
```

The `Dockerfile` builds the Svelte MPA assets, compiles `hatrie-cache` and
`hatrie-cli` in a CGO-enabled builder stage, and runs the daemon as a non-root
user on Debian slim. The default container command serves the monitoring UI/API
on `0.0.0.0:8080` with LevelDB, snapshot, and journal files under
`/var/lib/hatrie-cache`; pass your own args to enable gRPC with
`-grpc-addr 0.0.0.0:9090` or to set `-monitoring-auth-token`. The image also
includes a Docker healthcheck that runs `hatrie-cli health` against
`HATRIE_HEALTHCHECK_ADDR` (default `http://127.0.0.1:8080`) and forwards
`MONITORING_AUTH_TOKEN` to the probe when authentication is enabled.
The production compose template expects `MONITORING_AUTH_TOKEN` and can be
rendered with:

```
MONITORING_AUTH_TOKEN=change-me docker compose -f deploy/docker-compose.production.yml config
```

### Restore And Recovery Runbook

Restore snapshot+journal data to a clean data directory, then start the node
with the same paths:

```
make restore DATA_DIR=data BACKUP_DIR=backup/run-001
make monitoring-server SNAPSHOT_PATH=data/snapshot.hc JOURNAL_PATH=data/commands.journal
```

On startup, the server loads the snapshot and replays journal entries after the
snapshot checkpoint. If the journal contains records older than the compacted
checkpoint, they are skipped; if a follower asks for a journal range older than
the checkpoint, `/api/journal` returns `409` and the follower must fall back to a
snapshot or an explicit replication sync.

Restore LevelDB data by restoring the directory and starting with `DB_PATH`:

```
make restore DATA_DIR=data BACKUP_DIR=backup/run-001
make monitoring-server DB_PATH=data/cache.leveldb
```

`make restore` refuses to copy into a non-empty `DATA_DIR` unless
`RESTORE_OVERWRITE=true` is set.

Verify a backup directory or atomic bundle before restore:

```
make doctor DOCTOR_PATH=backup/run-001
make cli ARGS='doctor -path backup/run-001.tar.gz'
```

Rehearse a restore into an isolated work directory before touching the real
`DATA_DIR`. The rehearsal verifies the source, restores or copies it into a
temporary `data` directory, verifies the restored result, starts a temporary
loopback monitoring server from that restored data, and checks health, stats,
and GET command handling:

```
make restore-rehearsal RESTORE_REHEARSAL_PATH=backup/run-001
make restore-rehearsal RESTORE_REHEARSAL_PATH=backup/run-001.tar.gz
make restore-rehearsal RESTORE_REHEARSAL_PATH=backup/run-001.tar.gz RESTORE_REHEARSAL_KEEP_WORK_DIR=true
make restore-rehearsal RESTORE_REHEARSAL_PATH=backup/run-001 RESTORE_REHEARSAL_RUNTIME_GET='name=ivi session:1'
```

Set `RESTORE_REHEARSAL_RUNTIME_CHECK=false` only when you want the older
file-level rehearsal without starting a temporary server. Set
`RESTORE_REHEARSAL_RUNTIME_SERVER_BIN=/path/to/hatrie-cache` to reuse an
existing binary instead of building a temporary one from the current checkout.

Restore an atomic backup bundle after verification:

```
make restore-bundle RESTORE_BUNDLE_PATH=backup/run-001.tar.gz DATA_DIR=data
make restore-bundle RESTORE_BUNDLE_PATH=backup/run-001.tar.gz DATA_DIR=data RESTORE_BUNDLE_OVERWRITE=true
```

Bundle and incremental-repository restore use a sibling staging directory.
Payload files are streamed and checksum-verified once, semantically loaded from
staging, fsynced, and only then published to `DATA_DIR`. With overwrite enabled,
the old directory is retained under a rollback name until the new directory and
its parent entry are synced. A failed extraction or semantic check leaves the
old directory untouched. Restore rejects a symlink destination, symlinked parent
components, and source/destination overlap. The measured checkpoint restore is
1.24x faster with half the payload passes and 1.03x less timed heap; small local
repository restore is 1.09x slower because durability now includes staged-file
fsync. See [BENCHMARK.md](BENCHMARK.md#single-pass-staged-restore).

For a server-side atomic backup bundle, ask the monitoring API to write a
tar.gz bundle. The sane `auto` default is `snapshot`, which contains
`manifest.json`, `snapshot.hc`, and a compacted `commands.journal` checkpoint.
The manifest records the selected mode, file sizes, SHA-256 checksums, storage
or snapshot format, journal sequence, and optional partition metadata:

```
make cli ARGS='backup -path backup/run-001.tar.gz'
make cli ARGS='backup -path backup/run-001.tar.gz -snapshot-format gzip-binary'
make cli ARGS='backup -path backup/run-001.tar.gz -mode snapshot'
make cli ARGS='backup -path backup/sg.tar.gz -partitions sg -partition-mode partitioned -partition-node node-sg-a -partition-epoch 42 -partition-fingerprint topology-v1 -partition-prefixes sg:'
```

When the server is running with a Pebble `DB_PATH`, explicitly request a
file-level checkpoint bundle when lower restore heap and a ready-to-open native
store matter more than elapsed time and transfer size:

```
make cli ARGS='backup -path backup/run-001.tar.gz -mode pebble-checkpoint'
make cli ARGS='doctor -path backup/run-001.tar.gz'
make restore-bundle RESTORE_BUNDLE_PATH=backup/run-001.tar.gz DATA_DIR=data
make monitoring-server DB_PATH=data/cache.leveldb DB_BACKEND=auto
```

Checkpoint creation first saves a complete generation, compacts obsolete
generations, and then uses Pebble's file checkpoint API. Bundle verification
and restore stream files to disk while checking declared sizes and SHA-256
digests, so neither mode reads the complete archive into Go memory. Existing
snapshot bundles remain readable. Snapshot stays the default because the local
10,000-key benchmark is 1.34x faster to create, 1.37x faster to restore, and
1.06x smaller; checkpoint restore uses 1.60x less timed heap and 1.62x fewer
allocations. See [BENCHMARK.md](BENCHMARK.md#pebble-checkpoint-backup-bundles).

For recurring Pebble backups, `pebble-incremental` writes a content-addressed
repository directory. The first run creates a full base; subsequent runs on the
same store generation persist dirty keys and reuse unchanged checkpoint files.
Retention defaults to 32 manifests and can be changed per backup with
`-retain`. This is explicit opt-in: `auto` remains `snapshot`.

```sh
make cli ARGS='backup -path backup/pebble-repository -mode pebble-incremental'
make cli ARGS='backup -path backup/pebble-repository -mode pebble-incremental -retain 14'
make cli ARGS='doctor -path backup/pebble-repository'
make restore-bundle RESTORE_BUNDLE_PATH=backup/pebble-repository DATA_DIR=data
make monitoring-server DB_PATH=data/cache.leveldb DB_BACKEND=auto
```

Keep one writer per repository and let each backup command finish before an
external copy or object-store sync. Copy `repository.json`, `latest`,
`manifests/`, and `objects/` together. A changed Pebble generation or source
store identity automatically starts a new full base; manifests and objects are
verified by content hash before restore. On the measured 10,000-key, 1%-changed
fixture, an incremental backup is 6.73x faster, uses 10.49x less timed heap and
23.89x fewer allocations, and writes 60.09x fewer transferable bytes than a
fresh checkpoint bundle. The first backup has full-backup cost and retained
history consumes disk. See
[BENCHMARK.md](BENCHMARK.md#content-addressed-incremental-backups).

Partition metadata is intentionally descriptive today. It records which
operator-defined partition ids, key prefixes, node id, topology epoch, and
topology fingerprint a backup covered, and `doctor`/`restore-bundle` echo it
back in their JSON reports. When `key_prefixes` are present, `doctor` verifies
that recovered snapshot keys and bundled journal write keys are covered and
reports `partition_validation` with checked key counts, `checked_journal_keys`,
invalid key samples, and `invalid_journal_key_samples`; `restore-bundle` refuses
bundles that fail that check. It does not enable automatic partition routing or
partial restore by itself.

For crash recovery, restart with the same persistence flags first. If a node was
offline while a leader continued accepting writes, also pull journal catch-up or
run anti-entropy replication sync after the node is reachable:

```
make monitoring-server \
  JOURNAL_PATH=data/commands.journal \
  JOURNAL_PULL_SOURCE=http://leader:8080 \
  JOURNAL_PULL_INTERVAL=5s

make cli ARGS='replication -sync'
make cli ARGS='replication -sync -prefix session:'
```

### Joining A Cluster

Use the CLI join workflow to add a running node to a peer's topology, upload the
same topology to the joining node, and pull the peer journal into the joining
node:

```
make cli ARGS='cluster join -peer http://node-a:8080 -node node-c -address http://node-c:8080'
make cli ARGS='cluster join -peer http://node-a:8080 -node node-c -address http://node-c:8080 -pull-journal=false'
```

Joining a cluster means adding the node to the topology, starting it with a
stable `NODE_ID`, catching it up from an existing node, and then allowing
replication and leader routing to use it.

1. Add the new node to the topology JSON. For sharded mode, add it as a replica
   first; move primaries or bucket ranges only after it has caught up.
2. Copy the same topology file to every node or update it through
   `/api/topology`.
3. Start the new node with its own data directory, `NODE_ID`, and
   `TOPOLOGY_PATH`.
4. Catch up from an existing leader with journal pull or an explicit
   replication sync.
5. Enable `REPLICATION=true` on leaders and, when ready to enforce routing,
   enable `ENFORCE_LEADER_WRITES=true`.

Example new replica startup:

```
make monitoring-server \
  NODE_ID=node-c \
  TOPOLOGY_PATH=data/topology.json \
  JOURNAL_PATH=data/commands.journal \
  JOURNAL_PULL_SOURCE=http://node-a:8080 \
  JOURNAL_PULL_INTERVAL=5s \
  REPLICATION=true
```

After catch-up, check routing, election, and replication status:

```
make cluster-status CLUSTER_PEER=http://node-a:8080
make cli ARGS='cluster status -peer http://node-a:8080'
make cli ARGS='cluster doctor -peer http://node-a:8080 -probe-nodes=false'
make cli ARGS='topology'
make cli ARGS='election'
make cli ARGS='replication'
make cli ARGS='entries -limit 10'
```

`cluster status` and `cluster doctor` fetch the peer health, topology,
election, and replication state. With node probes enabled, they also call each
topology node's health, topology, and election endpoints and report topology
drift when a node's normalized topology differs from the peer view. They also
compare elected shard leaders across probed nodes and report election drift
when a peer would route writes to a different leader.

For sharded clusters, only enable `ENFORCE_LEADER_WRITES=true` after clients can
write to the elected leader for each key or after the client/proxy layer handles
leader errors. Internal replication commands are still accepted on followers.

### Rolling Restart Checklist

1. Confirm persistence is configured with `SNAPSHOT_PATH + JOURNAL_PATH` or
   `DB_PATH`.
2. Trigger a snapshot when snapshots are configured.
3. Stop one node at a time.
4. Start it with the same `NODE_ID`, `TOPOLOGY_PATH`, and persistence paths.
5. Confirm `/api/health`, `/api/election`, `/api/replication`, and
   `/api/stats`.
6. Wait for journal pull or replication sync to finish before restarting the
   next node.

Regenerate native gRPC/protobuf stubs after editing
`proto/hatriecache/v1/cache.proto`:

```
make generate-proto
```

The monitoring web/API server is opt-in. It does not run unless the
`-monitoring-server` flag is set. Monitoring HTTP uses a 5 second request
header timeout and 2 minute idle keep-alive timeout by default; set
`MONITORING_READ_HEADER_TIMEOUT=0` or `MONITORING_IDLE_TIMEOUT=0` to disable a
bound deliberately:

```
make monitoring-server MONITORING_ADDR=127.0.0.1:8080
make monitoring-server MONITORING_READ_HEADER_TIMEOUT=10s MONITORING_IDLE_TIMEOUT=30s
make run CMD='go run ./cmd/hatrie-cache'
```

Set `AUDIT_LOG_PATH` to append JSONL audit events for dangerous HTTP/gRPC API
actions such as mutating commands, snapshots, backups, topology/election
updates, replication syncs, and journal pulls. Audit records include command
names and keys but intentionally omit command values:

```
make monitoring-server AUDIT_LOG_PATH=data/audit.jsonl
```

The Svelte MPA Admin page requires explicit confirmation before running flush,
compact, or replication sync operations. It also shows the bounded in-memory
recent audit view from `GET /api/audit?limit=25`; the full durable trail stays
in the JSONL file configured by `AUDIT_LOG_PATH`.

Set `WRITE_PROTECTION=true` to reject dangerous write/admin actions while still
allowing read-only health, stats, entry listing, metrics, and status endpoints.
Set `RATE_LIMIT=N` to token-bucket limit dangerous HTTP/gRPC API actions to
`N` requests per caller per second, with a burst capacity of `N`; `0` disables
rate limiting. Per-caller limiter state is sharded across independent locks,
bounded, and oldest caller records are evicted under high cardinality:

```
make monitoring-server WRITE_PROTECTION=true
make monitoring-server RATE_LIMIT=50 AUDIT_LOG_PATH=data/audit.jsonl
```

Per-key monitoring metadata is off by default, so high-cardinality workloads
retain only exact cache-wide counters and `StatsForKey` reports `false`. This
default path updates cache-wide counters and monotonic timestamps atomically,
without taking the per-key telemetry mutex. It adds 64 fixed bytes per cache
and no per-operation allocation; the measured 32-reader path is 2.38x faster.
See [BENCHMARK.md](BENCHMARK.md#atomic-cache-wide-telemetry). This does not
limit or evict cached values. Use `bounded` to retain details for up to
100,000 active keys by default; its tracker samples five candidates and replaces
the least recently active candidate in constant time. Use `full` only when
unlimited per-key statistics are explicitly required:

```
make monitoring-server KEY_STATS_MODE=bounded
make monitoring-server KEY_STATS_MODE=bounded KEY_STATS_CAPACITY=250000
make monitoring-server KEY_STATS_MODE=full
```

Independent in-process HAT-trie partitions are off by default. Enable a power
of two from 2 through 256 when concurrent writes are contending on the single
trie lock; 16 is the measured starting point for a 16-writer workload:

```sh
make monitoring-server LOCAL_PARTITIONS=16
# Equivalent direct daemon flag: -local-partitions 16
```

Keys are assigned deterministically with XXH64 and all command families, direct
Go value APIs, TTLs, scans, snapshots, Pebble/LevelDB persistence, cold-value
spilling, replication inventory, compaction, and monitoring remain available
through the parent cache. Cross-partition `BATCH` groups independent keys and
runs the groups concurrently while preserving response order. Snapshot and
persistence capture use a sorted k-way cursor merge and one logical backup
image. This is an in-process lock-partitioning optimization, not topology
sharding or operator-owned regional partitioning; it does not change ownership,
failover, or backup boundaries.

The option must be configured before data is loaded and cannot be changed on a
nonempty cache. It adds one C trie and typed backing set per partition, and
whole-keyspace operations still visit every partition. When enabled, key and
entry scans collect partitions concurrently up to `GOMAXPROCS`, sort each child
locally, and perform a deterministic k-way merge. Monitoring inventory,
replication scans, Merkle rebuilds, expiration cleanup, and memory compaction use
the same independent partition parallelism. Snapshot and persistence capture
install one shared mutation tracker, scan generation-checked 256-entry child
pages with work limited to `GOMAXPROCS`, and reconcile dirty keys at one short
journal barrier. They retain one point-in-time image without holding every child
lock for the complete scan.

Multi-page replication retains one generation-checked iterator per partition
and a k-way merge heap across pages. It no longer rescans and globally sorts the
complete keyspace for every 1,000-key page; a mutation in any child invalidates
the merged cursor and safely restarts after the caller's last key. The
100,000-key, 100-page fixture is 18.97x faster, uses 142.52x less cumulative
heap, and performs 33.51x fewer allocations. See
[BENCHMARK.md](BENCHMARK.md#persistent-partition-replication-cursors).

Internal digest, Merkle, snapshot-stream, and compact-replication scans borrow
keys from reusable native-batch arenas and use a typed partition heap. Public
and persistence scans instead retain one immutable arena per batch so returned
keys remain valid. On the same 100,000-key traversal, the internal path is
1.14x faster than the prior persistent cursor, uses 27.41x less cumulative
heap, and performs 449.23x fewer allocations. The durable batch-arena path was
1.03x faster than the borrowed path in this run but used 7.85x more heap. See
[BENCHMARK.md](BENCHMARK.md#packed-internal-scan-arenas).

For 100,000 keys across 16 partitions, bounded snapshot pages reduce median
maximum read pause from 154.40 ms to 2.30 ms (67.14x) while total snapshot time
rises 7.8%, cumulative heap rises 1.7%, and allocation count is effectively
unchanged. Snapshot and storage formats do not change. See
[BENCHMARK.md](BENCHMARK.md#bounded-partition-snapshot-locking).

Portable snapshot restore now decodes once into an isolated data generation,
validates that generation completely, and atomically swaps it into the existing
trie. On 100,000 256-byte values, the default single-trie restore is 1.64x
faster, uses 2.00x less cumulative heap, and performs 1.80x fewer allocations;
the 16-partition restore is 1.46x faster with 1.80x less heap and 1.80x fewer
allocations. Malformed or failed restores leave the live generation unchanged.
See [BENCHMARK.md](BENCHMARK.md#atomic-generation-snapshot-restore).

Partitioned restore and Pebble startup also use partition-stable workers bounded
by `GOMAXPROCS`. The historical 100,000-record comparison measured Pebble
startup at 1.18x faster. See
[BENCHMARK.md](BENCHMARK.md#parallel-partition-restore).

The measured 100,000-write fixture is 2.24x faster at 16 workers, while
separate-process maximum RSS rose from 51,588 KiB to 54,096 KiB. On a 100,000-key
whole-keyspace fixture, parallel sorted-key collection is 4.24x faster with
1.51x lower cumulative heap, and sorted-entry collection is 5.33x faster with
1.57x lower cumulative heap. Allocation counts are effectively flat and wire
and storage formats do not change. See
[BENCHMARK.md](BENCHMARK.md#local-hat-trie-partitions) and
[BENCHMARK.md](BENCHMARK.md#partition-parallel-whole-keyspace).

Existing non-TTL counters can opt into striped concurrent updates. The default
is `0` (off). Use a power of two from 2 through 256; 64 is the measured general
starting point for independent counter keys:

```
make monitoring-server COUNTER_WRITE_STRIPES=64
```

This is not keyspace sharding. With `LOCAL_PARTITIONS=0`, the cache has one
HAT-trie; with local partitions enabled, it still exposes one persistence image
and unchanged scan/backup semantics. The fast path applies to `SETINT` and
`INC` only after a counter key exists. It automatically uses the exclusive path
for missing or non-counter keys, TTL counters, enabled per-key telemetry, active
snapshot capture or Merkle tracking, and LevelDB spill accounting. The measured
CPU and fixed-memory tradeoffs are in [BENCHMARK.md](BENCHMARK.md#striped-existing-counter-writes).

Delete-heavy processes can reclaim trie and typed-pool high-water capacity with
the opt-in memory compactor. `MEMORY_COMPACTION_INTERVAL=0` is the default and
disables it; a positive Go duration runs compaction only after the trie changed
since the previous pass:

```
make monitoring-server MEMORY_COMPACTION_INTERVAL=15m
```

`CompactMemory` provides the same operation directly to Go callers and returns
before/after backing estimates. The operation preserves values, TTLs, global
and per-key statistics, lazy LevelDB references, and Merkle state. It scans the
live trie in bounded 256-key pages, builds a dense generation off-lock, replays
coalesced concurrent mutations, and atomically swaps generations. The measured
100k insert/90k delete fixture reduced the maximum reader pause 9.40x, retained
backing 13.17x, and retained heap 5.36x. Total compaction was 1.54x slower and
used 6.80x more transient heap than the previous exclusive rebuild, so periodic
compaction remains off by default. Reproduce the result and retain its raw output
with `make bench-online-compaction BENCHTIME=1x COUNT=7`; see
[BENCHMARK.md](BENCHMARK.md#delete-churn-memory-compaction). The separate 100k
live-string fixture cuts retained heap objects 800x and retained heap 3.79%; see
[packed string compaction](BENCHMARK.md#packed-string-compaction-arena).

Long-running daemon options can also live in a JSON config file. Config keys
match flag names and may use hyphens or underscores; duration values use Go
duration strings. Explicit CLI flags override file values:

```json
{
  "monitoring_server": true,
  "monitoring_addr": "0.0.0.0:8080",
  "monitoring_web_dir": "svelte-mpa/dist",
  "key_stats_mode": "off",
  "key_stats_capacity": 0,
  "local_partitions": 0,
  "counter_write_stripes": 0,
  "memory_compaction_interval": "0",
  "db_path": "data/cache.leveldb",
  "db_backend": "auto",
  "snapshot_path": "data/snapshot.hc",
  "snapshot_interval": "30s",
  "journal_path": "data/commands.journal"
}
```

Generate a redacted sane default config without starting listeners:

```
make print-sane-config
make print-sane-config CONFIG_PROFILE=dev
make print-sane-config CONFIG_PROFILE=bench
```

Profiles are explicit opt-ins. `production` enables the monitoring server on
`0.0.0.0:8080`, persistent storage, snapshot, and journal paths under `data/`, efficient
binary/gzip formats, periodic dirty saves, and periodic snapshots. `dev` keeps
loopback monitoring defaults. `bench` enables loopback monitoring and leaves
persistence disabled. Override any profile default with normal flags, for
example `make print-sane-config PRINT_CONFIG_ARGS='-monitoring-server=false'`.

Run a config-file based server through the script wrapper:

```
make server CONFIG_PATH=deploy/hatrie-cache.json
make server CONFIG_PATH=deploy/hatrie-cache.json SERVER_ARGS='-monitoring-addr 127.0.0.1:8081'
```

Validate a config file without opening listeners or persistence handles:

```
make check-config CONFIG_PATH=deploy/hatrie-cache.json
make check-config CHECK_CONFIG_ARGS='-grpc-tls-cert server.pem -grpc-tls-key server-key.pem -grpc-client-ca clients-ca.pem'
make check-config CONFIG_PATH=deploy/hatrie-cache.json CHECK_CONFIG_ARGS='-print-config'
```

The validator parses the same flags/config file as the daemon and checks
cross-field constraints, referenced TLS key pairs, gRPC client CA PEM files,
and topology JSON when `TOPOLOGY_PATH`/`-topology-path` is set. Use
`-print-config` through `make server SERVER_ARGS='-print-config'` or
`make check-config CHECK_CONFIG_ARGS='-print-config'` to print the effective
configuration as JSON with `monitoring_auth_token` redacted. Running servers
also expose the same redacted view at `GET /api/config`.

Provide a TLS certificate and key to serve the same monitoring API over HTTPS
with HTTP/2 ALPN enabled:

```
make monitoring-server MONITORING_ADDR=127.0.0.1:8443 MONITORING_TLS_CERT=cert.pem MONITORING_TLS_KEY=key.pem
make monitoring-server MONITORING_AUTH_TOKEN='change-me'
make cli ARGS='-token change-me health'
```

Set `GRPC_ADDR` to expose the native protobuf API from
`proto/hatriecache/v1/cache.proto`:

```
make monitoring-server GRPC_ADDR=127.0.0.1:9090
```

Use `MONITORING_AUTH_TOKEN` with `GRPC_ADDR` when the native protobuf API is
reachable outside a trusted localhost-only environment. For native gRPC TLS,
set `GRPC_TLS_CERT` and `GRPC_TLS_KEY`; add `GRPC_CLIENT_CA` to require mTLS
client certificates signed by that CA:

```
make monitoring-server GRPC_ADDR=0.0.0.0:9090 GRPC_TLS_CERT=server.pem GRPC_TLS_KEY=server-key.pem
make monitoring-server GRPC_ADDR=0.0.0.0:9090 GRPC_TLS_CERT=server.pem GRPC_TLS_KEY=server-key.pem GRPC_CLIENT_CA=clients-ca.pem
```

The matching JSON config keys are `grpc_tls_cert`, `grpc_tls_key`, and
`grpc_client_ca`.

Set `DB_PATH` to load and save cache data through the persistent storage
backend. `DB_BACKEND=auto` is the default: a new empty path uses Pebble, a
backend marker at `<DB_PATH>.backend` preserves the selected engine, and an
existing non-empty path without a marker is treated as legacy LevelDB. Use
`DB_BACKEND=leveldb` for the previous engine or `DB_BACKEND=pebble` to require
Pebble explicitly. A marker mismatch is rejected before opening the database;
do not switch an existing directory in place. Migrate through a snapshot or
backup/restore workflow and keep the adjacent marker in filesystem backups.

Both backends use the binary storage format by default
(`DB_FORMAT=binary`), which avoids JSON object-field overhead and stores byte
values as raw bytes instead of base64. Map, slice, set, priority queue, Top-K,
radix tree, and reservoir sample payloads always use the versioned tagged
binary value codec. Signed and unsigned integers use varints, byte slices use
raw length-prefixed bytes, and JSON-marshalable concrete values are normalized
into the same recursive binary tree instead of being embedded as inner JSON.
Bloom filter, Count-Min Sketch, and HyperLogLog values use compact binary
payload codecs. Cuckoo filter values store packed fingerprints directly; both
built and staged XOR filters have binary representations. Roaring bitmap
and sparse-bitset containers store their array or bitset payloads directly too.
Fenwick tree snapshots store their numeric tree vector as binary varints.
Quantile sketches store summary samples as binary float/varint tuples, and
reservoir samples store retained stream items as binary priority/sequence/value
tuples. Version-1 binary values and existing inner-JSON records still load
automatically; new binary writes use value codec version 2.
Leaving format variables unset in the Makefile wrapper uses the compiled Go
defaults; set them only when you want to override the default format.
Set `DB_FORMAT=json` to keep writing the previous JSON record layout.
Pebble full saves write a complete generation as an external SST, ingest it,
and then atomically switch a synced active-generation marker. A crash before
that marker leaves the previous generation active; a crash after it exposes
the complete new generation. Startup removes unreferenced generations. Capture
is page-bounded and serializes outside the trie mutex; under sustained writes it
falls back to a bounded disk spool so save memory does not grow with the cache.
LevelDB keeps its existing record-by-record replacement format.
`DB_SYNC_INTERVAL` uses an atomic applied-journal sequence to skip the former
full rewrite when the loaded database is already current, then periodically
syncs only dirty keys changed by direct APIs, HTTP commands, gRPC commands,
journal replay, TTL expiration, and local partitions. Legacy databases without
the sequence metadata and authoritative snapshot replacement perform one exact
full migration save. See
[BENCHMARK.md](BENCHMARK.md#delta-only-startup-persistence):

```
make monitoring-server DB_PATH=data/cache.leveldb DB_SYNC_INTERVAL=30s
make monitoring-server DB_PATH=data/cache.leveldb DB_BACKEND=leveldb
make monitoring-server DB_PATH=data/cache.leveldb DB_FORMAT=json
make monitoring-server DB_PATH=data/cache.leveldb DB_COMPARE_BEFORE_WRITE=never
```

Dirty-key saves use `DB_COMPARE_BEFORE_WRITE=auto` by default. `auto` compares
small dirty batches against existing records to skip unchanged writes,
but skips that read-before-write check for large dirty batches where the random
read I/O is usually worse than rewriting changed records. Use `always` when
storage write amplification matters more than read CPU/I/O, and
`DB_COMPARE_BEFORE_WRITE=never` for bulk ingest or replay when you prefer lower
CPU and fewer storage reads.

Run a manual persistent-store flush before planned maintenance when
`DB_SYNC_INTERVAL=0` or when you want a full current in-memory state write
immediately. Run manual
compaction after large delete or rewrite batches to reclaim storage-file space
and reduce read amplification. Set `DB_COMPACT_INTERVAL` to compact the selected
backend automatically on a schedule; optional `DB_COMPACT_START_KEY` and
`DB_COMPACT_LIMIT_KEY` bound the periodic compaction to one key range:

```
make monitoring-server DB_PATH=data/cache.leveldb DB_SYNC_INTERVAL=30s DB_COMPACT_INTERVAL=10m
make monitoring-server DB_PATH=data/cache.leveldb DB_COMPACT_INTERVAL=10m DB_COMPACT_START_KEY=session: DB_COMPACT_LIMIT_KEY=session~
```

`GET /api/storage` reports whether persistent storage is configured, the
selected backend in `store`, the store `path`, storage `format`, approximate
`size_bytes`, selected engine `properties`, current `operation`,
and the `last_flush`/`last_compact` result remembered by the monitoring
handler. Empty bounds compact all cache-entry records;
`start-key` is inclusive and `limit-key` is exclusive. The compaction response
includes `size_bytes_before`, `size_bytes_after`, `size_bytes_delta`, duration,
and selected engine property snapshots before and after compaction:

```
make storage-status STORAGE_PEER=http://127.0.0.1:8080
make storage-flush STORAGE_PEER=http://127.0.0.1:8080
make storage-compact STORAGE_PEER=http://127.0.0.1:8080
make storage-compact STORAGE_PEER=http://127.0.0.1:8080 STORAGE_COMPACT_START_KEY=session: STORAGE_COMPACT_LIMIT_KEY='session;'
make cli ARGS='storage flush'
make cli ARGS='storage compact -start-key session: -limit-key session;'
```

Local storage benchmark on an AMD Ryzen 9 5950X with 512 materialized string
entries:

```
make bench-serialization SERIALIZATION_BENCH='BenchmarkLevelDB(Save|Load)Materialized' BENCHTIME=20x
```

| Format | Save CPU | Load CPU | Record bytes | Save heap | Load heap |
| --- | ---: | ---: | ---: | ---: | ---: |
| binary | 1.56 ms/op | 2.79 ms/op | 293,376 B/op | 657,016 B/op | 1,205,047 B/op |
| json | 3.34 ms/op | 4.25 ms/op | 394,194 B/op | 1,149,580 B/op | 1,912,317 B/op |

The tradeoff is readability: binary saves are about 2.1x faster, loads are
about 1.5x faster, and records are about 26% smaller for this workload, while
JSON remains easier to inspect with standard text tools.

The backend bakeoff uses 10,000 deterministic 256-byte values, then performs
500 updates, 250 deletes, 250 inserts, a full load, and manual compaction:

```
make bench-storage-backends BENCHTIME=3x COUNT=5
```

| Backend | Full cycle | Save/key | Churn/op | Load/key | Heap/cycle | Peak RSS | Disk/key |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| LevelDB | 91.602 ms | 2,197 ns | 2,471 ns | 1,856 ns | 41.52 MB | 81,384 KiB | 265.3 B |
| Pebble | 98.273 ms | 2,441 ns | 3,312 ns | 2,295 ns | 20.52 MB | 82,684 KiB | 285.7 B |

Pebble remains the new-path default because generation saves allocate 2.02x
less cumulative heap and reduce its live disk footprint to within 1.08x of
LevelDB while providing an atomic full-state switch. LevelDB completed this
mixed fixture 1.07x faster, loaded 1.24x faster, and closed 21.3x faster;
select it for latency-sensitive or frequently opened short-lived processes.
Peak RSS was effectively tied on this run. See
[BENCHMARK.md](BENCHMARK.md#persistent-storage-backend-bakeoff) for raw medians
and phase details.

Set `DB_HOT_LOAD=true` to load all non-expired persistent keys as compact
references while only materializing hot values in memory. By default a hot value
must be 1024 bytes or smaller, have a last hit within 1 hour, and have more than
1000 recorded hits. Cold values are hydrated from the selected backend on first
value access and are still preserved by later saves. The existing
`LevelDBStore`, `LevelDBLoadPolicy`, and lazy-reference API names remain for
source compatibility and work with both engines through `PersistentStore`.
Keep the store open while cold references may be accessed, or call
`HydrateLevelDBReferences` before
closing it to materialize all references into the trie:

```
make monitoring-server DB_PATH=data/cache.leveldb DB_HOT_LOAD=true
```

Cold backend reads run outside the global trie lock. Concurrent readers of the
same reference share one backend request, while updates and deletes may proceed;
a monotonic reference token prevents a stale read from replacing newer state.
On the 32-key delayed-I/O fixture, parallel hydration completes in 1.174 ms
versus 33.875 ms serialized, a 28.85x latency improvement. See
[BENCHMARK.md](BENCHMARK.md#parallel-cold-reference-hydration).

Lazy references use a 64-byte internal record while preserving the exported
`LevelDBReference` API. Store handles are interned once per trie, type strings
are encoded as compact IDs, and expiration metadata is stored inline. At 100k
references this retains 71.6 B/key instead of 90.2 B/key and builds 1.44x
faster. See [BENCHMARK.md](BENCHMARK.md#compact-lazy-reference-slab).

Set `DB_MEMORY_CAP_BYTES` and/or `DB_RSS_CAP_BYTES` with
`DB_MEMORY_EVICT_INTERVAL` to keep the running hot value set under a soft byte
cap or to react when process RSS crosses a threshold. The governor estimates
in-memory value payload bytes, writes cold eligible values to persistent storage,
and replaces them with lazy references. Values with no recent hits spill
first; ties prefer older hits, fewer hits, then larger values.
`DB_MEMORY_EVICT_MIN_VALUE_BYTES` defaults to 1024 so tiny keys are left in
memory unless you lower it. `DB_MEMORY_CAP_BYTES` targets estimated hot payload
bytes; `DB_RSS_CAP_BYTES` is a coarser process-level pressure trigger. With only
`DB_RSS_CAP_BYTES` set, a breached RSS threshold spills all eligible cold values.
With both set, RSS is an extra trigger while `DB_MEMORY_CAP_BYTES` remains the
hot-byte target. This is a soft cap: keys already stored outside the Go heap,
such as large byte payloads on disk, are not counted, and the cap may remain
above target when there are no eligible values to spill. The tradeoff is lower
heap/RSS pressure in exchange for storage write I/O during spill passes and one
storage read when a cold value is accessed again:

```
make monitoring-server DB_PATH=data/cache.leveldb DB_MEMORY_CAP_BYTES=1073741824 DB_MEMORY_EVICT_INTERVAL=30s
make monitoring-server DB_PATH=data/cache.leveldb DB_RSS_CAP_BYTES=2147483648 DB_MEMORY_EVICT_INTERVAL=30s
```

`GET /api/storage` includes the last spill result as `last_spill`, and
Prometheus exposes `hatrie_cache_storage_last_spill_keys`,
`hatrie_cache_storage_last_spill_hot_bytes_before`, and
`hatrie_cache_storage_last_spill_hot_bytes_after`.

Set `SNAPSHOT_PATH` to load a snapshot at startup and save it on shutdown.
Snapshots save as storage-optimized gzip binary by default
(`SNAPSHOT_FORMAT=gzip-best-binary`) and still load binary, gzip binary, older
gzip JSON, and plain JSON snapshots automatically. Set
`SNAPSHOT_FORMAT=gzip-binary` for a faster compact binary snapshot, or set
`SNAPSHOT_FORMAT=gzip-best-json` to keep writing the previous storage-optimized
JSON layout. Set `SNAPSHOT_FORMAT=gzip-json` for the previous faster gzip JSON
format, or `SNAPSHOT_FORMAT=json` for the previous plain JSON file format. Set
`SNAPSHOT_INTERVAL` to periodically write the same snapshot while the server
runs:

```
make monitoring-server SNAPSHOT_PATH=data/snapshot.json SNAPSHOT_INTERVAL=30s
make monitoring-server SNAPSHOT_PATH=data/snapshot.json SNAPSHOT_FORMAT=gzip-binary
make monitoring-server SNAPSHOT_PATH=data/snapshot.json SNAPSHOT_FORMAT=gzip-best-json
make monitoring-server SNAPSHOT_PATH=data/snapshot.json SNAPSHOT_FORMAT=gzip-json
make monitoring-server SNAPSHOT_PATH=data/snapshot.json SNAPSHOT_FORMAT=json
```

Online snapshots capture an immutable point-in-time entry set in bounded scan
pages, releasing the trie lock between pages, then perform encoding,
compression, disk writes, and network streaming without the trie lock.
Concurrent mutations are tracked and reconciled at the journal sequence
barrier. Journaled commands after that sequence remain available as ordered
deltas. Full LevelDB saves use the same capture boundary. Snapshot jobs sharing
a journal are serialized. In the 100,000-key benchmark, bounded pages reduced
the median maximum reader pause from 61.740 ms to 2.822 ms (21.88x) while total
snapshot time and cumulative heap remained effectively flat. See
[BENCHMARK.md](BENCHMARK.md#bounded-page-snapshot-capture).

Set `JOURNAL_PATH` to replay an append-only command journal at startup and fsync
mutating cache commands before applying them. When `SNAPSHOT_PATH` is also set,
snapshots store the journal checkpoint. The server defaults to 64 MiB journal
segments and retains 16 closed segments in `<JOURNAL_PATH>.segments/`. A
successful snapshot rotates the active file instead of rewriting the complete
history, so lagging replicas can continue incremental catch-up until their
sequence falls behind the retained segment window. Pruning removes only whole
closed segments and reports the resulting `compacted_through` boundary. Set
`JOURNAL_SEGMENT_MAX_BYTES=0` for the previous single-file compaction behavior;
set `JOURNAL_RETAINED_SEGMENTS` to size the catch-up window. Journal records
write in the binary format by default
(`JOURNAL_FORMAT=binary`) and still read existing line-delimited JSON journals,
including files that contain both old JSON records and new binary records.
Binary journal records store structured `values` and `pairs` payloads with the
compact binary value codec when that is smaller than their JSON representation,
and otherwise keep the JSON inner payload. Set `JOURNAL_FORMAT=json` to keep
writing the previous JSON journal layout.

Durable writes use group commit by default without delaying for a timer: the
worker yields once, batches up to 64 already queued commands, writes them in
order, and issues one `fsync`. Every successful command waits for that sync
before it is applied or acknowledged. Set a positive
`JOURNAL_GROUP_COMMIT_WINDOW` to wait briefly for larger batches under sparse
traffic, or set `JOURNAL_GROUP_COMMIT_MAX_BATCH=1` for the previous immediate
one-command-per-fsync behavior. Batch size is validated in the range 1-4096 to
prevent malformed configuration from triggering a large allocation:

```
make monitoring-server SNAPSHOT_PATH=data/snapshot.json JOURNAL_PATH=data/commands.journal
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_FORMAT=json
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_GROUP_COMMIT_WINDOW=250us JOURNAL_GROUP_COMMIT_MAX_BATCH=64
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_GROUP_COMMIT_MAX_BATCH=1
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_SEGMENT_MAX_BYTES=134217728 JOURNAL_RETAINED_SEGMENTS=32
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_SEGMENT_MAX_BYTES=0
```

Rotation occurs before a durable batch, never inside one, so rollback and
group-commit acknowledgement semantics remain unchanged. A segment can exceed
the byte target by one batch. With defaults, closed-history capacity is roughly
1 GiB plus the active file; choose retention from write rate and maximum replica
downtime, then alert on `409` journal responses that indicate the window was
exceeded.

A public `BATCH` with journaling enabled appends all successful subcommands in
order and performs one final `fsync` before acknowledgement, rather than one
sync per subcommand. A sync/write failure rolls back both journal bytes and the
batch's in-memory mutations. Ordinary command errors retain the documented
non-transactional pipeline behavior: earlier successful subcommands remain
applied. Three 4,096-item-or-smaller batches completed 10,000 durable writes in a
29.051 ms median with three syncs versus 9.821 seconds and 10,000 syncs for
individual commands; see
[BENCHMARK.md](BENCHMARK.md#durable-public-batches).

Without journal interception, same-family scalar batches of at least 32
commands pack keys and operations into one native HAT-trie call under one Go
lock. This covers `GET`/`GETSTR`/`EXISTS`, `SET`/`SETSTR`, `SETINT`, `INC`, and
`DEL`. Smaller, mixed, TTL-sensitive, or cold-reference-sensitive batches use
the existing Go executor. At 4,096 commands the native path is 1.14x faster for
counter sets and 1.15x faster for reads with unchanged steady-state heap. See
[BENCHMARK.md](BENCHMARK.md#native-c-command-batching).

When journaling is enabled, `GET /api/journal?after_sequence=N&limit=1000`
returns a bounded batch of ordered mutating commands after `N` plus the latest
journal sequence. Responses include `has_more` when another batch is available.
If `N` is older than the compacted snapshot checkpoint, the endpoint returns
`409`. `GET /api/journal/snapshot` streams a fast-gzip binary point-in-time
snapshot with its journal sequence. For a Pebble-backed source,
`GET /api/journal/checkpoint` streams a checksummed native Pebble checkpoint
bundle. Both full-state endpoints require the replication authentication token.
The source captures state and sequence under the journal write barrier, releases
that barrier before streaming, and retains writes after that sequence as ordered
delta records.
`POST /api/journal` accepts `source`, optional `after_sequence`, and optional
`limit`, pulls that source node's journal tail, and applies the returned
mutating commands locally through the configured journal. Set `until_current`
and optional `max_batches` to continue pulling bounded batches until the source
has no more entries or the batch cap is reached.
Set `JOURNAL_PULL_SOURCE` with `JOURNAL_PATH` to automatically pull bounded
catch-up batches from another node at startup; `JOURNAL_PULL_STATE_PATH`
persists the source sequence so non-idempotent commands are not replayed after
restart. Journal pull HTTP requests use a 30 second timeout by default; set
`JOURNAL_PULL_TIMEOUT=0` to disable that bound for a deliberate long-running
source. Add `JOURNAL_PULL_INTERVAL` to repeat catch-up periodically. Catch-up
uses the compact binary journal-tail wire format by default. A new follower
requests binary with JSON as an acceptable fallback, so it can pull from an
older source that only returns JSON. Set `JOURNAL_PULL_WIRE_FORMAT=json` to
force the previous wire format. This setting is independent of
`JOURNAL_FORMAT`, which controls durable journal records on disk. Direct
`GET /api/journal` clients continue to receive JSON unless they explicitly
request `application/vnd.hatrie-cache.journal-tail`.

For a 10,000-command `SETINT` tail, binary encode plus decode is 5.16x faster,
uses 4.69x less cumulative heap, performs 2,510x fewer allocations, and transfers
2.79x fewer body bytes than JSON. Binary is intended for service-to-service
catch-up; JSON remains easier to inspect with generic tools. See
[BENCHMARK.md](BENCHMARK.md#binary-journal-catch-up-wire).

For the default binary `SETINT` catch-up path, decoded keys and textual integer
values now remain borrowed only until synchronous WAL append and application
finish instead of being cloned per record. Decode plus ownership transfer is
1.37x faster, performs 6,667.67x fewer allocations, and uses 1.08x less
cumulative heap for 10,000 records. Stored strings, TTL keys, structured
commands, active key stats/snapshots/LevelDB indexes, persistent dirty tracking,
and local partitions keep conservative ownership clones. See
[BENCHMARK.md](BENCHMARK.md#selective-journal-wire-ownership).

Homogeneous binary tails of plain string sets or counter sets also decode into
internal 48-byte scalar records instead of full public command requests. For
10,000 `SETINT` records, the complete decode + durable WAL + apply path is 1.38x
faster and uses 1.89x less cumulative heap, with unchanged allocations, wire
bytes, WAL format, and one-final-fsync durability. TTL, mixed, and structured
tails automatically use the full decoder, malformed binary is
rejected, and short runs retain serial application. See
[BENCHMARK.md](BENCHMARK.md#compact-scalar-journal-tails).

Coalesced WAL staging for full, compact, and group-commit batches is bounded to
128 KiB by default instead of reserving up to 1 MiB. On the 10,000-record
compact fixture, this reduced staging heap 3.47x and improved the median 1.05x,
at the cost of four writes instead of one before the unchanged final `fsync`.
The full binary decode + durable WAL + apply path used 1.43x less cumulative
heap and was 1.21x faster in the fresh paired run. Oversized individual records
remain valid, and a later-chunk failure rolls the complete unapplied batch back.
See [BENCHMARK.md](BENCHMARK.md#bounded-wal-staging-arena).

The follower also coalesces pulled records into bounded WAL write chunks before
one final `fsync`, instead of allocating and writing every record separately.
For the same 10,000-command tail this makes durable apply 2.84x faster, uses
2.78x less cumulative heap, and performs 6,001x fewer allocations without
changing WAL bytes or rollback semantics. See
[BENCHMARK.md](BENCHMARK.md#coalesced-journal-batch-append).

After that WAL is durable, homogeneous runs of at least 32 plain string or
counter sets are also applied under one trie lock with coalesced stats
bookkeeping. The 10,000-command application phase is 1.61x faster, and the
fsync-inclusive durable path is 1.15x faster, with unchanged heap, allocation,
and WAL-byte measurements. TTL writes, increments, mixed/short runs, and local
partitions automatically keep the serial correctness path. See
[BENCHMARK.md](BENCHMARK.md#single-lock-journal-scalar-apply).

Journal pull is delta-first by default. If the requested delta was compacted,
an existing Pebble follower first requests `/api/journal/recovery`, verifies its
source-specific content-addressed object cache, downloads only missing
checkpoint objects, checksum-stages an exact store, replaces the complete key
set (including stale-key deletion), resets its local journal checkpoint,
atomically adopts the verified checkpoint as its active Pebble generation, and
only then advances `JOURNAL_PULL_STATE_PATH`. The configured DB path and shared
store handle remain stable for background persistence and monitoring.
The leader repository defaults to
`DB_PATH.journal-recovery-repository`; the follower cache is derived from the
state path and a hash of `JOURNAL_PULL_SOURCE`. Both are created only when
recovery is needed. The leader retains 32 manifests and garbage-collects
unreachable objects; the follower retains the current manifest's objects.

Incremental recovery is enabled by default with
`JOURNAL_PULL_INCREMENTAL_RECOVERY=true`. It requires Pebble on both nodes and
matching storage formats. If negotiation, validation, staging, or replacement
fails, or either node is unsupported, the follower automatically downloads
`/api/journal/snapshot`, validates it before replacing the previous recovery
file, and follows the existing exact snapshot path. The durable snapshot
filename is also source-specific. On restart, the newer of this recovery
snapshot and `SNAPSHOT_PATH` is loaded before journal replay.
Set `JOURNAL_PULL_FULL_SYNC_FALLBACK=false` to keep the previous fail-closed
`409` behavior. Full fallback is intentionally more CPU, disk, and bandwidth
work than a delta pull, but it runs only when the required deltas no longer
exist. Set `JOURNAL_PULL_INCREMENTAL_RECOVERY=false` to skip repository
negotiation and use the snapshot fallback directly. On the 10,000-key,
1%-changed fixture, repository recovery is 1.62x faster, uses 1.05x less heap,
performs 2.26x fewer allocations, and transfers 56.39x fewer response-body
bytes than full snapshot recovery. Direct checkpoint adoption then removes the
remaining full local record rewrite, reducing incremental recovery heap 1.24x
and allocations 1.34x while improving latency 1.05x. Pull status reports
`recovery_checkpoint_adopted=true` when this path succeeds. Runtime publication
failure rolls back and reopens the old database; startup repairs an interrupted
`.recovery-old` exchange. See
[BENCHMARK.md](BENCHMARK.md#active-recovered-pebble-generation).

On a fresh follower whose `DB_PATH` does not exist, Pebble checkpoint bootstrap
is enabled by default with `JOURNAL_PULL_CHECKPOINT_BOOTSTRAP=true`. Before the
database opens, the follower downloads and checksum-stages the leader's native
checkpoint, records a crash-recovery marker, atomically installs the store and
backend marker, resets the local journal to the checkpoint sequence, and only
then advances pull state. Normal startup opens and semantically loads the
installed store before any listener starts. An interrupted install resumes from
the marker; an existing database is never replaced at startup and continues to
use normal delta, incremental repository, or snapshot recovery. Unsupported
backends and unavailable checkpoint endpoints also retain the snapshot path. Set
`JOURNAL_PULL_CHECKPOINT_BOOTSTRAP=false` to disable this fresh-node fast path.
For 10,000 256-byte values it was 1.74x faster with 2.72x lower cumulative heap
and 2.76x fewer allocations than snapshot bootstrap, at the cost of 2.55% more
wire bytes; see [BENCHMARK.md](BENCHMARK.md#checkpoint-replica-bootstrap).

Examples:

```
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080 JOURNAL_PULL_INTERVAL=5s
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080 JOURNAL_PULL_TIMEOUT=5s
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080 JOURNAL_PULL_WIRE_FORMAT=json
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080 JOURNAL_PULL_FULL_SYNC_FALLBACK=false
make monitoring-server DB_PATH=data/cache JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080 JOURNAL_PULL_INCREMENTAL_RECOVERY=false
make monitoring-server DB_PATH=data/cache JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080 JOURNAL_PULL_CHECKPOINT_BOOTSTRAP=false
make cli ARGS='journal -pull-from http://leader:8080 -until-current -wire-format json'
```

Set `NODE_ID` and `TOPOLOGY_PATH` to expose and persist cluster topology JSON.
The topology endpoint validates nodes, shard ownership, and replicas, and can
route a key to its shard. Each node uses `address` for HTTP and can set a
separate `grpc_address` for native gRPC, for example `node-b:9090`,
`grpc://node-b:9090`, or `grpcs://node-b:9090`. Sharding is opt-in. Topologies
without a mode or shard metadata default to `mode: "full_replica"`, routing
every key to every node. Existing mode-less topology files that contain
`shards`, `bucket_count`, or `bucket_ranges` remain compatible and are inferred
as sharded. Set `mode: "sharded"` explicitly for new sharded topologies, then
set `bucket_count` and compact `bucket_ranges` for vbucket-style routing:
see [`SHARDING_PROPOSAL.md`](SHARDING_PROPOSAL.md) for the proposed XXH3 slot
hash, hash tags, rendezvous placement planner, and migration states. For
multi-datacenter or region-owned datasets, see
[`PARTITIONING_PROPOSAL.md`](PARTITIONING_PROPOSAL.md) for the proposal to keep
explicit partition ownership, backup boundaries, and failover separate from
automatic sharding.

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json
```

Set `ELECTION_TIMEOUT` to control deterministic topology-based leader failover.
The current shard primary stays leader while healthy; when it is marked offline
or its heartbeat times out, the first healthy replica becomes leader. A running
monitoring server refreshes its own node heartbeat periodically while it is up:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json ELECTION_TIMEOUT=15s
```

Set `REPLICATION=true` to enable the configured replication mode.
`REPLICATION_MODE=journal` is the default and makes the command journal the
primary replication log; it requires `JOURNAL_PATH`. Source nodes append local
mutations to the journal and serve `/api/journal`; replicas use
`JOURNAL_PULL_SOURCE`, `JOURNAL_PULL_STATE_PATH`, and `JOURNAL_PULL_INTERVAL` to
pull ordered journal tails and persist the applied sequence. Use this mode for
normal replica catch-up because it preserves command order and avoids per-write
remote fanout in the write path.

Set `REPLICATION_MODE=command` to keep the previous HTTP command fanout mode, or
`REPLICATION_MODE=dual` to run journal-stream replication and command fanout
together during migration. Command fanout lets an elected leader broadcast
successful local mutations to the current key's topology owners over HTTP. It
uses `INTERNALSETV3` keyless binary snapshot values by default with protobuf,
plus the internal `INTERNALDEL` and batch commands. It skips internal
replication commands to avoid loops and records the last command-fanout attempt
at `/api/replication`. `INTERNALBATCHV2` batches multiple internal replication commands
for the same target during sync and async
replay while carrying source, sequence, and topology metadata once on the batch
envelope. For an older peer, the sender first retries `INTERNALSETV2`, whose
binary value includes the key, then automatically retries the legacy
`INTERNALSET` or `INTERNALBATCH` request with JSON snapshots and per-item
metadata. Replication
batches are split before send when their estimated
uncompressed request body would exceed `REPLICATION_BATCH_MAX_BYTES` (default
`1048576`). Set `REPLICATION_BATCH_MAX_BYTES=0` to disable byte-based
splitting. Replication delivers to at most four targets concurrently by default
(`REPLICATION_MAX_IN_FLIGHT_TARGETS=4`). Set
`REPLICATION_MAX_IN_FLIGHT_TARGETS=1` for serial target delivery, or lower the
default when each target is reached through the same bandwidth-limited link.
Target results remain in deterministic topology order. HTTP replication command bodies use protobuf by default
(`REPLICATION_WIRE_FORMAT=protobuf`), then automatically use the previous JSON
wire format for structured `values` or `pairs` payloads that protobuf cannot
represent. Set `REPLICATION_WIRE_FORMAT=json` to always use JSON;
`REPLICATION_WIRE_FORMAT=json` converts snapshots to legacy JSON before send.
Large HTTP replication request bodies are gzip-compressed.
Anti-entropy digest discovery uses the authenticated HTTP command endpoint.
Set `REPLICATION_TRANSPORT=grpc-stream` to keep one ordered, gzip-compressed
HTTP/2 `ReplicationStream` open per target for both live command fanout and
repair writes. The default `REPLICATION_GRPC_WINDOW=32` permits up to 32 sent,
unacknowledged batches per target; set it from 1 through 1,024 to bound
throughput, memory, and backpressure. A dedicated receiver pairs acknowledgements
by sequence while per-key replay safety permits independent keys to complete
out of order. Live fanout also groups up to
`REPLICATION_GRPC_BATCH_MAX_COMMANDS=32` already-queued commands into one wire
batch and fans its acknowledgement back to each caller. The default
`REPLICATION_GRPC_BATCH_WINDOW=0` adds no timed wait; set a positive duration
only to trade latency for fewer bytes. Set the command limit to `1` for the
previous one-command-per-batch behavior. The existing
`REPLICATION_BATCH_MAX_BYTES` also bounds each grouped batch. The target node
must publish `grpc_address` and run its opt-in
native listener with `GRPC_ADDR`. A bare address or `grpc://` uses an insecure
internal connection, while `grpcs://` verifies the server with the host trust
store. `REPLICATION_AUTH_TOKEN` is accepted by the stream without granting
access to other gRPC methods.

If a stream cannot be opened or fails in transit, sync falls back to the
existing HTTP path by default. Set `REPLICATION_HTTP_FALLBACK=false` to fail
closed instead. For 10,000 live writes from 32 callers, zero-wait
micro-batching reduced the valid pipelined gRPC baseline from 10,000 to 2,910
wire batches. Grouping jobs before allocating protobuf requests and caching
immutable topology metadata then reduced the median from 154.3 ms to 126.9 ms,
from 2,959 to 2,305 batches, and from 353.6 MB to 303.2 MB of cumulative heap.
Reproduce it with `make bench-live-replication BENCHTIME=1x COUNT=7`. See
[BENCHMARK.md](BENCHMARK.md#pipelined-live-grpc-replication).
`POST /api/replication` runs an explicit command-fanout anti-entropy sync. For
an unfiltered single-shard data set, the source and target first compare an
maintained 1,024-bucket Merkle root. Equal replicas stop after one
small request. A mismatch fetches only differing bucket digest pages, then sends
changed or missing values plus deletes for target-only stale keys. The index is
dormant until the first unfiltered sync, then retains about 29.6 B/key. Active
writes coalesce up to 1,024 unique pending keys; the next sync applies each final
value once, while broader churn triggers one linear index rebuild. This reduced
the measured 100,000-write-plus-sync cycle from 45.5 ms to 25.8 ms. Prefix sync
and multi-shard routing use the compatible bounded, sorted digest path.
Write batches retain at most 1,024 keys and are also split by
`REPLICATION_BATCH_MAX_BYTES`.

Peers that do not support `INTERNALDIGESTV1` automatically fall back to bounded
full value push. That compatibility path can update and create source keys but
cannot discover target-only stale keys; upgrade both peers before relying on
anti-entropy deletion. Digest comparison is deliberately non-cryptographic: it
trades a theoretical 64-bit collision risk for low CPU and memory overhead.
Anti-entropy requires `REPLICATION_MODE=command` or `REPLICATION_MODE=dual`:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true JOURNAL_PATH=data/node-a.journal
make monitoring-server NODE_ID=node-b TOPOLOGY_PATH=data/topology.json REPLICATION=true JOURNAL_PATH=data/node-b.journal JOURNAL_PULL_SOURCE=http://node-a:8080 JOURNAL_PULL_INTERVAL=5s
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command REPLICATION_WIRE_FORMAT=json
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command REPLICATION_TRANSPORT=grpc-stream
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command REPLICATION_TRANSPORT=grpc-stream REPLICATION_GRPC_WINDOW=64
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command REPLICATION_TRANSPORT=grpc-stream REPLICATION_GRPC_BATCH_MAX_COMMANDS=1
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command REPLICATION_TRANSPORT=grpc-stream REPLICATION_GRPC_BATCH_WINDOW=25us
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command REPLICATION_TRANSPORT=grpc-stream REPLICATION_HTTP_FALLBACK=false
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=dual JOURNAL_PATH=data/node-a.journal REPLICATION_BATCH_MAX_BYTES=262144
```

Set `REPLICATION_AUTH_TOKEN` on each node to authenticate outbound HTTP
replication and require the same token for inbound `INTERNALSET`, `INTERNALDEL`,
`INTERNALSETV2`, `INTERNALSETV3`, `INTERNALBATCH`, `INTERNALBATCHV2`, and
`INTERNALDIGESTV1` commands. Replication clients send both
`Authorization: Bearer <token>` and `X-Hatrie-Replication-Token: <token>`.
The replication token is intentionally narrow: it is accepted on
`POST /api/commands` for internal replication traffic and on journal recovery
reads (`GET /api/journal` and `GET /api/journal/snapshot`). It is not accepted
for health, metrics, config, normal client commands, `POST /api/journal`, or
`POST /api/replication`. The
operator `MONITORING_AUTH_TOKEN` still has full monitoring API access.

Replication payloads include source-node, monotonic sequence, and topology
fingerprint metadata. Receivers suppress duplicate internal replication
commands from the same source/sequence. Clustered receivers reject replication
when the sender fingerprint does not match their local topology, which catches
split-brain or stale-topology delivery before the write is applied. Nodes
without cluster routing metadata still accept replication and only use the
source/sequence duplicate guard.

Set `REPLICATION_SYNC_INTERVAL` to run the command-fanout anti-entropy sync
periodically from the local leader. It requires `REPLICATION_MODE=command` or
`REPLICATION_MODE=dual`. The first sync runs immediately at startup, then
repeats at the configured interval. Set `REPLICATION_SYNC_PREFIX` to limit the
scheduled sync to one key prefix. See
[`BENCHMARK.md`](BENCHMARK.md#incremental-anti-entropy) for equal, 1%-changed,
and full-transfer CPU, heap, request, and bandwidth measurements:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command REPLICATION_SYNC_INTERVAL=30s
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_MODE=command REPLICATION_SYNC_INTERVAL=30s REPLICATION_SYNC_PREFIX=session:
```

Set `REPLICATION_ASYNC=true` with `REPLICATION_MODE=command` or
`REPLICATION_MODE=dual` to enqueue command-fanout replication in a bounded
in-process outbox instead of waiting for remote owners in the write request
path. Queued jobs store the already-materialized internal snapshot payload, so
later local mutations do not change what is delivered for the original write.
Tune
`REPLICATION_QUEUE_SIZE`, `REPLICATION_RETRY_INTERVAL`, and
`REPLICATION_MAX_ATTEMPTS` to bound memory and retry failed HTTP deliveries.
Set `REPLICATION_OUTBOX_PATH=data/replication-outbox` to persist queued async
jobs and recent dead letters to local disk so a restarted node replays jobs
that were not confirmed before shutdown. The default
`REPLICATION_OUTBOX_FORMAT=auto` uses LevelDB for non-`.json` paths, storing
each queued job as its own record so enqueue/delete does not rewrite the full
outbox. Existing `*.json` paths keep the previous JSON snapshot backend; set
`REPLICATION_OUTBOX_FORMAT=json` or `REPLICATION_OUTBOX_FORMAT=leveldb` to
force either backend. LevelDB records use the compact binary codec by default
and automatically read existing JSON records. Set
`REPLICATION_OUTBOX_CODEC=json` for new JSON LevelDB records. LevelDB restart
reads jobs lazily in ordered pages and refills at half capacity, so
`REPLICATION_QUEUE_SIZE` remains the hard in-memory job and metadata bound even
when disk contains a much larger backlog. New durable jobs remain behind the
restore cursor to preserve FIFO delivery. The legacy whole-file JSON backend
uses the bounded channel too, but still materializes its JSON file when opened.
Queue status exposes `durable_backlog` while unread disk pages remain.
Concurrent durable puts use a 1 ms group-commit window by default; set
`REPLICATION_OUTBOX_BATCH_WINDOW=0` to sync every put independently. Every
caller still waits for its group sync before success. Keep the outbox on durable
local storage and do not share the same outbox path between nodes.
When both `JOURNAL_PATH` and a LevelDB `REPLICATION_OUTBOX_PATH` are configured,
the server automatically stores the exact post-mutation replication envelope in
the command journal and only an unsynced sequence reference in LevelDB. The
journal fsync is the durability boundary; restart reconciliation recreates a
missing reference, a synced completion watermark prevents acknowledged jobs
from returning, and segmented journals retain records needed by pending jobs.
This halves sync writes on a single durable enqueue. To retain the previous
full-job outbox behavior, omit `JOURNAL_PATH` or select
`REPLICATION_OUTBOX_FORMAT=json`. The JSON fallback remains independently
durable and inspectable but cannot share the journal fsync.
After the final async retry fails, the job is retained in a bounded dead-letter
list without command values; tune `REPLICATION_DEAD_LETTER_LIMIT` or set it to
`0` to disable retention.
Per-target circuit breakers stop repeatedly calling unhealthy replicas after
`REPLICATION_CIRCUIT_BREAKER_FAILURES` consecutive failures, then allow a
half-open probe after `REPLICATION_CIRCUIT_BREAKER_COOLDOWN`. Set either value
to `0` to disable the breaker.
Library users can pass `HTTPReplicatorOptions.Context` to tie the async worker
lifetime to a parent service context.
`GET /api/replication` includes the latest replication start/finish timestamps,
duration, async queue depth, capacity, enqueue/drop counts, delivery attempts,
successes, failures, retries, oldest queued key/age, in-flight key/age, last
retry age, durable backlog state, per-target drops, per-target failures, closed state,
`dead_letter_count`, recent `dead_letters`, `circuit_breakers`, target-level
`circuit_open` state, and a `health_score` from `0` to `100` with `health` and
`health_reason`:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_ASYNC=true
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_ASYNC=true REPLICATION_OUTBOX_PATH=data/replication-outbox
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_ASYNC=true REPLICATION_OUTBOX_PATH=data/replication-outbox REPLICATION_OUTBOX_CODEC=json
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_ASYNC=true REPLICATION_OUTBOX_PATH=data/replication-outbox REPLICATION_OUTBOX_BATCH_WINDOW=0
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_ASYNC=true REPLICATION_OUTBOX_PATH=data/replication-outbox.json REPLICATION_OUTBOX_FORMAT=json
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_AUTH_TOKEN=replica-secret
```

Set `ENFORCE_LEADER_WRITES=true` on clustered nodes to reject mutating client
commands unless the local node is the elected leader for the command key.
Internal replication commands are still accepted so followers can apply leader
updates:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true ENFORCE_LEADER_WRITES=true
```

The monitoring server exposes JSON APIs at `/api/health`, `/api/stats`,
`/api/entries`, `/api/storage`, `/api/storage/flush`, `/api/storage/compact`,
`/api/audit`, `/api/topology`, `/api/election`, `/api/replication`,
`/api/journal`, and `/api/commands`, plus
Prometheus metrics at `/metrics`.
Prometheus output includes cache counters plus audit and protection counters:
`hatrie_cache_audit_events_total`, `hatrie_cache_audit_errors_total`,
`hatrie_cache_write_protection_rejections_total`,
`hatrie_cache_rate_limit_rejections_total`, and gauges for
`hatrie_cache_write_protection_enabled` and
`hatrie_cache_rate_limit_per_second`. When LevelDB is configured, Prometheus
exports `hatrie_cache_leveldb_dirty_keys` and
`hatrie_cache_storage_operation_running`. When replication is configured,
Prometheus also exports `hatrie_cache_replication_health_score`,
`hatrie_cache_replication_dead_letters`,
`hatrie_cache_replication_queue_capacity`, and async queue counters such as
`hatrie_cache_replication_queue_enqueued_total` and
`hatrie_cache_replication_retried_total`.
Use `GET /api/entries?prefix=...&limit=N` to bound large key listings; limited
responses include `has_more` and `next_after_key` for cursor paging with
`after_key`. Empty keys are valid, so when `next_after_key` is empty and
`has_more` is true, send an explicit empty cursor as `after_key=`.
The Svelte MPA dashboard and key browser use bounded entry requests by default.
`/api/commands` accepts JSON and protobuf command request bodies based on
`Content-Type`; regular browser/API clients can continue to use JSON.
Responses are gzip-compressed when clients send `Accept-Encoding: gzip`.

<a id="serialization-tradeoffs"></a>
Serialization tradeoffs are measured with `make bench-serialization` on an AMD
Ryzen 9 5950X. The storage and snapshot rows below use `BENCHTIME=20x` to keep
the slower compression and LevelDB cases bounded. The `Structured` variants
exercise maps, queues, filters, sketches, bitmaps, Fenwick trees, reservoir
samples, XOR filters, and radix trees.

| Path | Format | CPU | Wire/disk bytes | Heap bytes | Allocs |
| --- | --- | ---: | ---: | ---: | ---: |
| HTTP command wire | JSON | 15,012 ns/op | 3,185 wire_B/op | 3,387 B/op | 3 |
| HTTP command wire | protobuf (default) | 12,637 ns/op | 3,146 wire_B/op | 3,408 B/op | 3 |
| Journal encode | JSON fallback | 7,800 ns/op | 3,224 journal_B/op | 8,496 B/op | 3 |
| Journal encode | binary (default) | 3,362 ns/op | 3,159 journal_B/op | 6,400 B/op | 2 |
| Journal decode | JSON fallback | 30,034 ns/op | 3,224 journal_B/op | 22,728 B/op | 29 |
| Journal decode | binary (default) | 20,035 ns/op | 3,159 journal_B/op | 18,071 B/op | 25 |
| Structured journal encode | JSON fallback | 2,848 ns/op | 668 journal_B/op | 2,443 B/op | 7 |
| Structured journal encode | binary (default) | 4,434 ns/op | 553 journal_B/op | 2,388 B/op | 20 |
| Structured journal decode | JSON fallback | 5,528 ns/op | 668 journal_B/op | 4,428 B/op | 65 |
| Structured journal decode | binary (default) | 3,539 ns/op | 553 journal_B/op | 3,856 B/op | 62 |
| Snapshot save | JSON fallback | 1,791,394 ns/op | 465,912 disk_B/op | 654,791 B/op | 15,877 |
| Snapshot save | binary | 866,052 ns/op | 294,407 disk_B/op | 663,589 B/op | 4,097 |
| Snapshot save | gzip JSON (fast JSON fallback) | 5,122,924 ns/op | 7,757 disk_B/op | 761,425 B/op | 29,192 |
| Snapshot save | gzip best JSON (previous default) | 6,451,204 ns/op | 5,469 disk_B/op | 762,164 B/op | 29,192 |
| Snapshot save | gzip binary (fast binary fallback) | 1,754,702 ns/op | 5,109 disk_B/op | 664,319 B/op | 4,097 |
| Snapshot save | gzip best binary (default) | 9,592,754 ns/op | 4,549 disk_B/op | 664,564 B/op | 4,097 |
| Structured snapshot save | JSON fallback | 1,904,499 ns/op | 254,274 disk_B/op | 478,893 B/op | 10,114 |
| Structured snapshot save | binary | 1,182,100 ns/op | 79,891 disk_B/op | 510,680 B/op | 4,114 |
| Structured snapshot save | gzip best JSON fallback | 18,866,057 ns/op | 6,956 disk_B/op | 595,135 B/op | 24,454 |
| Structured snapshot save | gzip best binary (default) | 9,847,768 ns/op | 5,787 disk_B/op | 511,394 B/op | 4,114 |
| LevelDB save | binary materialized values | 1,558,684 ns/op | 293,376 record_B/op | 657,016 B/op | 3,602 |
| LevelDB save | JSON materialized values | 3,341,825 ns/op | 394,194 record_B/op | 1,149,580 B/op | 6,163 |
| LevelDB save | binary structured values | 1,751,318 ns/op | 79,404 record_B/op | 507,664 B/op | 3,827 |
| LevelDB save | JSON structured values | 2,179,589 ns/op | 175,315 record_B/op | 690,626 B/op | 4,597 |
| LevelDB save | unchanged binary cold refs | 1,353,682 ns/op | 293,376 record_B/op | 673,102 B/op | 4,625 |
| LevelDB save | stats-changed binary cold refs | 1,736,767 ns/op | 293,376 record_B/op | 832,172 B/op | 4,780 |
| LevelDB load | binary materialized values | 2,786,401 ns/op | 293,376 record_B/op | 1,205,047 B/op | 4,706 |
| LevelDB load | JSON materialized values | 4,250,143 ns/op | 394,223 record_B/op | 1,912,317 B/op | 12,386 |
| LevelDB load | binary structured values | 2,933,838 ns/op | 79,404 record_B/op | 983,042 B/op | 6,771 |
| LevelDB load | JSON structured values | 4,685,072 ns/op | 175,336 record_B/op | 1,235,988 B/op | 11,012 |
| LevelDB load | binary cold refs | 2,377,551 ns/op | 293,376 record_B/op | 1,068,327 B/op | 4,706 |

For the benchmark payload, protobuf command wire is about 1.2x faster with a
small byte reduction and equivalent allocation count. Binary journal records are
about 2.3x faster to encode, about 1.5x faster to decode, about 2% smaller, and
use less heap; binary structured journal `values` and `pairs` now always use the
compact tagged value codec instead of measuring and embedding inner JSON. JSON
remains an explicitly configurable, easier-to-inspect journal format. Snapshot
and LevelDB
records omit unrelated null fields before compression, so scalar entries do not
carry empty collection fields. Binary snapshots reuse the compact LevelDB
record codec, avoid base64 for byte values, and use the same tagged collection,
priority-queue, Top-K, radix-tree, XOR-filter, and reservoir-sample payloads.
Bloom filter, Count-Min Sketch, and HyperLogLog snapshots use direct compact
binary codecs. Cuckoo filter fingerprints, roaring bitmap
containers, and sparse-bitset containers store their raw payloads directly, and
Fenwick trees store numeric vectors as varints. Quantile-sketch summaries use
compact binary float/varint tuples.
The gzip-best-binary snapshot default uses about 17% fewer bytes than the
previous gzip-best JSON default for scalar entries and about 17% fewer bytes for
the structured payload; the structured binary path is also about 1.9x faster
than gzip-best JSON in this benchmark. Plain binary snapshots are the lowest CPU
choice and are 37% smaller than plain JSON for scalar entries and 69% smaller
for structured entries. Use `SNAPSHOT_FORMAT=gzip-binary` when lower snapshot
CPU matters more than maximum compression. LevelDB saves stream the sorted trie
against the sorted LevelDB keyspace, skip unchanged records, delete stale
records, and avoid the synced write entirely when the diff batch is empty. The
binary LevelDB format is 26% smaller for scalar records and 55% smaller for the
structured payload, with lower save/load CPU and heap than JSON. Unchanged
cold-reference saves reuse stored record bytes; read-stat changes force a
validated rewrite and cost more CPU and heap. LevelDB loads apply records once
and use a sorted active-key merge for stale deletion instead of an active-key
hash map; cold-reference loads avoid materializing values, saving heap and CPU
when startup can hydrate cold values lazily.
JSON request bodies may also be sent with `Content-Encoding: gzip`.
`GET /api/election` returns node liveness and elected shard leaders.
`GET /api/election?key=...` returns the topology route plus the elected leader
for that key. `POST /api/election` accepts `node` and `online` to record a
heartbeat or mark a node offline. `GET /api/replication` returns the most recent
replication result with timing metadata. `POST /api/replication` accepts
optional `prefix` and reconciles matching local entries with their remote
topology owners.
`GET /api/journal?after_sequence=...&limit=...` returns the command journal tail
when journaling is configured. `POST /api/journal` pulls a remote journal tail
from `source` and applies it locally.
`POST /api/commands` accepts `command`, `key`, optional `value`, `values`,
`batch`, `subkey`, `pairs`,
`priority`, `ttl_seconds`, and `unix_seconds`; it currently
supports `BATCH`, `GET`, `GETSTR`, `EXISTS`, `SET`, `SETSTR`, `SETX`, `SETSTRX`,
`SETINT`, `SETINTX`, `INC`, `DEL`, `TTL`, `EXPIRE`, `EXPIREAT`, `PUTMAP`,
`PEEKMAP`, `TAKEMAP`, `PUSHSLICE`, `POPSLICE`, `SHIFTSLICE`, `HEADSLICE`,
`TAILSLICE`, `ADDSET`, `REMSET`, `HASSET`, `GETSET`, `PUSHPQ`, `PEEKPQ`,
`POPPQ`, `GETPQ`, `CREATEBF`, `ADDBF`, `HASBF`, `INFOBF`, `CREATECF`,
`ADDCF`, `HASCF`, `DELCF`, `INFOCF`, `CREATEXF`, `ADDXF`, `BUILDXF`,
`HASXF`, `INFOXF`, `CREATERB`, `ADDRB`, `REMRB`, `HASRB`,
`COUNTRB`, `GETRB`, `INFORB`, `CREATESB`, `ADDSB`, `REMSB`, `HASSB`,
`COUNTSB`, `GETSB`, `INFOSB`, `CREATERT`, `PUTRT`, `GETRT`, `DELRT`,
`HASRT`, `PREFIXRT`, `INFORT`, `CREATECMS`, `INCRCMS`, `ESTCMS`,
`INFOCMS`, `CREATEHLL`, `ADDHLL`, `COUNTHLL`, `INFOHLL`, `CREATETOPK`,
`ADDTOPK`, `ESTTOPK`, `GETTOPK`, `INFOTOPK`, `CREATERS`, `ADDRS`,
`GETRS`, `INFORS`, `CREATEQ`, `ADDQ`, `ESTQ`, `INFOQ`, `CREATEFW`,
`ADDFW`, `GETFW`, `SUMFW`, `RANGEFW`, `INFOFW`, `DUMP`, `INTERNALSET`,
`INTERNALSETV2`, `INTERNALSETV3`, `INTERNALDEL`, `INTERNALBATCH`,
`INTERNALBATCHV2`, and `INTERNALDIGESTV1`.
`DUMP`,
`INTERNALSET`, `INTERNALSETV2`, `INTERNALSETV3`, and `INTERNALDEL` are low-level replication
primitives. `INTERNALSETV3` moves one key using a keyless binary value encoding;
`INTERNALSETV2` is the binary compatibility fallback whose value repeats the
request key, and `INTERNALSET` is the snapshot-entry JSON fallback.
`INTERNALBATCH` and `INTERNALBATCHV2` batch multiple internal replication
commands and are accepted only for internal replication traffic.
`INTERNALDIGESTV1` is the read-only, topology-scoped digest page used by
anti-entropy and is also accepted only for authenticated internal replication.
`BATCH` is the public pipeline command: send `{"command":"BATCH","batch":[...]}`
with ordinary command requests to reduce client/server round trips. It executes
subcommands in order, returns one response per subcommand in `responses`, and is
not transactional; a failed subcommand does not roll back earlier subcommands.
Internal replication commands are rejected inside public `BATCH` requests.
See [`BENCHMARK.md`](BENCHMARK.md) for benchmarked supported commands, seconds
per 10k operations, raw HAT-trie/Redis/Tarantool output, memory summaries, and
Redis/Tarantool speedup comparisons. The comparison includes single-command
rows, pipelined write rows, and mixed read-heavy/write-heavy workload profiles.

Use the HTTP client CLI against a running monitoring server:

```
make cli ARGS='stats'
make cli ARGS='-timeout 5s health'
make cli ARGS='-timeout 0 journal -pull-from http://leader:8080 -until-current'
make cli ARGS='entries -prefix session:'
make cli ARGS='entries -prefix session: -limit 1000'
make cli ARGS='entries -prefix session: -limit 1000 -after-key session:1000'
make cli ARGS='command -cmd SETSTR -key name -value ivi'
make cli ARGS="command -batch '[{\"command\":\"SETSTR\",\"key\":\"name\",\"value\":\"ivi\"},{\"command\":\"GETSTR\",\"key\":\"name\"}]'"
make cli ARGS='command -cmd INC -key views'
make cli ARGS="command -cmd PUTMAP -key user:1 -pairs '{\"name\":\"ivi\",\"age\":32}'"
make cli ARGS="command -cmd PUSHSLICE -key jobs -values '[\"build\",\"verify\"]'"
make cli ARGS="command -cmd ADDSET -key tags -values '[\"go\",\"cache\"]'"
make cli ARGS='command -cmd PUSHPQ -key jobs -priority 1 -value rebuild'
make cli ARGS='command -cmd POPPQ -key jobs'
make cli ARGS='command -cmd CREATEBF -key seen:emails -value 10000'
make cli ARGS='command -cmd ADDBF -key seen:emails -value user@example.com'
make cli ARGS='command -cmd HASBF -key seen:emails -value user@example.com'
make cli ARGS='command -cmd CREATECF -key active:users -value 10000 -subkey 0.01'
make cli ARGS='command -cmd ADDCF -key active:users -value user-123'
make cli ARGS='command -cmd DELCF -key active:users -value user-123'
make cli ARGS='command -cmd CREATEXF -key allow:domains -value 10000'
make cli ARGS="command -cmd ADDXF -key allow:domains -values '[\"example.com\",\"openai.com\"]'"
make cli ARGS='command -cmd BUILDXF -key allow:domains'
make cli ARGS='command -cmd HASXF -key allow:domains -value openai.com'
make cli ARGS='command -cmd CREATERB -key cohort:ids'
make cli ARGS='command -cmd ADDRB -key cohort:ids -value 65543'
make cli ARGS='command -cmd COUNTRB -key cohort:ids'
make cli ARGS='command -cmd CREATESB -key ids:active64'
make cli ARGS='command -cmd ADDSB -key ids:active64 -value 18446744073709551615'
make cli ARGS='command -cmd COUNTSB -key ids:active64'
make cli ARGS='command -cmd CREATERT -key index:sessions'
make cli ARGS='command -cmd PUTRT -key index:sessions -subkey user:100/profile -value active'
make cli ARGS='command -cmd PREFIXRT -key index:sessions -subkey user:'
make cli ARGS='command -cmd CREATECMS -key freq:paths -value 2048 -subkey 4'
make cli ARGS='command -cmd INCRCMS -key freq:paths -value /api/users -subkey 3'
make cli ARGS='command -cmd ESTCMS -key freq:paths -value /api/users'
make cli ARGS='command -cmd CREATEHLL -key card:visitors -value 14'
make cli ARGS='command -cmd ADDHLL -key card:visitors -value user-123'
make cli ARGS='command -cmd COUNTHLL -key card:visitors'
make cli ARGS='command -cmd CREATETOPK -key top:paths -value 100'
make cli ARGS='command -cmd ADDTOPK -key top:paths -value /api/users -subkey 7'
make cli ARGS='command -cmd GETTOPK -key top:paths'
make cli ARGS='command -cmd CREATERS -key sample:requests -value 128'
make cli ARGS="command -cmd ADDRS -key sample:requests -values '[\"/api/users\",\"/api/cache\"]'"
make cli ARGS='command -cmd GETRS -key sample:requests'
make cli ARGS='command -cmd CREATEQ -key latency:p95 -value 0.01'
make cli ARGS="command -cmd ADDQ -key latency:p95 -values '[10,20,30]'"
make cli ARGS='command -cmd ESTQ -key latency:p95 -value 0.95'
make cli ARGS='command -cmd CREATEFW -key scores:hourly -value 1024'
make cli ARGS='command -cmd ADDFW -key scores:hourly -value 13 -subkey 7'
make cli ARGS='command -cmd SUMFW -key scores:hourly -value 13'
make cli ARGS='command -cmd RANGEFW -key scores:hourly -value 8 -subkey 13'
make cli ARGS='command -cmd DUMP -key tags'
make cli ARGS='topology'
make cli ARGS='topology -key session:1'
make cli ARGS='topology -file data/topology.json'
make cli ARGS='election'
make cli ARGS='election -key session:1'
make cli ARGS='election -heartbeat node-a'
make cli ARGS='election -offline node-a'
make cli ARGS='replication'
make cli ARGS='replication -sync'
make cli ARGS='replication -sync -prefix session:'
make cli ARGS='storage status'
make cli ARGS='storage flush'
make cli ARGS='storage compact'
make cli ARGS='storage compact -start-key session: -limit-key session;'
make cli ARGS='journal -after-sequence 42 -limit 1000'
make cli ARGS='journal -pull-from http://leader:8080 -after-sequence 42 -limit 1000'
make cli ARGS='journal -pull-from http://leader:8080 -after-sequence 42 -limit 1000 -until-current -max-batches 100'
make cli ARGS='snapshot'
```

The CLI `command` subcommand uses `-wire-format auto` by default, which uses
protobuf request/response bodies when the command payload can be represented as
protobuf and falls back to JSON requests for complex values or servers that
reject protobuf request bodies with `415 Unsupported Media Type`. CLI output
remains JSON. Use `-wire-format json` to force the previous JSON
request/response body format or `-wire-format protobuf` to require
protobuf-only request encoding.
All CLI requests use a 30 second timeout by default; pass global
`-timeout 0` before the subcommand to disable it for a deliberate long-running
operation.

Example sharded topology with 1024 virtual buckets:

```
{
  "version": 1,
  "mode": "sharded",
  "bucket_count": 1024,
  "self": "node-a",
  "nodes": [{"id": "node-a"}, {"id": "node-b"}],
  "shards": [{"id": 0, "primary": "node-a"}, {"id": 1, "primary": "node-b"}],
  "bucket_ranges": [{"start": 0, "end": 511, "shard": 0}, {"start": 512, "end": 1023, "shard": 1}]
}
```

Example full-replica topology:

```
{
  "version": 1,
  "mode": "full_replica",
  "self": "node-a",
  "nodes": [{"id": "node-a"}, {"id": "node-b"}]
}
```

The Go wrapper supports key expiration with `Expire`, `ExpireAt`, `Persist`,
and `TTL`. Expired entries are removed lazily when the key is read or mutated.
`TTL` returns `NoTTL` for missing, expired, or persistent keys. Use
`VacuumExpired` for immediate cleanup or `StartExpirationCleaner` for periodic
background cleanup. Use `StartExpirationCleanerContext` when cleaner lifetime
should follow a parent service context. Use `VacuumExpiredOnMemoryPressure` or
`StartMemoryPressureVacuum` to remove expired keys only when heap allocation is
above a configured threshold; `StartMemoryPressureVacuumContext` also stops on
context cancellation. Background cleaners exit cleanly if `Destroy` is called
before their returned stop function. `CompactMemory` explicitly reclaims C trie
and typed-pool high-water capacity after heavy deletion; `StartMemoryCompactor`
and `StartMemoryCompactorContext` provide opt-in periodic execution.

Use `Keys`, `KeysWithPrefix`, `Entries`, and `EntriesWithPrefix` to iterate
over non-expired keys and value metadata. Prefix iteration returns full keys and
supports keys containing NUL bytes.

Ordinary in-memory `Get`, `Exists`, `GetString`, `GetCounter`, and `GetBytes`
calls use concurrent shared locking; exact command `GET` uses the same path for
HTTP/gRPC command traffic. Reads that must remove an expired value or hydrate a
lazy LevelDB reference automatically retry under the exclusive lock.

Strings and byte slices use independent compact backing pools. String writes do
not retain a mirrored byte copy, so ordinary `SETSTR` replacement and
`GetString` are allocation-free. On the 100,000-key 256-byte fixture this makes
insertion 1.26x faster, reduces incremental retained heap 16.08x, and removes
100,052 of 100,080 allocations. Requesting bytes from a string still returns a
caller-owned conversion. See
[BENCHMARK.md](BENCHMARK.md#single-representation-string-storage).

Use `MarshalMapJSON`, `UnmarshalMapJSON`, `UpsertMapJSON`, and `GetMapJSON`
for JSON serialization of Go map values. The JSON decoder preserves numbers as
`json.Number`.

Byte values larger than `DiskBytesThreshold` (64KB) are stored on disk and set
the `HatValue.OnDisk()` flag. `CreateHatTrie` uses an owned temporary spill
directory that is removed by `Destroy`; use `CreateHatTrieWithDiskDir` to supply
a specific directory.

Use `Stats` to read exact cache counters and hit-rate metadata. `StatsForKey`
returns retained per-key read/write counters and last access times without
creating stats for unknown-key misses; its boolean is `false` for both unknown
and currently untracked keys. `ConfigureKeyStats` selects bounded, full, or off
retention, and `KeyStatsPolicy` reports the mode, capacity, and tracked count.
`SaveStats` writes the global statistics snapshot as JSON, and `LoadStats`
restores a saved snapshot.

Use `SaveSnapshot` and `LoadSnapshot` for portable data snapshots.
`SaveSnapshot` writes gzip-best binary by default; use
`SaveSnapshotWithFormat(path, SnapshotFormatGzipBestJSON)` for the previous
storage-optimized JSON layout or `SnapshotFormatJSON` for plain JSON. Snapshot
loads auto-detect gzip, binary, and JSON, replace the current in-memory key set,
skip expired entries, restore per-key access metadata when present, and re-apply
the normal disk spill threshold for large byte values. Snapshot capture
pre-encodes values into compact pages bounded by 1 MiB or 4,096 records and
merges only keys changed before the final journal barrier. This keeps snapshot
output point-in-time consistent without retaining a wide materialized entry for
every key. Restore decodes once into a private generation and publishes it only
after full validation, so readers never observe a partial restore and a failed
restore does not require live-data rollback. Disk-spilled values are staged in
temporary generation directories beneath the configured spill root and old
generation files are removed after cutover. Existing snapshot wire formats and
older files remain compatible.

Use `OpenLevelDBStore`, `SaveLevelDB`, and `LoadLevelDB` for LevelDB-backed
disk persistence. LevelDB loads replace the current in-memory key set. The
full LevelDB writer uses Snappy compression, skips unchanged records, clears
stale keys on each full save, and preserves per-key access metadata.
`LevelDBDirtyTracker` plus `LevelDBStore.SaveDirty` can persist only tracked
dirty keys between full saves. LevelDB writes use
`DefaultStorageFormat` (`StorageFormatBinary`) by default; use
`SaveLevelDBWithFormat(path, StorageFormatJSON)` or
`OpenLevelDBStoreWithFormat(path, StorageFormatJSON)` to keep writing the
previous JSON record layout. Loads auto-detect both binary and JSON records.
`LevelDBStore.Close` is idempotent; operations after close return
`ErrLevelDBStoreClosed`.

Use `NewCacheGRPCServer` and `RegisterCacheGRPCServer` to mount the native gRPC
service in another Go process, or use the generated client in
`internal/gen/hatriecache/v1`. Set `CacheGRPCOptions.AuthToken` or run the
daemon with `MONITORING_AUTH_TOKEN`/`-monitoring-auth-token` to require the same
token used by the HTTP monitoring API; gRPC clients can send either
`authorization: Bearer <token>` or `x-hatrie-auth-token: <token>` metadata.
gRPC command handling can use the same journal, leader-write enforcement, and
HTTP replication options as the monitoring command API. Clients may request
gRPC transfer compression with the standard `gzip` compressor; the server
registers it at the fastest compression level.
`CacheService.CommandStream` is the persistent bidirectional alternative to
the unary `CacheService.Command` RPC. Requests are executed and responses are
returned in stream order with the same authentication, write protection, rate
limits, journal, replication, leader routing, metrics, and audit behavior.
Clients may send and receive concurrently to pipeline commands; use one sender
goroutine and one receiver goroutine per stream and pair responses by order.
`CacheService.CommandBatchStream` accepts explicit `CommandBatchRequest`
envelopes with a caller-provided `batch_id` and returns one ordered
`CommandBatchResponse` with the same ID. Each envelope runs through the native
`BATCH` executor, so compatible scalar commands share one cache lock and
durable journal sync while individual application failures remain in their
matching response positions. A batch of 16 is a measured throughput-oriented
starting point; clients control envelope size up to the normal 4,096-command
`BATCH` limit and should use smaller batches when tail latency matters.
Application-level command failures remain `CommandResponse` values, while
authentication, rate-limit, write-protection, and transport failures close the
stream with their normal gRPC status. The stream is available whenever the
same opt-in native listener is enabled with `GRPC_ADDR`; it starts no additional
server or port. See [BENCHMARK.md](BENCHMARK.md#persistent-grpc-command-stream).

For scalar-heavy clients, `CacheService.ScalarBatchStream` is the lower-CPU,
lower-allocation protobuf interface. Its request is columnar: `operations` and
`keys` have one entry per command, `string_values` has one entry per
`SET_STRING`, and `integer_values` has one entry per `SET_COUNTER` or
`INCREMENT`, each in operation order. Supported operations are `GET`, `EXISTS`,
`SET_STRING`, `SET_COUNTER`, `INCREMENT`, and `DELETE`. Responses return one
status and value kind per command. Byte results share one `values` buffer with
cumulative `value_ends`; integer and boolean results use `integer_values`.
Malformed columns fail only their envelope, while authentication, write
protection, and rate limits retain normal gRPC status errors. When journaling,
dirty persistence, replication, or leader-write enforcement is configured, the
server automatically uses the established transactional side-effect executor;
otherwise it executes the validated scalar columns directly under one trie
lock. Reproduce the comparison with:

```sh
make bench-scalar-batch BIG_WINS_OPS=10000 BENCHTIME=1x COUNT=7
```

For collection-heavy clients, `CacheService.StructuredBatchStream` provides the
same columnar layout for maps, slices, sets, and priority queues. It supports
`PUT_MAP`, `PEEK_MAP`, `TAKE_MAP`, `PUSH_SLICE`, `POP_SLICE`, `SHIFT_SLICE`,
`HEAD_SLICE`, `TAIL_SLICE`, `ADD_SET`, `REMOVE_SET`, `HAS_SET`, `GET_SET`,
`PUSH_PRIORITY`, `PEEK_PRIORITY`, `POP_PRIORITY`, and `GET_PRIORITY`.
`operations` and `keys` are positional; `subkeys`, `values`, and `priorities`
contain only the entries consumed by their corresponding operations, in order.
Each consuming operation accepts one value, so clients should send adjacent
operations for multiple values or use `CommandBatchStream` for the existing
multi-value request shape. Responses reuse scalar status/value-kind enums and
pack byte results into `values` plus cumulative `value_ends`. The server uses
the established side-effect executor whenever journaling, dirty persistence,
replication, or leader enforcement is enabled. Other commands and older clients
retain `CommandBatchStream`. Reproduce the mixed-command comparison with:

```sh
make bench-structured-batch BIG_WINS_OPS=10000 BENCHTIME=1x COUNT=7
```

See [BENCHMARK.md](BENCHMARK.md#compact-typed-protobuf-structured-batches) for
CPU, heap, allocation, and measured wire tradeoffs.
`EntriesRequest.limit` bounds large key listings and returns `has_more` with
`next_after_key`; pass that value as `EntriesRequest.after_key` to read the next
page. Empty keys are valid, so Go clients should set the optional `AfterKey`
field to a pointer to `""` when `has_more` is true and `next_after_key` is empty.
The
`Replication` RPC returns the same last result, timing metadata, and async queue
stats as `GET /api/replication`; set `sync=true` with an optional `prefix` to
run the same anti-entropy sync exposed by `POST /api/replication`. The
`Topology`, `UpdateTopology`, `Election`, and `UpdateElection` RPCs mirror the
HTTP topology/election endpoints for generated clients.

The bundled C HAT-trie tests can be compiled directly with GCC when autotools
build files have not been generated.

## TODO:

- [x] bind [HAT-Trie](https://github.com/luikore/hat-trie) to Go using CGO
- [x] `hat_map<string,int+byte>` stores index or special types (deque/set/etc) to `[][]byte` (aka raws); raws can be serialized using [FlatBuffers](http://github.com/google/flatbuffers) or [FastBinaryEncoding](http://github.com/chronoxor/FastBinaryEncoding)
- [x] add TTL map, check for expiration when read, delete if expired
- [x] use a min-heap expiration schedule so TTL vacuuming does not scan every TTL key
- [x] need benchmark which how much faster: `[][]byte` compared to `map[int][]byte` (~170 bytes overhead)
- [x] replace reusable-index hash maps with a compact bitset-backed stack for typed backing pools
- [x] trim freed backing-pool tail slots so delete-heavy workloads release references
- [x] rebuild and densely reindex trie, typed pools, and Merkle state after heavy churn
- [x] create a web UI for management and monitoring (frontend: Svelte MPA)
- [x] create backend service using HTTP/2 JSON APIs so it can be accessed from another language
- [x] add native gRPC protobuf APIs for strongly typed client generation
- [x] create a client CLI for monitoring stats, key listing, and running commands
- [x] add client CLI support for cache command management:
```		
any type:
  BATCH [command request...]
  SET/SETSTR/SETINT key value
  SETX/SETSTRX/SETINTX key ttl value
  EXISTS/GET/GETSTR/DUMP key
   check the value on the hat_map
  DEL key
  INTERNALSET/INTERNALDEL/INTERNALBATCH key
  TTL
   check if key exists -1 if expired or not exists, >0 if has ttl
  EXPIRE/EXPIREAT key
   make expired
counter type:
  INC key value=1
    maximum of 32-bit integer
map type:
  PUTMAP key subkey val [subkey val]...
  TAKEMAP/PEEKMAP key subkey
slice/arr/stack/queue type:
  PUSHSLICE key val...
  POPSLICE,SHIFTSLICE,HEADSLICE,TAILSLICE key
set type:
  ADDSET,REMSET key val...
  HASSET key val
  GETSET key
priority queue type:
  PUSHPQ key priority val...
  PEEKPQ/POPPQ/GETPQ key
bloom filter type:
  CREATEBF key expected_items [false_positive_rate]
  ADDBF key val...
  HASBF/INFOBF key
cuckoo filter type:
  CREATECF key capacity [false_positive_rate]
  ADDCF,DELCF key val...
  HASCF/INFOCF key
xor filter type:
  CREATEXF key [expected_items]
  ADDXF key val...
  BUILDXF key
  HASXF/INFOXF key
roaring bitmap type:
  CREATERB key
  ADDRB,REMRB key uint32...
  HASRB/COUNTRB/GETRB/INFORB key
sparse bitset type:
  CREATESB key
  ADDSB,REMSB,HASSB key uint64...
  COUNTSB/GETSB/INFOSB key
radix tree type:
  CREATERT key
  PUTRT key subkey value
  GETRT/DELRT/HASRT key subkey
  PREFIXRT/INFORT key [prefix]
count-min sketch type:
  CREATECMS key width [depth]
  INCRCMS key val [count]
  ESTCMS/INFOCMS key
hyperloglog type:
  CREATEHLL key [precision]
  ADDHLL key val...
  COUNTHLL/INFOHLL key
top-k heavy hitter type:
  CREATETOPK key [capacity]
  ADDTOPK key val [count]
  ESTTOPK/GETTOPK/INFOTOPK key
reservoir sample type:
  CREATERS key [capacity]
  ADDRS key val...
  GETRS/INFORS key
quantile sketch type:
  CREATEQ key [epsilon]
  ADDQ key number...
  ESTQ key quantile
  INFOQ key
fenwick tree type:
  CREATEFW key [size]
  ADDFW key index delta
  GETFW/SUMFW key index
  RANGEFW key start end
  INFOFW key
```
- [x] add client CLI support for cluster/server topology management and replication internals:
```
master/leader write, journal, and broadcasting: internalSET key idx value ttl
currenttime+ttl set to an array, and checked every second, execute DEL if expired
the idx is 32-bit integer, 1 bit is for ttl flag, 1 bit if on disk, remaining 6-bit is for special type
master/leader write, journal, and broadcasting: internalDEL key idx
deleted index saved on another map
```
- [x] add option to shard/partition it or full replica, copy tarantool's vbucket/vshard logic
- [x] make sure all read/write operation synchronized, so no stale read/data corruption (in cost of performance)
- [x] check if serializer can support Go's map
- [x] add portable JSON snapshot persistence to disk
- [x] data persisted to disk using lmdb, leveldb, or rocksdb, preferably one with snappy compression
- [x] binary data that are >64KB always stored on disk
- [x] write all pending transaction on journal (backup if program terminated unexpectedly)
- [x] update statistics (last hit, last write, hit rate, cumulative hit rate) to disk
- [x] on-load check for expired snapshot data
- [x] when service starts, non-expired snapshot keys are loaded into memory
- [x] when service start, non-expired keys and (<1KB AND <1h last hit AND >1000 hit rate) values loaded from database to memory
- [x] when service stopped/timer, snapshot data written to disk
- [x] add explicit sync-write force API/CLI
- [x] create iterator command to get all keys and keys based on certain prefix
- [x] create timer vacuum goroutine to clean expired data
- [x] add OOM-triggered vacuum policy
- [x] when master/leader disconnected from all slave, new master/leader elected by remaining slave
- [x] distributed operation via persisted topology, deterministic shard leader
      election, bounded HTTP/protobuf replication, explicit anti-entropy sync,
      and journal pull catch-up; external MQ/raft transports such as emitter.io,
      consul, etcd/raft, or dragonboat can still be evaluated later as alternate
      transports if the built-in topology/replication layer is not enough

## Use cases:

- storing session keys
- counting url hits, likes
- caching 
