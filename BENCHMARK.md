# Benchmark

This compares the cache command surface exposed by `POST /api/commands` and
`make cli ARGS='command ...'` with comparable Redis and Tarantool feature
families. It is a benchmarked feature/command coverage report, not a
wire-protocol compatibility statement.

Sources:

- HAT-trie cache: [`command.go`](command.go), generated gRPC API, and README
  command examples.
- Redis: official command and data type docs at
  <https://redis.io/docs/latest/commands/> and
  <https://redis.io/docs/latest/develop/data-types/>.
- Tarantool: official `box.space` and `box.index` Lua API docs at
  <https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/>
  and
  <https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_index/>.

## Benchmark Results

The comparison tables are split by baseline because the workloads are not the
same kind of process:

- [HAT-trie vs Tarantool](#hat-trie-vs-tarantool) uses embedded Tarantool engine
  calls and 1,000,000 feature cycles, which is 100x the earlier 10,000-cycle
  Tarantool run.
- [HAT-trie vs Redis](#hat-trie-vs-redis) uses Redis' local TCP command path,
  one client, and 10,000 requests per Redis command.
- [HAT-trie Transport Costs](#hat-trie-transport-costs) measures the same
  HAT-trie command families through in-process calls, HTTP JSON, HTTP protobuf,
  and native gRPC so local Redis/Tarantool comparisons can be read alongside
  HAT-trie's own wire overhead.
- [Memory Summary](#memory-summary) reports process/server memory from the same
  local runs.

The speedup columns are `baseline seconds / HAT-trie seconds`. Values above
`1.00x` mean HAT-trie was faster; values below `1.00x` mean the baseline was
faster. HAT-trie command benchmarks are in-process Go calls, Redis includes
loopback TCP/protocol/server dispatch, and Tarantool is embedded Lua calling
the engine directly, so the numbers are useful local comparisons rather than
perfect apples-to-apples microbenchmarks.

Local runs were measured on an AMD Ryzen 9 5950X.

## Run Commands

Large HAT-trie comparable command rows, including the public `BATCH` pipeline
row:

```
make bench-hatrie-command-features HATRIE_COMMAND_BENCH='^BenchmarkCommandFeature/(StringSet|PipelineBatch16|StringGet|CounterInc|TTLExpire|MapPut|MapPeek|MapGet|SlicePushPop|SetAddHas|PriorityQueuePushPop|RoaringAdd|RoaringHas|SparseBitsetAdd|SparseBitsetHas|RadixPut|RadixPrefix|ReplicationDump)$' BENCHTIME=1000000x
```

HAT-trie HyperLogLog rows used by the Redis comparison:

```
make bench-hatrie-command-features HATRIE_COMMAND_BENCH='^BenchmarkCommandFeature/(HyperLogLogAdd|HyperLogLogCount)$' BENCHTIME=1000000x
```

Tarantool 100x larger run:

```
make bench-tarantool-command-features TARANTOOL_REQUESTS=1000000 TARANTOOL_KEYSPACE=10000 TARANTOOL_MEMTX_MEMORY=1073741824
```

Redis 10,000-request network run:

```
make bench-redis-command-features REDIS_START_DOCKER=1 REDIS_PORT=6380 REDIS_REQUESTS=10000 REDIS_CLIENTS=1 REDIS_KEYSPACE=10000 REDIS_PIPELINE=16
```

Pipeline rows are normalized to seconds per 10,000 sub-operations. HAT-trie
uses `BenchmarkCommandFeature/PipelineBatch16` with a public `BATCH` of 16
`SETSTR` commands, Redis uses `redis-benchmark -P 16`, and Tarantool times 16
`space:replace()` calls per loop with `TARANTOOL_PIPELINE=16`.

Mixed profile rows are also normalized to seconds per 10,000 sub-operations.
`MixedReadHeavy100` runs 90 reads, 5 writes, 4 existence checks, and 1 counter
increment per profile cycle. `MixedWriteHeavy100` runs 40 writes, 30 TTL
updates, 20 reads, and 10 counter increments per profile cycle. Redis uses an
`EVAL` profile to keep the mix server-side; Tarantool runs the equivalent loop
inside `scripts/tarantool-command-features.lua`.

Full HAT-trie command benchmark and command support extraction:

```
make bench-command-features BENCHTIME=100x
make command-support
```

The full HAT-trie benchmark includes rows beyond the Redis/Tarantool comparable
tables, such as `BenchmarkCommandFeature/FenwickTreeRange`.

Artifact-based comparison regeneration:

```
make bench-hatrie-command-features BENCHMARK_ARTIFACT_DIR=build/benchmarks BENCHTIME=100x
make bench-redis-command-features BENCHMARK_ARTIFACT_DIR=build/benchmarks REDIS_START_DOCKER=1 REDIS_PORT=6380 REDIS_REQUESTS=10000 REDIS_PIPELINE=16
make bench-tarantool-command-features BENCHMARK_ARTIFACT_DIR=build/benchmarks TARANTOOL_REQUESTS=10000 TARANTOOL_KEYSPACE=10000 TARANTOOL_PIPELINE=16
make bench-command-comparison BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

The artifact directory receives `hatrie-command-features.tsv`,
`redis-command-features.tsv`, `tarantool-command-features.tsv`, matching memory
TSV files, raw Markdown output, and generated
`command-feature-comparison.md`.

HAT-trie end-to-end transport rows:

```
make bench-hatrie-transport-features HATRIE_TRANSPORT_BENCH='^BenchmarkCommandTransportFeature/(InProcess|HTTPJSON|HTTPProtobuf|GRPC|GRPCStream)/(StringSet|StringGet|CounterInc|MapPut|MapPeek)$' BENCHTIME=100x
```

The transport benchmark uses the same command execution semantics as the
monitoring HTTP API and native gRPC API. HTTP protobuf uses
`application/x-protobuf` on `/api/commands`; gRPC uses the generated
`CacheService.Command` or persistent `CacheService.CommandStream` RPC over a
local bufconn listener.

## Architectural Big-Wins Baseline

Run the cross-cutting baseline before and after changes to locking, telemetry,
durability, snapshots, anti-entropy, or command transport:

```sh
make bench-big-wins BIG_WINS_KEYS=100000 BIG_WINS_OPS=100000 BENCHTIME=1x COUNT=3
```

The table records medians from the pre-optimization `b61923b` implementation
on the same AMD Ryzen 9 5950X host. Snapshot pause is the longest observed
latency of a concurrent `GetString` while the 100,000-key snapshot ran.

| Architectural path | Work/op | Baseline median | Primary metric |
| --- | ---: | ---: | --- |
| Concurrent reads | 100,000 reads, 32 logical CPUs | 1,528 ns/read | Contended read latency |
| Per-key memory | 100,000 string keys | 242.5 retained B/key | Post-GC heap delta |
| Durable journal write, serial | 100 writes | 915,191 ns/write | Append plus per-command `fsync` |
| Durable journal write, 16 callers | 100 writes | 878,909 ns/write | Contended append plus per-command `fsync` |
| Snapshot | 100,000 keys | 541,364,799 ns/snapshot | Total snapshot duration |
| Snapshot reader pause | 100,000 keys | 536,817,175 ns | Maximum concurrent read pause |
| Full anti-entropy | 100,000 keys | 1,643 ns/key | Full scan and HTTP transfer |
| Unary gRPC command | 100,000 reads | 64,542 ns/command | Persistent connection, unary RPC |

The combined benchmark process reached 99,824 KiB maximum RSS. These rows are
diagnostic workloads rather than CI thresholds; each optimization section
keeps the same fixture and reports its own before/after ratio.

<a id="final-architecture-improvements"></a>
## Measured Improvement Summary

This is the single summary table for all earlier and final architecture
optimizations with a defensible before/after measurement. Feature names link to
the detailed fixture, run command, raw metrics, and tradeoff discussion.
Reliability, security, operational tooling, and final-only command spot checks
without a comparable baseline remain documented in `IMPROVEMENT_REPORT.md` and
their detailed sections; they are not assigned invented speedup ratios.

| Pass | Implemented improvement | Baseline | Final | Improvement | Main tradeoff |
| --- | --- | ---: | ---: | ---: | --- |
| Earlier | [HTTP protobuf command wire](README.md#serialization-tradeoffs) | JSON: 15,012 ns; 3,185 wire B | Protobuf: 12,637 ns; 3,146 wire B | 1.19x faster, 1.2% smaller wire | Heap is 0.6% higher; complex values retain JSON fallback |
| Earlier | [Binary journal encode](README.md#serialization-tradeoffs) | JSON: 7,800 ns; 3,224 B; 8,496 heap B | Binary: 3,362 ns; 3,159 B; 6,400 heap B | 2.32x faster, 2.0% smaller, 1.33x lower heap | Binary records require project tooling to inspect |
| Earlier | [Binary journal decode](README.md#serialization-tradeoffs) | JSON: 30,034 ns; 22,728 heap B; 29 allocs | Binary: 20,035 ns; 18,071 heap B; 25 allocs | 1.50x faster, 1.26x lower heap | Existing JSON remains a supported fallback |
| Earlier | [Structured binary journal](README.md#serialization-tradeoffs) | JSON: 668 record B; 5,528 ns decode | Binary: 553 record B; 3,539 ns decode | 17.2% smaller, 1.56x faster decode | Encode is 1.56x slower because both representations are size-checked |
| Earlier | [Structured gzip-best snapshot](README.md#serialization-tradeoffs) | Gzip JSON: 18,866,057 ns; 6,956 disk B | Gzip binary: 9,847,768 ns; 5,787 disk B | 1.92x faster, 16.8% smaller, 5.94x fewer allocs | Maximum compression remains CPU-intensive |
| Earlier | [Binary LevelDB scalar records](README.md#serialization-tradeoffs) | JSON save/load: 3,341,825/4,250,143 ns; 394,194 B | Binary: 1,558,684/2,786,401 ns; 293,376 B | Save 2.14x, load 1.53x faster; 25.6% smaller | Binary is less manually inspectable than JSON |
| Earlier | [Binary LevelDB structured records](README.md#serialization-tradeoffs) | JSON save/load: 2,179,589/4,685,072 ns; 175,315 B | Binary: 1,751,318/2,933,838 ns; 79,404 B | Save 1.24x, load 1.60x faster; 54.7% smaller | Some staged structures retain inner JSON fallback |
| Current pass | [Complete tagged structured storage](#complete-tagged-structured-storage), fallback-heavy seven-record cycle | Inner JSON: 3,846/11,651 ns encode/decode; 674 record B | Tagged binary: 2,551/5,743 ns; 410 record B | Encode 1.51x, decode 2.03x faster; 1.64x smaller; 2.42x/1.92x lower heap | Uncommon concrete Go values normalize once to JSON-compatible types; version-1 binary and inner JSON remain readable |
| Current pass | [Delta-only startup persistence](#delta-only-startup-persistence), unchanged Pebble checkpoint with 10k keys | Full generation rewrite: 16.285 ms; 4.64 MB heap; 21,085 allocs | Sequence check: 16.460 us; 12.9 KB heap; 7 allocs | 989x faster, 359x lower heap, 3,012x fewer allocs | Legacy stores and authoritative snapshot replacement perform one full migration save; journal writes pause behind the atomic persistence barrier |
| Current pass | [Generation-based Pebble full save](#pebble-generation-full-save), 10k x 256 B | Legacy Pebble batch: 18.369 ms; 21.05 MB heap; 598.0 disk B/key | Generation SST: 24.651 ms; 9.61 MB heap; 299.6 disk B/key | 2.19x lower heap, 2.00x smaller disk, 10,680x less WAL | Full-save latency is 1.34x higher |
| Current pass | [Native Pebble checkpoint bundles](#pebble-checkpoint-backup-bundles), restore 10k x 256 B | Snapshot: 61.219 ms; 21.35 MB heap; 101,064 allocs | Checkpoint: 83.666 ms; 13.35 MB heap; 62,406 allocs | 1.60x lower heap, 1.62x fewer allocs | Explicit mode: snapshot is 1.37x faster and 1.06x smaller |
| Current pass | [Content-addressed incremental backups](#content-addressed-incremental-backups), 10k x 256 B, 1% changed | Full checkpoint bundle: 98.602 ms; 9.81 MB heap; 2,104,489 written B | Incremental repository: 14.659 ms; 0.94 MB heap; 35,020 written B | 6.73x faster, 10.49x lower heap, 60.09x fewer written bytes | Explicit mode; first backup is full and retained history consumes repository storage |
| Current pass | [Single-pass staged restore](#single-pass-staged-restore), checkpoint 10k x 256 B | Verify then extract: 69.346 ms; 13.18 MB heap; 2 payload passes | Stage, verify, fsync, publish: 56.057 ms; 12.78 MB heap; 1 payload pass | 1.24x faster, 1.03x lower heap, half the payload passes | Repository restore is 1.09x slower because the durable path fsyncs staged files |
| Current pass | [Checkpoint replica bootstrap](#checkpoint-replica-bootstrap), 10k x 256 B | Snapshot: 146.369 ms; 36.66 MB heap; 172,569 allocs; 2,051,371 wire B | Pebble checkpoint: 84.246 ms; 13.50 MB heap; 62,423 allocs; 2,103,717 wire B | 1.74x faster, 2.72x lower heap, 2.76x fewer allocs | Fresh Pebble databases only; wire size is 2.55% larger |
| Current pass | [Incremental existing-replica recovery](#incremental-existing-replica-recovery), 10k x 256 B, 1% changed | Full snapshot: 169.906 ms; 38.68 MB heap; 2,047,776 wire B | Cached repository: 104.739 ms; 36.70 MB heap; 36,313 wire B | 1.62x faster, 1.05x lower heap, 2.26x fewer allocs, 56.39x smaller wire | Pebble and a cached base are required; exact restore still performs a full local DB save |
| Current pass | [Active recovered Pebble generation](#active-recovered-pebble-generation), 10k x 256 B, 1% changed | Incremental recovery plus full local save: 112.016 ms; 34.16 MB heap; 75,000 allocs | Stable-handle checkpoint adoption: 107.014 ms; 27.52 MB heap; 56,080 allocs | 1.05x faster, 1.24x lower heap, 1.34x fewer allocs; full-record rewrite eliminated | Temporarily retains old and staged DB directories; directory publication requires same-filesystem staging |
| Current pass | [Parallel cold-reference hydration](#parallel-cold-reference-hydration), 32 delayed reads | Serialized: 33.875 ms; 18,648 heap B | Parallel singleflight: 1.174 ms; 30,166 heap B | 28.85x faster | Cumulative heap is 1.62x higher and allocations are 1.80x higher |
| Current pass | [Compact lazy-reference slab](#compact-lazy-reference-slab), 100k references | Public-struct slab: 29.617 ms; 90.2 retained B/ref | Compact slab: 20.513 ms; 71.6 retained B/ref | 1.44x faster, 1.26x lower retained heap | Type IDs are internal; exported references are expanded on access |
| Current pass | [Persistent storage backend bakeoff](#persistent-storage-backend-bakeoff), 10k x 256 B plus 1k churn | LevelDB: 91.602 ms cycle; 41.52 MB heap; 265.3 disk B/key | Pebble: 98.273 ms cycle; 20.52 MB heap; 285.7 disk B/key | 2.02x lower cumulative heap; disk is within 1.08x | LevelDB completes the mixed cycle 1.07x faster |
| Earlier | [Replication request batching](#replication-batching-benchmark), 10k keys | Historical: 51,455,645,995 ns; 10,000 requests | First batched baseline: 162,195,812 ns; 1 request | About 317x faster, 10,000x fewer requests | Historical rows came from separate controlled runs |
| Earlier | [Replication routing and encoding](#replication-batching-benchmark), 10k keys | 162,195,812 ns; 144,227 wire B; 57,035,706 heap B | 18,893,092 ns; 55,795 wire B; 948,495 heap B | 8.58x faster, 2.59x smaller wire, 60.13x lower heap | Compact paths retain legacy materialization fallbacks |
| Current pass | [Direct fallback repair collection](#replication-descriptor-optimizations), 10k keys | Buffered entries: 4.939 ms; 1,854,982 heap B; 92 allocs | Direct changes: 4.544 ms; 421,376 heap B; 83 allocs | 1.09x faster, 4.40x lower heap, 1.11x fewer allocs | No measured regression; applies only after an older target rejects digest comparison |
| Current pass | [Direct digest value arena](#replication-descriptor-optimizations), 1,024 sets | Per-value buffers: 1.089 ms; 108,878 heap B; 1,158 allocs | Direct records: 1.050 ms; 87,235 heap B; 136 allocs | 1.04x faster, 1.25x lower heap, 8.51x fewer allocs | No measured wire/CPU regression; JSON and mixed-delete compatibility paths are unchanged |
| Current pass | [Legacy-target capability cache](#replication-descriptor-optimizations), 10k-key full sync | Probe every sync: 26.379 ms; 3 requests; 1,003,978 heap B; 608 allocs | Cached fallback: 13.148 ms; 2 steady requests; 979,718 heap B; 432 allocs | 2.01x faster, 1.02x lower heap, 1.41x fewer allocs | At most 1,024 small entries are retained for five minutes; address or topology changes invalidate them |
| Current pass | [Single-shard replication scan routing](#replication-descriptor-optimizations), 10k routes | Generic routing: 718,882 ns | Direct one-shard scope: 588,964 ns | 1.22x faster; zero heap and allocations in both | Applies only to the default one-shard scan; multi-shard and public routing remain unchanged |
| Current pass | [Prevalidated invariant scan scope](#replication-descriptor-optimizations), 10k-key legacy fallback | Per-key scope validation: 3.418 ms; 421,376 heap B; 83 allocs | One prevalidation: 2.873 ms; 421,376 heap B; 83 allocs | 1.19x faster; reader pause 1.46x shorter; heap and allocations unchanged | Applies only to one-shard, unfiltered leader scans with a known target; all other scans retain dynamic routing |
| Current pass | [Known-legacy Merkle bypass](#replication-descriptor-optimizations), 10k-key full sync | 11.403 ms; 3 steady requests; 1,002,881 heap B; 557 allocs | 11.070 ms; 2 steady requests; 959,672 heap B; 417 allocs | 1.03x faster, 1.04x lower heap, 1.34x fewer allocs | An in-place target upgrade at the same address can wait up to the existing five-minute capability TTL for a Merkle retry |
| Current pass | [Packed fallback batch lookup](#replication-descriptor-optimizations), 10k-key known-legacy full sync | 10.688 ms; 951,616 heap B; 413 allocs; 4.583 ms reader pause | 9.209 ms; 652,898 heap B; 369 allocs; 3.438 ms reader pause | 1.16x faster, 1.46x lower heap, 1.12x fewer allocs, 1.33x shorter reader pause | No measured runtime tradeoff; JSON compatibility and local partitions retain the scalar path |
| Current pass | [Shared-lock fallback key scan](#replication-descriptor-optimizations), 10k-key known-legacy sync under reader load | Exclusive scan: 10.075 ms; 4.121 ms reader pause; 1,064,766 heap B; 394 allocs | Shared scan: 8.966 ms; 0.215 ms reader pause; 1,049,238 heap B; 391 allocs | 1.12x faster, 19.19x shorter reader pause, 1.01x lower heap and allocations | One fixed mutex per trie; TTL, local-partition, snapshot, digest-value, and value-materialization paths remain exclusive |
| Current pass | [Native key-only striped-counter scan](#replication-descriptor-optimizations), 10k keys | Value-copy iterator: 2.612 ms; 19,712 heap B; 43 allocs | Key-only iterator: 2.164 ms; 19,712 heap B; 43 allocs | 1.21x faster with unchanged heap and allocations | Used only by shared fallback scans when opt-in striped counter writes are enabled; value-producing scans are unchanged |
| Current pass | [Epoch-validated scanned-value reuse](#replication-descriptor-optimizations), 10k-key known-legacy sync | Read values after scan: 9.882 ms; 663,319 heap B; 374 allocs; 40 native batches | Reuse scanned values: 8.188 ms; 661,423 heap B; 372 allocs; 0 native batches | 1.21x faster; 1.27x faster focused preparation; reader pause 1.46x shorter | Mutation, TTL, partitions, and striped counters use the prior bounded lookup; the paired mutation control was neutral and allocations were unchanged |
| Current pass | [In-place native radix ordering](#replication-descriptor-optimizations), 10k-key ordered scan | Comparator sort: 3.744 ms; 841,584 heap B; 100 allocs | MSD radix sort: 3.507 ms; 841,584 heap B; 100 allocs | 1.07x faster with unchanged heap and allocations | Uses fixed stack histograms with logarithmically bounded recursion; no per-key sort allocation |
| Current pass | [Request-scoped fallback arena ring](#replication-descriptor-optimizations), 10k keys in ten pages | Fresh arena/page: 12.234 ms; 766,579 heap B; 1,408 allocs | Two reusable arenas: 11.333 ms; 287,987 heap B; 1,369 allocs | 1.08x faster, 2.66x lower heap, 1.03x fewer allocs | At most two page arenas remain live until their HTTP body writers finish; requests, wire bytes, and scan lock boundaries are unchanged |
| Current pass | [Bounded two-page fallback aggregation](#replication-descriptor-optimizations), 10k keys | One page/request: 11.333 ms sender; 4.443 ms receiver; 10.01 requests; 287,987 sender heap B | Two pages/request: 9.699 ms sender; 3.663 ms receiver; 5.01 requests; 218,509 sender heap B | Combined CPU 1.18x faster; 2x fewer requests; sender heap 1.32x lower; 1.84x fewer sender allocs | Largest protobuf grows from 61,156 B to 122,156 B, still 8.6x below the default 1 MiB limit; scan lock pages stay at 1,024 keys |
| Current pass | [Packed batch no-split proof](#replication-descriptor-optimizations), 10k-key known-legacy sync | Exact per-key estimate: 9.738 ms; 215,929 heap B; 744 allocs; 5.003 requests | Aggregate upper bound: 9.020 ms; 206,446 heap B; 744 allocs; 5.003 requests | 1.08x faster, 1.05x lower heap; wire and reader pause neutral | Applies only to complete packed arenas without carried estimates; all other layouts retain the exact splitter |
| Current pass | [Direct single-target digest inventory](#direct-single-target-digest-inventory), 10k-key planning | Target map: 4.479 ms; 19,955 heap B; 58 allocs | Direct inventory: 3.907 ms; 19,643 heap B; 56 allocs | 1.15x faster, 312 fewer heap B, two fewer allocations | Caller selects only a proven sole target; the multi-target map function is unchanged |
| Reverted | [Direct native packed scan](#replication-descriptor-optimizations), 10k-key known-legacy sync | Existing: 9.441 ms; 207,179 heap B; 744 allocs | Direct arena drain: 9.228 ms; 184,267 heap B; 720 allocs | 1.02x faster, 1.12x lower heap, 1.03x fewer allocs | Rolled back; the focused path improved 1.10x, but 2.3% end-to-end CPU did not clear the 5% gate for a new C ABI |
| Current pass | [CLI redirect credential isolation](#cli-redirect-credential-isolation), authenticated request | Bearer token reached cross-origin redirect; 464.75 ns; 904 B; 6 allocs | Token suppressed across origin; 464.45 ns; 904 B; 6 allocs | Security fix with CPU neutral within 0.1% and identical heap/allocations | Same-origin redirects retain authentication; redirected APIs on another origin must authenticate independently |
| Reverted | [Single-pass legacy repair](#replication-descriptor-optimizations), 10k keys | Existing: 11.459 ms; 55,892 wire B; 977,706 heap B; 433 allocs | Unordered: 10.675 ms; 64,258 wire B; sorted: 12.316 ms | Unordered was 1.07x faster but wire was 1.15x larger; sorted was 1.075x slower | Both candidates were rolled back; no runtime tradeoff remains |
| Reverted | [Exact protobuf batch coalescing](#replication-descriptor-optimizations), 10k-key legacy fallback | Two requests: sender 10.422 ms; receiver decode 4.066 ms; largest protobuf 305,156 B | One request: sender 10.215 ms; receiver decode 4.444 ms; largest protobuf 609,046 B | Sender 1.02x faster and 1.44x fewer allocs, but receiver 1.09x slower and combined CPU 1.012x slower | Rolled back; halving requests did not offset receiver decode cost and doubled the largest request |
| Reverted | [Carried compact payload estimates](#replication-descriptor-optimizations), 10k-key scan, preparation, and split | Estimate during split: 4.215 ms | Carry from serialization: 4.230 ms | 0.996x; 0.36% slower with identical allocations | Rolled back; the isolated splitter was 4.37x faster, but moving the estimate made the complete CPU path slower |
| Reverted | [Specialized compact payload estimator](#replication-descriptor-optimizations), 10k-key known-legacy sync | Generic estimator: 8.118 ms; 2.004 requests; 55,880 wire B | Specialized estimator: 8.159 ms; 2.004 requests; 55,878 wire B | 0.995x end to end despite a 1.92x faster focused splitter | Rolled back; no memory, request, or wire gain justified the 0.50% complete-path loss |
| Earlier | [Replication page traversal](#replication-page-traversal), 10 pages | 61,122,327 ns; 1,877,005 heap B; 123,996 allocs | 19,709,083 ns; 999,805 heap B; 11,885 allocs | 3.10x faster, 1.88x lower heap, 10.43x fewer allocs | Mutation invalidates and safely restarts the cursor |
| Earlier | [gRPC replication transport](#replication-transport), 10k keys | HTTP: 44,957,163 ns; 57,479 wire B | gRPC: 37,765,365 ns; 52,006 wire B | 1.19x faster, 9.52% smaller wire, 24.41% fewer allocs | Cumulative heap is 16.18% higher; HTTP remains fallback |
| Earlier | [Bounded gzip writer cache](#replication-compression-tradeoff), 50 syncs | 15.23 MB compressor allocation | 1.14 MB | 13.4x less compressor allocation | Retains at most four initialized writers |
| Earlier | [Four-target replication fanout](#replication-target-fanout) | Serial: 9,544,371 ns | Bound 4: 2,617,552 ns | 3.65x faster | 1.15x cumulative heap and 12 more allocations |
| Earlier | [Journal delta durability](#journal-delta-first-recovery-benchmark), 100 records | Per-command fsync: 0.122684 s | One batch fsync: 0.002170 s | 56.55x faster | Filesystem fsync latency is host/load sensitive |
| Earlier | [Retained journal catch-up](#journal-delta-first-recovery-benchmark) | Exact 10k snapshot: 0.092649 s; 25,709,960 heap B | 100 deltas: 0.002170 s; 163,918 heap B | 42.70x faster, 156.85x lower heap, 5.97x smaller wire | Snapshot remains required after journal compaction gaps |
| Earlier | [Two-value small-set read](#collection-allocation-follow-up) | 155.5 ns; 48 B; 3 allocs | 54.46 ns; 32 B; 1 alloc | 2.86x faster, 1.50x lower heap, 3x fewer allocs | Promotes to a map at three entries |
| Earlier | [Priority queue push+pop](#collection-allocation-follow-up) | 875.9 ns; 56 B; 3 allocs | 769.1 ns; 40 B; 2 allocs | 1.14x faster, 1.40x lower heap | Typed string fast path retains generic fallback |
| Current pass | [Compact priority-queue items](#compact-priority-queue-items), 100k string items | Tagged dual slot: 56.06 retained B/item; 135.2 ns/item build | Tag-free slot: 48.04 retained B/item; 119.2 ns/item build | 1.17x lower retained heap; 1.13x faster build; string churn 1.27x faster | No per-cache or per-item overhead; empty strings use one process-global pre-boxed value; wire and persistence formats are unchanged |
| Current pass | [Direct priority-queue command reads](#compact-priority-queue-items), empty/one/16/100 string items | Public materialization: 214.6/414.2/2,894/25,384 ns; up to 108 allocs | Direct JSON: 54.02/145.5/2,098/18,676 ns; at most 2 allocs | 3.97x/2.85x/1.38x/1.36x faster; up to 2.11x lower heap and 54x fewer allocations | All validated values preserve generic JSON semantics; cold references retain checked hydration; wire, ordering, storage, and ownership are unchanged |
| Current pass | [Direct generic priority-queue GET](#compact-priority-queue-items), empty/one/16/100 string items | Generic materialization: 207.1/422.4/2,863/23,449 ns | Shared-lock direct JSON: 159.3/268.3/2,386/17,977 ns | 1.30x/1.57x/1.20x/1.30x faster; up to 2.11x lower heap and 54x fewer allocations | Other value types retain the prior GET branch; a 100-item mixed queue is 1.37x faster with 1.56x lower heap and 28x fewer allocations |
| Current pass | [Typed priority-queue pop extraction](#compact-priority-queue-items), exact plain-string response | Interface round trip: 49.27 ns; 32 B; 1 alloc | Existing typed accessor: 45.88 ns; 32 B; 1 alloc | 1.07x faster response extraction; heap and allocations unchanged | Exact string `POPPQ` only; empty, escaped, structured, missing, cold-reference, wire, and storage behavior are unchanged |
| Reverted | [Radix-node tag compaction](#radix-node-tag-compaction-rollback), 111,112 nodes | 64 B struct; 115.2 retained B/node; 235.9 ns/key build | 56 B candidate; 102.4 retained B/node; 226.7 ns/key build | Candidate was 1.125x lower retained heap and 1.04x faster to build | Rolled back: pinned string, stored-`nil`, and missing-key reads were 1.10x-1.16x slower; no runtime tradeoff remains |
| Earlier | [Radix prefix scan](#collection-allocation-follow-up) | 3,979 ns; 1,468 B; 20 allocs | 1,972 ns; 1,024 B; 1 alloc | 2.02x faster, 1.43x lower heap, 20x fewer allocs | Escaped/non-string values use generic JSON encoding |
| Current pass | [Allocation-free duplicate radix updates](#idempotent-plain-string-radix-updates), exact plain-string `PUTRT` | 260.6 ns; 16 B; 1 alloc | 207.6 ns; 0 B; 0 allocs | 1.26x faster; allocation eliminated; focused duplicate 2.62x faster | Exact command only; public generic writes are unchanged, while replacements, dynamic builds, and reads are neutral or faster |
| Current pass | [Order-independent radix bulk insertion](#order-independent-radix-bulk-insertion), 64/4,096-entry build and replacement | Sorted builds: 12,681/1,350,624 ns; replacements: 6,840/794,342 ns | Direct builds: 10,566/1,101,969 ns; replacements: 3,058/441,584 ns | Builds 1.20x/1.23x faster; replacements 2.24x/1.80x faster; one allocation and 1,152/65,536 B eliminated per call | No measured tradeoff; exact tree shape, item count, cloning, sorted traversal, snapshots, wire, and storage remain unchanged |
| Current pass | [Borrowed command pair fields](#borrowed-command-pair-fields), pair-only `PUTMAP`/`PUTRT` replacement with 64/4,096 fields | Map: 26,107/2,311,800 ns; radix: 28,813/2,544,023 ns; 11,513/783,543 B | Map: 14,976/1,453,940 ns; radix: 16,328/1,742,736 ns; 2,144/131,181 B | Map 1.74x/1.59x faster; radix 1.76x/1.46x faster; 5.37x/5.97x lower heap; up to 25x fewer allocs | No measured tradeoff; storage still clones all retained values, and mixed subkey-plus-pairs requests retain an owned merge map |
| Current pass | [Flat scalar structured validation](#flat-scalar-structured-validation), pair-only `PUTMAP`/`PUTRT` replacement with 64/4,096 fields | Map: 14,976/1,453,940 ns; radix: 16,328/1,742,736 ns; 2,144/131,181 B | Map: 3,251/247,728 ns; radix: 3,767/525,596 ns; 0 B; 0 allocs | Map 4.61x/5.87x faster; radix 4.33x/3.32x faster; command validation heap eliminated | No measured tradeoff; ordinary nested fallback is 1.02x-1.13x faster and custom marshalers, invalid values, cloning, wire, and storage are unchanged |
| Current pass | [Flat scalar sequence validation](#flat-scalar-sequence-validation), checked slice/priority-queue replacement with 64/4,096 items | Slice: 3,926/198,293 ns; priority queue: 8,049/443,711 ns; 2,200-368,678 B; 3 allocs | Slice: 523.2/42,955 ns; priority queue: 2,791/182,809 ns; 1,152-196,608 B; 1 alloc | Slice 7.50x/4.62x faster; priority queue 2.88x/2.43x faster; 1.85x-2.00x lower heap; 3x fewer allocs | No measured tradeoff; worst-case nested fallback is 1.11x-1.26x faster with lower heap, while acceptance, cloning, wire, and storage are unchanged |
| Current pass | [Single-fallback slice payload validation](#flat-scalar-sequence-validation), checked push with one nested value among 64/4,096 items | 6,432/284,075 ns; 2,764/131,753 B; 6/8 allocs | 2,969/38,259 ns; 1,588/66,179 B; 4/5 allocs | 2.17x/7.43x faster; 1.74x/1.99x lower heap; 1.50x/1.60x fewer allocs | No measured tradeoff; two nested values remain on the exact materialized path and are CPU-neutral with identical heap and allocations |
| Current pass | [Trailing-fallback whole-sequence validation](#flat-scalar-sequence-validation), checked replacement with one trailing nested value among 64/4,096 items | Slice: 2,589/128,450 ns; priority queue: 6,693/411,923 ns; 5 allocs | Slice: 1,835/63,606 ns; priority queue: 4,639/233,791 ns; 4 allocs | Slice 1.41x/2.02x faster; priority queue 1.44x/1.76x faster; one fewer allocation | No measured tradeoff; an earlier non-scalar selects the exact prior whole-sequence path with identical heap and allocations |
| Current pass | [Compact XOR-filter headers](#compact-xor-filter-headers), 100k empty filters | 72-byte header; 72.01 retained B/filter; 51.28 ns/filter initialization | 64-byte header; 64.06 retained B/filter; 34.19 ns/filter initialization | 1.12x lower retained heap; 1.50x faster bulk initialization; same-binary lookup 1.02x faster | Field reorder only; allocations, fingerprints, staged values, behavior, wire, and persistence formats are unchanged |
| Current pass | [Linked XOR-filter build queue](#linked-xor-filter-build-queue), 64/4,096/65,536 items | Slice queue: 4,084 ns/0.339 ms/6.474 ms; 3,680/173,312/2,752,520 B; 4 allocs | Slot-linked queue: 3,944 ns/0.324 ms/6.198 ms; 3,200/152,832/2,424,840 B; 3 allocs | 1.04x-1.05x faster; 1.13x-1.15x lower heap; 1.33x fewer allocs | Uses four existing padding bytes per build slot; fingerprints, retained filters, wire, persistence, and public behavior are unchanged |
| Current pass | [Order-independent XOR-filter build](#order-independent-xor-filter-build), 64/4,096/65,536 staged items | Sorted keys: 7.520 us/0.895 ms/18.136 ms | Direct map order: 4.513 us/0.431 ms/10.071 ms | 1.67x/2.08x/1.80x faster; heap and allocations unchanged | Slot aggregation is commutative and the peel queue is slot ordered; explicit reversed-order tests preserve seed, block length, and fingerprint bytes |
| Current pass | [Compact XOR-filter build hash index](#compact-xor-filter-build-hash-index), 64/4,096/65,536 staged items | String headers: 6,281/371,676/7,918,608 ns; 4,352/218,368/3,473,422 B | Base hashes: 5,675/367,143/7,820,713 ns; 3,200/185,600/2,949,129 B | 1.11x/1.01x/1.01x faster; 1.36x/1.18x/1.18x lower heap; one small-build allocation removed | Uses at most 512 transient stack bytes through 64 items; no retained state or format change; a forced retry is CPU-neutral within 0.4% |
| Current pass | [Adaptive generic XOR batch deduplication](#adaptive-xor-batch-deduplication), one through eight requested values | Per-request map: 275.2/426.0/696.3/1,219 ns for 1/2/4/8 unique values | Direct scalar or pending-slice scan: 245.6/396.5/643.6/1,173.5 ns | 1.04x-1.12x faster; heap and allocations unchanged | Transactional validation and deduplication are unchanged; batches of nine or more retain the map path |
| Current pass | [Inline sparse-bitset containers](#inline-sparse-bitset-containers), 100k singleton containers | Slice-backed: 26.63 ms; 79.60 retained B/container; 0.500 retained objects/container; 100,031 allocs | Two-value inline: 23.49 ms; 71.60 retained B/container; 0.000030 retained objects/container; 31 allocs | 1.13x faster; 1.11x lower retained heap; about 16,667x fewer retained objects; 3,227x fewer allocs | Uses four existing padding bytes; the third value promotes; 4,096-value array and bitmap build/read controls are neutral or faster; formats are unchanged |
| Current pass | [Compact sparse-bitset headers](#compact-sparse-bitset-headers), 100k singleton containers | 64-byte header: 22.061 ms; 71.60 retained B/container; 34.51 MB cumulative heap | 48-byte header: 17.478 ms; 57.75 retained B/container; 27.82 MB cumulative heap | 1.26x faster; 1.24x lower retained and cumulative heap | No measured operation regression; allocations, inline values, fixed bitmap bytes, wire, persistence, and behavior are unchanged |
| Current pass | [Compact Roaring-container headers](#compact-roaring-container-headers), 50k singleton containers | 64-byte header: 14.510 ms; 80.75 retained B/container; 17.47 MB cumulative heap | 48-byte header: 10.762 ms; 66.66 retained B/container; 14.15 MB cumulative heap | 1.35x faster; 1.21x lower retained heap; 1.23x lower cumulative heap | No measured operation regression; the fixed 1,024-word bitmap backing, allocations, wire, persistence, and behavior are unchanged |
| Current pass | [Incremental HyperLogLog estimates](#incremental-hyperloglog-estimates), precision-10 commands and default precision-14 reads | Full register scan: add 3,476 ns; count 3,393 ns; default count 53,283 ns | Derived state: add 251.7 ns; count 231.6 ns; default count 31.73 ns | Commands 13.81x/14.65x faster; default count 1,679x faster; 4,096-value add/count 1.61x faster | Header adds 8 B/filter and the materialized fixture adds 0.050% heap; unexported update-only primitive is 1.06x slower; allocations and formats are unchanged |
| Earlier | [Reservoir sample add](#collection-allocation-follow-up) | 956.7 ns; 168 B; 6 allocs | 465.3 ns; 64 B; 1 alloc | 2.06x faster, 2.63x lower heap, 6x fewer allocs | Fast path applies to plain strings |
| Current pass | [Reservoir sample reads](#reservoir-sample-read-materialization), 16 string items | Generic materialization: 3,910 ns; 2,336 B; 8 allocs | Direct JSON: 2,323 ns; 1,688 B; 3 allocs | 1.68x faster, 1.38x lower heap, 2.67x fewer allocs | All-verbatim strings retain the specialized writer; encoded and structured values now use the same direct response buffer |
| Current pass | [Direct generic reservoir GET](#reservoir-sample-read-materialization), 16/128 string and mixed items | Generic materialization: 2,709/18,349 ns strings; 2,941/18,861 ns mixed | Shared-lock direct JSON: 2,225/17,563 ns strings; 2,701/18,275 ns mixed | 1.03x-1.22x faster; up to 1.23x lower heap; 1.67x-2.00x fewer allocs | Stored state, ordering, JSON bytes, public ownership, wire, and persistent formats are unchanged |
| Current pass | [Multi-item Top-K reads](#multi-item-top-k-read-materialization), 16/default-100 string items | Generic materialization: 2,851/10,558 ns; 2,297/13,516 B; 8 allocs | Direct JSON: 1,851/6,898 ns; 1,624/9,752 B; 3 allocs | 1.54x/1.53x faster, 1.41x/1.39x lower heap, 2.67x fewer allocs | One-item and write implementations are unchanged; structured fallback improves 1.11x |
| Current pass | [Direct generic Top-K GET](#multi-item-top-k-read-materialization), 16/100 string and mixed items | Generic materialization: 2,376/13,936 ns strings; 3,019/13,966 ns mixed | Shared-lock direct JSON: 1,660/10,024 ns strings; 2,148/10,263 ns mixed | 1.36x-1.43x faster; 1.35x-1.53x lower heap; 2.00x-2.20x fewer allocs | Stored state, ordering, JSON bytes, public ownership, wire, and persistent formats are unchanged |
| Current pass | [Generic Top-K encoding outside read lock](#multi-item-top-k-read-materialization), 16/100 string and mixed items | Lock-held direct JSON: 2,023/13,713 ns strings; 2,486/16,561 ns mixed; writer stalled behind caller JSON | Point-in-time copy: 1,988/13,598 ns strings; 2,469/16,036 ns mixed; writer progresses during JSON | 1.01x-1.03x faster serially with identical heap/allocations; unbounded caller-marshaler writer stall removed | Exact generic `GET` only; the dedicated `GETTOPK` candidate was rejected after a repeatable 1.05x CPU regression |
| Reverted | [Generic Top-K slice sorter](#multi-item-top-k-read-materialization), 16/default-100 string items | `sort.Interface`: exact reads 2,240/8,108 ns | `slices.SortFunc`: 2,407/9,099 ns | One allocation and 24 B removed, but CPU 1.07x/1.12x slower | Rolled back; the small transient-memory saving did not justify slower reads |
| Current pass | [Lazy small Top-K indexes](#lazy-small-top-k-indexes), 100k one/two-item sketches | Eager map: 384/464 retained B; 5/7 objects per sketch | Inline: 128/208 retained B; 3/5 objects per sketch | 3.00x/2.23x lower heap; 1.67x/1.40x fewer objects; builds 2.62x/1.94x faster | Third distinct item promotes automatically with unchanged retained heap; complete map-backed commands are neutral or faster |
| Current pass | [Allocation-free inline Top-K duplicates](#lazy-small-top-k-indexes), one/two tracked strings | Quoted-key lookup: 37.06/45.40 ns; 16 B; 1 alloc | Virtual quoted comparison: 12.48/12.94 ns; 0 B; 0 allocs | 2.97x/3.51x faster; all lookup heap removed; complete `ADDTOPK` 1.21x faster with half the allocations | Missing values still allocate their retained canonical key; three/16-item map-backed controls are neutral or 1.01x faster |
| Final architecture | [Per-key telemetry](#per-key-telemetry-modes), 100k keys | 242.5 retained B/key, unbounded | 63.57 retained B/key, off by default | 73.8% lower memory, 3.81x efficiency | `StatsForKey` requires explicit bounded/full opt-in |
| Current pass | [Grouped storage headers](#grouped-storage-headers), empty cache construction | Separate headers: 16,148 ns; 3,360 heap B; 25 allocs | Grouped: 15,320 ns; 3,360 heap B; 8 allocs | 1.05x faster, 3.13x fewer allocations; heap unchanged | Internal constructor only; public storage constructors, typed pointers, command paths, retained values, wire, and persistence are unchanged |
| Current pass | [Deferred optional maps](#deferred-optional-maps), default empty cache | Eager maps: 14,976 ns; 3,360 heap B; 8 allocs | Lazy maps: 14,721 ns; 3,264 heap B; 6 allocs | 1.02x faster, 96 B lower, 1.33x fewer allocations | First TTL is 1.04x faster with one fewer allocation; 10k distinct TTL scheduling is CPU-neutral within 0.2% with unchanged heap |
| Current pass | [Single-representation string storage](#single-representation-string-storage), 100k x 256 B | Mirrored string/bytes: 236.169 ms; 303.5 retained B/key; 100,080 allocs | Dedicated string pool: 187.566 ms; 18.87 retained B/key; 28 allocs | 1.26x faster, 16.08x lower retained heap, 3,574x fewer allocs | String-to-bytes reads materialize the requested clone; wire and storage formats are unchanged |
| Current pass | [Live string-slot replacement](#live-string-slot-replacement), duplicate and changing values | Public `Put`: 3.057/2.731 ns | Proven-live replace: 1.416/1.532 ns | Primitive 2.16x/1.78x faster; complete API 1.015x/1.012x faster | Private cache callers rely on the existing live-index invariant; public deleted-index revival and all formats remain unchanged |
| Current pass | [Packed small-map storage](#packed-small-map-storage), 100k one/two-field maps | Go maps: 354.5 retained B/map; 2.000 retained objects/map; 200,064 timed allocs | Packed pool: 84.00 retained B/map; 0.00025 retained objects/map; 29 timed allocs | 4.22x lower retained heap, about 8,000x fewer retained objects, 6,899x fewer timed allocs | Promotes at the third field with baseline-equivalent heap/allocations; no measured operation, large-map, wire, or persistence regression |
| Current pass | [Allocation-free duplicate packed-map writes](#packed-small-map-storage), exact repeated `PUTMAP` | Generic boxed replacement: 258.2 ns; 16 B; 1 alloc | Typed equality reuse: 198.8 ns; 0 B; 0 allocs | 1.30x faster; allocation eliminated | One/two-field plain-string maps only; actual replacements are 1.07x faster and promoted duplicate/replacement controls are neutral |
| Current pass | [Map field encoding outside cache lock](#packed-small-map-storage), exact `PEEKMAP` | Lock-held field encoding: 34.86 ns strings; 508.4 ns structured; writer stalled behind caller JSON | Point-in-time field reference: 29.68 ns strings; 456.4 ns structured; writer progresses during JSON | 1.17x/1.11x faster with identical zero/three allocations; unbounded caller-marshaler stall removed | Replacing a field may complete while the prior point-in-time response is still encoding; wire bytes and ownership are unchanged |
| Current pass | [Packed small string-set storage](#packed-small-string-set-storage), 100k one/two-member sets | Slice/map entries: 94.36/142.4 retained B/set; 2.000/3.000 retained objects/set | Packed pools: 18.87/36.98 retained B/set; 0.00026/0.00026 retained objects/set | 5.00x/3.85x lower retained heap; about 7,692x/11,538x fewer retained objects; 1.39x/1.42x faster writes | Adds 160 fixed bytes/cache; promotes at the third member with unchanged generic retention and a 1.21x faster measured transition |
| Current pass | [Direct packed string-set JSON](#packed-small-string-set-storage), empty/one/two-member command GET | Temporary set: 245.3/338.3/379.1 ns; 64/88/112 B; 3/4/4 allocs | Direct JSON: 76.35/134.9/153.1 ns; 0/16/16 B; 0/1/1 allocs | 3.21x/2.51x/2.48x faster; up to 7x lower heap; up to 4x fewer allocations | Packed plain strings only; promoted sets retain the generic encoder with unchanged wire, storage, ordering, and ownership |
| Current pass | [Direct promoted-set JSON](#packed-small-string-set-storage), 3/16 strings and four mixed values | Sorted keys plus cloned set: 582.0/2,016/1,385 ns; 184/760/752 B; 5/5/10 allocs | Sorted keys plus direct values: 355.6/1,637/1,056 ns; 72/448/264 B; 2/2/4 allocs | 1.23x-1.64x faster; 1.70x-2.85x lower heap; 2.50x fewer allocs | Required deterministic key sorting remains; packed/public reads, writes, wire, storage, and persistent formats are unchanged |
| Current pass | [Packed small-slice storage](#packed-small-slice-storage), 100k zero/one/two-value slices | Deques: 46.23/62.23/78.23 retained B/slice; one retained object for nonempty slices | Packed pools: 27.39/27.39/46.23 retained B/slice; 0.00025 retained objects/slice | 1.69x/2.27x/1.69x lower retained heap; about 4,000x fewer retained objects for nonempty slices; tiny push retention improves up to 4.02x | Adds 160 fixed bytes/cache; promotion retains the generic deque, measures neutral, and halves transition allocations |
| Current pass | [Direct packed-slice JSON](#packed-small-slice-storage), nil/empty/one/two-value command GET | Temporary slice: 222.8/253.2/345.5/376.1 ns | Direct JSON: 80.56/80.05/141.7/150.9 ns | 2.77x/3.16x/2.44x/2.49x faster; nil/empty become allocation-free; nested values improve 1.55x | Negative packed indexes only; promoted-deque follow-up is reported separately; wire bytes, storage, ordering, and ownership are unchanged |
| Current pass | [Direct promoted-slice JSON](#packed-small-slice-storage), 3/16 strings and four mixed values | Temporary clone: 513.4/1,153/1,250 ns; 136/504/688 B; 4/4/9 allocs | Ring-order writer: 167.4/491.9/750.4 ns; 24/192/264 B; 1/1/4 allocs | 1.67x-3.07x faster; 2.61x-5.67x lower heap; 2.25x-4.00x fewer allocs | No measured tradeoff; public cloning, lock scope, writes, wire, storage, and persistent formats are unchanged |
| Reverted | [Packed-string compaction](#string-compaction-allocation-rollback), 100k varied 33-512 B strings | Packed copy: 30.07 MB cumulative heap; 121,848 KiB peak RSS | Dense remap: 2.81 MB cumulative heap; 93,516 KiB peak RSS | 10.71x lower cumulative heap, 1.30x lower peak RSS | Retains 3.79% more heap and forced GC is 1.81x slower; packing was not worth its immediate memory spike |
| Reverted | [Online generational compaction](#online-generational-compaction-rollback), 100k insert/90k delete | Exclusive rebuild: 10.258 ms reader pause; 0.91 MB heap | Staged generation: 1.091 ms reader pause; 6.18 MB heap | 9.40x shorter pause; 13.17x lower retained backing; 5.36x lower retained heap | Rolled back: total compaction was 1.54x slower, transient heap 6.80x higher, and allocations 2.67x higher |
| Current pass | [Atomic cache-wide telemetry](#atomic-cache-wide-telemetry), 32 readers | 222.0 ns/read | 93.21 ns/read | 2.38x faster | Adds 64 fixed bytes/cache; detailed key telemetry retains its mutex |
| Current pass | [Lazy rate-limiter shard maps](#lazy-rate-limiter-shard-maps), constructor plus first client | Eager maps: 2,683.5 ns; 4,640 heap B; 66 allocs | First-shard allocation: 414.9 ns; 1,616 heap B; 3 allocs | 6.47x faster, 2.87x lower heap, 22x fewer allocations | Each shard allocates its map on first use; steady-state admission is slightly faster, and activating all 64 shards is CPU-neutral within 0.2% with identical memory |
| Final architecture | [Concurrent scalar reads](#concurrent-scalar-read-fast-path), 32 CPUs | 1,528 ns/read | 632.4 ns/read | 2.42x faster | Expiration cleanup and LevelDB hydration still take the exclusive path |
| Final architecture | [Striped existing-counter writes](#striped-existing-counter-writes), 2 writers | 362.8 ns/write | 209.7 ns/write | 1.73x faster | Opt-in; 64 stripes retain 1,536 B and semantic writes fall back |
| Current pass | [Local HAT-trie partitions](#local-hat-trie-partitions), 100k writes, 16 workers | One trie: 29.147 ms; 291.5 ns/write | 16 tries: 12.992 ms; 129.9 ns/write | 2.24x faster, 1.84x lower timed heap, 1.73x fewer timed allocs | Opt-in; separate-process maximum RSS is 1.05x higher and whole-keyspace operations merge partitions |
| Current pass | [Partition-parallel whole-keyspace scans](#partition-parallel-whole-keyspace), 100k keys, 16 partitions | Serial merge: keys 36.990 ms/18.43 MB; entries 49.800 ms/24.50 MB | Parallel k-way merge: keys 8.722 ms/12.21 MB; entries 9.346 ms/15.62 MB | Keys 4.24x faster/1.51x lower heap; entries 5.33x faster/1.57x lower heap | Opt-in partition mode only; worker coordination adds 13/9 allocations |
| Current pass | [Persistent partition replication cursors](#persistent-partition-replication-cursors), 100k keys, 100 pages | Full materialize/sort per page: 1.076 s; 1.394 GB heap; 10,069,862 allocs | 16 retained cursors plus k-way heap: 56.721 ms; 9.78 MB heap; 300,538 allocs | 18.97x faster, 142.52x lower heap, 33.51x fewer allocs | Cursor restarts after a partition mutation; local partitions remain opt-in |
| Current pass | [Packed internal scan arenas](#packed-internal-scan-arenas), 100k keys, 100 pages | Prior persistent cursor: 56.721 ms; 9.78 MB heap; 300,538 allocs | Reusable arenas plus typed heap: 49.752 ms; 0.357 MB heap; 669 allocs | 1.14x faster, 27.41x lower heap, 449.23x fewer allocs | Borrowed keys are internal-only; durable scans retain one immutable arena per 256-key batch |
| Final architecture | [Durable journal group commit](#durable-journal-group-commit), 16 callers | 878,909 ns/write | 73,286 ns/write | 11.99x faster | Sparse traffic can opt into a collection window; durability still precedes apply/ack |
| Current pass | [Durable public batches](#durable-public-batches), 10k writes | 9.821 s; 10,000 syncs | 29.051 ms; 3 syncs | 338x faster, 3,333x fewer syncs | Cumulative heap is 1.20x higher; ordinary item errors remain non-transactional |
| Current pass | [Native C command batching](#native-c-command-batching), 4,096 commands | Go loop: set 1.137 ms, get 1.123 ms | One C call: set 0.998 ms, get 0.979 ms | Set 1.14x faster, get 1.15x faster | Activates at 32 same-family commands; state-sensitive batches fall back |
| Current pass | [Exact batch telemetry aggregation](#exact-batch-telemetry-aggregation), 4,096 native reads and 16/256 direct scalar reads | Per-item clocks: 1.012 ms; 3.903/55.274 us | One batch clock: 0.857 ms; 3.328/47.116 us | 1.18x/1.17x/1.17x faster; heap and allocations unchanged | Default global telemetry becomes visible at batch completion; explicit per-key telemetry retains per-item updates |
| Current pass | [Adaptive typed scalar execution](#adaptive-typed-scalar-execution), 16 distinct reads/mixed commands/repeated reads | Go loop: 3,320/4,006/885.1 ns; 736/608/736 B; 12/20/12 allocs | Native/coalesced: 2,438/3,050/396.5 ns; 736/592/512 B; 12/17/5 allocs | 1.36x/1.31x/2.23x faster; mixed/repeated heap and allocations also lower | Native starts at four commands and retains bounded reusable scratch; size-two, TTL, cold-reference, and intercepted batches keep the prior path |
| Current pass | [Adaptive native bucket size classes](#adaptive-native-bucket-size-classes), 100k insert plus 50% delete/reinsert | Exact resize: 24.458/21.053 ms insert/churn; 200,000 resizes | One-record reserve: 23.711/17.460 ms; 58,925 resizes | Insert 1.03x, churn 1.21x faster; 3.39x fewer resizes | Slot capacity is 7.0% higher; isolated RSS +4.5%, full-cache RSS +0.7% |
| Current pass | [Compact typed protobuf scalar batches](#compact-typed-protobuf-scalar-batches), 10k GET, batch 16 | Generic batch: 8.657 ms; 9.67 MB heap; 37.04 wire B/command | Scalar batch: 3.911 ms; 2.63 MB heap; 23.72 wire B/command | 2.21x faster, 3.67x lower heap, 2.66x fewer allocs, 1.56x smaller wire | Supports six scalar operations; other command families retain typed structured or generic batches |
| Current pass | [Shared scalar-batch keys](#shared-scalar-batch-keys), 10k same-key GET, batch 16 | Repeated key column: 394.3 ns/command; 2,368,864 heap B; 34,823 allocs; 23.72 wire B/command | One shared key: 316.1 ns/command; 1,684,878 heap B; 23,020 allocs; 11.54 wire B/command | 1.25x faster, 1.41x lower heap, 1.51x fewer allocs, 2.06x smaller wire | Additive request form; mixed-version clients retry expanded keys after an older server's column-count error |
| Current pass | [Compact typed protobuf structured batches](#compact-typed-protobuf-structured-batches), 10k mixed commands, batch 16 | Generic batch: 27.743 ms; 10.61 MB heap; 60.41 wire B/command | Structured batch: 19.909 ms; 3.59 MB heap; 33.22 wire B/command | 1.39x faster, 2.96x lower heap, 1.54x fewer allocs, 1.82x smaller wire | One value per mutating operation; multi-value and unsupported command families retain the generic batch path |
| Current pass | [Bounded structured batch execution](#bounded-structured-batch-execution), 10k mixed commands, batch 16 | Per-command dispatch: 1,724 ns/command; 3,586,784 heap B; 77,681 allocs | Four-command executor: 1,503 ns/command; 3,587,480 heap B; 77,686 allocs | 1.15x faster; heap and allocations effectively unchanged; wire unchanged | Default telemetry and unpartitioned local execution only; all compatibility cases retain the command loop |
| Current pass | [Shared structured-batch keys](#shared-structured-batch-keys), 10k same-key `PEEK_MAP`, batch 16 | Repeated key column: 805.1 ns/command; 3,362,504 heap B; 64,490 allocs; 36.72 wire B/command | One shared key: 718.6 ns/command; 2,636,371 heap B; 53,186 allocs; 18.91 wire B/command | 1.12x faster, 1.28x lower heap, 1.21x fewer allocs, 1.94x smaller wire | Additive request form; mixed-version clients retry expanded keys after an older server's column-count error |
| Current pass | [Shared structured-batch subkeys](#shared-structured-batch-subkeys), 10k same-field `PEEK_MAP`, batch 16 | Shared key plus repeated subkeys: 721.8 ns/command; 2,636,371 heap B; 53,186 allocs; 18.91 wire B/command | One shared key and subkey: 670.1 ns/command; 2,305,907 heap B; 41,915 allocs; 12.35 wire B/command | 1.08x faster, 1.14x lower heap, 1.27x fewer allocs, 1.53x smaller wire | Additive request form; mixed-version clients retry expanded subkeys after an older server's column-count error |
| Current pass | [Shared structured-batch values](#shared-structured-batch-values), 10k same-member `HAS_SET`, batch 16 | Shared key plus repeated values: 721.6 ns/command; 2,731,184 heap B; 60,051 allocs; 15.16 wire B/command | One shared key and value: 654.4 ns/command; 2,305,612 heap B; 48,777 allocs; 7.662 wire B/command | 1.10x faster, 1.18x lower heap, 1.23x fewer allocs, 1.98x smaller wire | Additive request form; mixed-version clients retry expanded values after an older server's column-count error |
| Current pass | [Go 1.26.5 toolchain refresh](#go-1265-toolchain-refresh), direct command operations | Go 1.26.4 set/get/inc/TTL: 192.9/168.6/243.1/227.5 ns | Go 1.26.5: 182.3/164.8/239.0/229.9 ns | 1.06x/1.02x/1.02x faster; TTL 1.01x slower; heap and allocations unchanged | Minimum supported Go version and Docker builder become 1.26.5 |
| Current pass | [Latest fastime refresh](#latest-fastime-refresh), Go 1.26.5 direct commands | v1.1.9 normalized fastime advantage, set/get/inc/TTL: 1.18x/1.26x/1.15x/1.58x | v1.1.10: 1.16x/1.27x/1.17x/1.68x | Set advantage 1.02x lower; get effectively unchanged; increment 1.02x and TTL 1.06x higher; heap unchanged | Retains latest typed-atomic and daemon-cancellation fixes; absolute medians are reported below because process speed varied |
| Current pass | [Cached default trie clock](#cached-default-trie-clock), direct command operations | `time.Now`: set/get/inc/TTL 228.3/210.8/273.1/365.2 ns | `fastime.Now`: 177.6/162.9/226.5/240.8 ns | 1.29x/1.29x/1.21x/1.52x faster; heap and allocations unchanged | Default clock has a 5 ms refresh cadence without a hard scheduler-lag bound; injected test clocks and monotonic elapsed measurements are unchanged |
| Reverted | [Exact scalar mutation dispatch](#exact-scalar-mutation-dispatch), nine alternating binaries | Generic set/get/inc/expire: 200.2/181.1/277.3/238.1 ns | Exact helper: 203.8/185.9/272.4/239.2 ns | INC 1.02x faster; SET 1.02x, GET 1.03x, and TTL 1.005x slower | Rolled back; unchanged heap, allocations, wire, and storage did not justify regressions in three of four complete command paths |
| Current pass | [Segmented WAL compaction](#segmented-wal-compaction), 100k records | 31.462 ms; 20,810,464 heap B; 500,033 allocs | 1.845 ms; 22,256 heap B; 56 allocs | 17.06x faster, 935x lower heap, 8,929x fewer allocs | Retains bounded sidecar files; rotation adds directory metadata syncs |
| Current pass | [Binary journal catch-up wire](#binary-journal-catch-up-wire), 10k `SETINT` records | JSON: 6.182 ms; 11,178,528 heap B; 10,042 allocs; 808,943 wire B | Binary: 1.197 ms; 2,383,920 heap B; 4 allocs; 289,886 wire B | 5.16x faster, 4.69x lower heap, 2,510x fewer allocs, 2.79x smaller wire | JSON remains configurable and is negotiated as an old-source fallback |
| Current pass | [Selective journal wire ownership](#selective-journal-wire-ownership), 10k binary `SETINT` records | Clone all fields: 0.956 ms; 2,216,240 heap B; 20,003 allocs | Borrow through apply: 0.696 ms; 2,056,240 heap B; 3 allocs | 1.37x faster, 1.08x lower heap, 6,667.67x fewer allocs | Stored strings and potentially retained keys are still cloned |
| Current pass | [Compact scalar journal tails](#compact-scalar-journal-tails), 10k binary `SETINT` records | Full requests: 8.074 ms durable; 2,720,000 heap B | 48-byte records: 5.864 ms durable; 1,442,048 heap B | 1.38x faster, 1.89x lower heap | Six allocations, 349,886 wire B, one fsync, and structured fallback are unchanged |
| Current pass | [Bounded WAL staging arena](#bounded-wal-staging-arena), 10k compact `SETINT` records | 1 MiB limit: 5.705 ms; 598,640 heap B; 1 write | 128 KiB limit: 5.416 ms; 172,656 heap B; 4 writes | 1.05x faster, 3.47x lower staging heap | More `write` syscalls before the same single final fsync; oversized records remain valid |
| Current pass | [Coalesced journal batch append](#coalesced-journal-batch-append), 10k pulled `SETINT` records | Per-record WAL writes: 20.935 ms; 1,686,384 heap B; 30,004 allocs | Bounded shared append: 7.364 ms; 606,832 heap B; 5 allocs | 2.84x faster, 2.78x lower heap, 6,001x fewer allocs | WAL bytes and one-final-fsync durability are unchanged; buffers flush in bounded chunks |
| Current pass | [Single-lock journal scalar apply](#single-lock-journal-scalar-apply), 10k pulled `SETINT` records | Serial apply: 4.189 ms CPU; 8.907 ms durable | One-lock run: 2.603 ms CPU; 7.744 ms durable | 1.61x apply CPU; 1.15x durable | Heap, allocations, WAL bytes, and fsync durability are unchanged; unsupported runs stay serial |
| Final architecture | [Point-in-time snapshot capture](#point-in-time-snapshot-capture), 100k keys | 528,624,130 ns maximum read pause | 142,374,086 ns | 3.71x shorter pause | Total snapshot time is 5.5% higher and cumulative heap is 2.63x higher |
| Current pass | [Bounded-page snapshot capture](#bounded-page-snapshot-capture), 100k keys | 61.740 ms maximum read pause | 2.822 ms | 21.88x shorter pause | Total time and heap remain within 1% |
| Current pass | [Bounded partition snapshot locking](#bounded-partition-snapshot-locking), 100k keys, 16 partitions | Whole-set lock: 154.398 ms pause; 241.262 ms total | Tracked 256-key pages: 2.300 ms pause; 259.974 ms total | 67.14x shorter pause | Total time is 7.8% higher and cumulative heap is 1.7% higher |
| Current pass | [Parallel partition restore](#parallel-partition-restore), 100k x 256 B, 16 partitions | Serial: snapshot 258.183 ms; Pebble 213.948 ms | Bounded parallel: snapshot 202.398 ms; Pebble 181.435 ms | 1.28x faster snapshot restore; 1.18x faster Pebble startup | Heap and allocations rise by at most 0.1%; local partitions must be enabled |
| Current pass | [Atomic generation snapshot restore](#atomic-generation-snapshot-restore), 100k x 256 B | Two-pass live mutation: 385.364 ms; 217.69 MB heap; 901,188 allocs | One-pass staged swap: 234.900 ms; 108.82 MB heap; 500,117 allocs | 1.64x faster, 2.00x lower heap, 1.80x fewer allocs | Restore temporarily retains old and staged generations; measured cutover is 1.72 us |
| Current pass | [Compact streaming snapshot capture](#compact-streaming-snapshot-capture), 100k keys | 182.221 ms; 47.61 MB heap; 97,152 KiB RSS | 151.348 ms; 24.57 MB heap; 63,104 KiB RSS | 1.20x faster, 1.94x lower heap, 1.54x lower RSS | Median maximum read pause is 7.9% higher at 3.24 ms |
| Current pass | [Selective snapshot mutation maps](#selective-snapshot-mutation-maps), no-mutation tracking cycle | Eager reset/replacements: 177.4 ns; 160 heap B; 4 allocs | Allocate only after mutation: 65.44 ns; 64 heap B; 2 allocs | 2.71x faster, 2.50x lower heap, 2x fewer allocations | Initial dirty map stays eager so first concurrent mutation retains prior latency; one-mutation CPU is neutral within 0.6% |
| Current pass | [Delete-churn memory compaction](#delete-churn-memory-compaction), 100k insert/90k delete | 9,679,075 retained backing B; 9,850,096 retained heap B | 704,912 retained backing B; 884,600 retained heap B | 13.73x lower backing, 11.13x lower heap | One rebuild pauses access for 8.80 ms and adds 2.4% cumulative allocation to the full churn cycle |
| Current pass | [Single-pass expiration-index compaction](#single-pass-expiration-index-compaction), 10k expiring keys | Double map rebuild: 8.254 ms; 1,562,256 heap B; 10,095 allocs | Heap-authoritative rebuild: 6.120 ms; 1,125,320 heap B; 10,060 allocs | 1.35x faster, 1.39x lower heap, 35 fewer allocations | No measured tradeoff; `CompactMemory` policy, lock scope, TTL state, heap order, wire, and persistence are unchanged |
| Current pass | [Linear expiration-index rebuild](#linear-expiration-index-rebuild), repeated 10k-TTL compaction | Heap `Push`: 6.033 ms; 1,125,278 heap B; 10,058 allocs | Clone plus direct positions: 5.964 ms; 1,125,278 heap B; 10,058 allocs | 1.01x faster with identical heap and allocations | No measured tradeoff; the right-sized heap, exact order, deadlines, index positions, and formats are unchanged |
| Current pass | [Validated bounded key-stat compaction](#validated-bounded-key-stat-compaction), 100k tracked keys | Unconditional seen map: 169.162 ms; 11,405,896 heap B; 100,577 allocs | Validated slots: 166.164 ms; 7,910,752 heap B; 100,317 allocs | 1.02x faster, 1.44x lower heap, about 260 fewer allocations | Inconsistent internal slot metadata retains the prior repair fallback; policy, eviction order, stats, and formats are unchanged |
| Current pass | [Indexed expiration heap](#indexed-expiration-heap), 100k deadline updates on one key | 250.0 ns/update; 91 B/op; 19 final heap nodes | 194.8 ns/update; 0 B/op; 1 heap node | 1.28x faster; cumulative allocation eliminated; 19x fewer final nodes | Heap index is `uint32`, limiting simultaneously scheduled TTL keys to practical in-memory sizes |
| Final architecture | [Equal-state anti-entropy](#incremental-anti-entropy), 10k x 1 KiB | 154,735,234 ns; 10,743,774 wire B | 22,129,470 ns; 215 wire B | 6.99x faster, 49,971x smaller wire | Equality still scans and hashes both replicas |
| Final architecture | [1%-changed anti-entropy](#incremental-anti-entropy), 10k x 1 KiB | Same full-transfer baseline | 72,812,784 ns; 240,086 wire B | 2.13x faster, 44.75x smaller wire | Digest pages add metadata before changed values |
| Current pass | [Merkle equal-state preflight](#hierarchical-merkle-anti-entropy), 10k x 1 KiB | Digest: 18.272 ms; 560,720 heap B | Merkle: 0.993 ms; 233,744 heap B | 18.40x faster, 2.40x lower heap | First activation builds a 29.60 B/key index |
| Current pass | [Merkle 1%-changed repair](#hierarchical-merkle-anti-entropy), 10k x 1 KiB | Digest: 55.401 ms; 240,086 wire B | Merkle: 25.443 ms; 132,820 wire B | 2.18x faster, 1.81x smaller wire | Active write tracking is 1.88x slower |
| Current pass | [Deferred Merkle maintenance](#hierarchical-merkle-anti-entropy), 100k writes plus root | Immediate update: 45.523 ms; 323,840 heap B | Coalesced/rebuild: 25.807 ms; 1,006,632 heap B | 1.76x faster cycle; active writes 2.04x faster | Root after broad churn is 6.00x slower; cycle heap is 3.11x higher |
| Current pass | [Lazy empty Merkle table backing](#lazy-empty-merkle-table-backing), empty index allocation | Eager: 6,497 ns; 35,840 heap B; 4 allocs; 33,792 retained B | Lazy: 3,122 ns; 18,432 heap B; 1 alloc; 16,384 retained B | 2.08x faster, 1.94x lower heap, 4x fewer allocations, 2.06x lower retention | First 10k-key build has identical heap/allocations and paired CPU within 1.3%; nonempty layout and lookup are unchanged |
| Current pass | [Stateless empty Merkle root](#stateless-empty-merkle-root), repeated empty snapshot activation | Lazy index: 17,186 ns; 33,424 heap B; 4 allocs; 16,384 retained B | Fixed root: 878.9 ns; 0 heap/allocs/retained B | 19.56x faster; all Merkle allocation and retention eliminated | First nonempty rebuild adds one native size read; paired 10k rebuild CPU is neutral within 0.5% with unchanged heap/allocations |
| Final architecture | [Sequential gRPC stream](#persistent-grpc-command-stream), 10k commands | Unary: 59,040 ns/command | 14,914 ns/command | 3.96x faster, 6.73x lower heap | Request/response remains sequential |
| Final architecture | [Pipelined gRPC stream](#persistent-grpc-command-stream), 10k commands | Unary: 59,040 ns/command | 3,118 ns/command | 18.94x faster, 7.67x lower heap, 6.57x fewer allocations | Requires concurrent sender/receiver with ordered response pairing |
| Current pass | [Native gRPC batch stream](#persistent-grpc-command-stream), 10k commands, batch 16 | Pipelined: 2,638 ns/command; 41.00 wire B/command | Native batch: 1,161 ns/command; 37.04 wire B/command | 2.27x faster, 1.62x lower heap, 2.77x fewer allocations, 1.11x smaller wire, 16x fewer messages | Batching can add queueing latency; client chooses envelope size |
| Current pass | [Pipelined live gRPC replication](#pipelined-live-grpc-replication), 10k writes | HTTP: 178.079 ms; 1,868,894 wire B | gRPC: 167.797 ms; 1,081,746 wire B | 1.06x faster, 1.73x smaller wire | Requires native gRPC listener; HTTP remains fallback |
| Current pass | [Live gRPC micro-batching](#pipelined-live-grpc-replication), 10k writes | 193.299 ms; 10,000 batches; 1,081,747 wire B | 149.682 ms; 2,910 batches; 368,252 wire B | 1.29x faster, 3.44x fewer batches, 2.94x smaller wire | One-caller throughput is 1.6% lower; set max commands to 1 for legacy behavior |
| Current pass | [Allocate-after-grouping live gRPC](#pipelined-live-grpc-replication), 10k writes | 154.265 ms; 2,959 batches; 353.63 MB heap; 2,037,671 allocs | 126.893 ms; 2,305 batches; 303.23 MB heap; 940,900 allocs | 1.22x faster, 1.28x fewer batches, 1.17x lower heap, 2.17x fewer allocs | Topology updates compute their fingerprint once; requests retain payload references until ack |
| Current pass | [Lazy gRPC session maps](#lazy-grpc-session-maps), unused create-and-close lifecycle | Eager live/sync: 170.3/175.7 ns; 208 heap B; 4 allocs | Lazy live/sync: 57.10/57.88 ns; 64 heap B; 1 alloc | 2.98x-3.04x faster, 3.25x lower heap, 4x fewer allocations | First successful stream and actual sticky fallback allocate their required maps; live sessions never allocate sticky fallback state |
| Current pass | [Direct single-target gRPC sync dispatch](#direct-single-target-grpc-sync-dispatch), one task group | Generic grouping: 569.1 ns; 808 heap B; 8 allocs | Direct result slot: 426.8 ns; 384 heap B; 4 allocs | 1.33x faster, 2.10x lower heap, 2x fewer allocations | Applies only to exactly one group; repeated-target and multi-target controls retain identical heap/allocations and CPU within 1.1% |
| Current pass | [Normalized topology-store routing](#normalized-topology-store-routing), one/four shards | Clone/sort all shards: 241.4/505.3 ns; 120/488 heap B; 4/9 allocs | Clone selected shard: 134.3/142.55 ns; 48/80 heap B; 2/2 allocs | 1.80x/3.54x faster; 2.50x/6.10x lower heap; 2x/4.50x fewer allocations | Store topology is already normalized; returned route ownership and generic routing for arbitrary topology values are unchanged |
| Current pass | [Direct election-key routing](#direct-election-key-routing), healthy one/four shards | Topology snapshot plus active map: 643.95/1,337.5 ns; 680/1,688 heap B; 10/18 allocs | Selected route plus direct candidates: 226.15/263.35 ns; 80/128 heap B; 3/3 allocs | 2.85x/5.08x faster; 8.50x/13.19x lower heap; 3.33x/6x fewer allocations | Timeout, offline, maintenance, failover, topology-generation consistency, ownership, and lock order are unchanged |
| Current pass | [Allocation-free election node updates](#allocation-free-election-node-updates), one/four-shard heartbeat | Full topology clone: 279.65/535.5 ns; 272/896 heap B; 3/6 allocs | Normalized node lookup: 60.91/60.505 ns; 0 heap B; 0 allocs | 4.59x/8.85x faster; all timed heap and allocations eliminated | Membership follows every topology generation; heartbeat/offline record writes, validation, timestamps, locks, and behavior are unchanged |
| Current pass | [Normalized election status generation](#normalized-election-status-generation), healthy one/four shards | Clone/sort topology: 808.05/1,851 ns; 944/2,432 heap B; 14/25 allocs | Borrow normalized generation: 387.1/800.0 ns; 464/976 heap B; 5/8 allocs | 2.09x/2.31x faster; 2.03x/2.49x lower heap; 2.80x/3.13x fewer allocations | Returned nodes, leaders, candidates, timestamps, ordering, generation consistency, locks, and behavior are unchanged |
| Current pass | [Election-record status leader lookup](#election-record-status-leader-lookup), healthy one/four/64 shards | Temporary active map: 459.15/961.65/12,100 ns; 464/976/14,680 heap B; 5/8/70 allocs | Existing election records: 289.9/744.65/9,153 ns; 208/720/11,136 heap B; 3/6/66 allocs | 1.58x/1.29x/1.32x faster; 256/256/3,544 fewer heap bytes; 2/2/4 fewer allocations | Maintenance generations retain the former active map and are 1.07x faster with identical memory; cached mode bit adds no topology-store bytes on amd64 |
| Current pass | [Cached replication routing fingerprint](#cached-replication-routing-fingerprint), one/four shards | Rehash: 3,238/7,028.5 ns; 3,920/7,832 heap B; 52/129 allocs | Cached: 1,621.5/3,447.5 ns; 3,032/5,600 heap B; 14/34 allocs | 2.00x/2.04x faster; 1.29x/1.40x lower heap; 3.71x/3.79x fewer allocations | Reuses the fingerprint already computed by validated topology installation; topology cloning, routing maps, wire, and behavior are unchanged |
| Current pass | [Normalized replication target precomputation](#normalized-replication-target-precomputation), one/four/64 shards | Per-shard duplicate map: 1,436.5/3,591.5/47,579.5 ns; 64 shards: 84,759 B, 403 allocs | Validated owners: 1,395/3,121.5/43,078.5 ns; 64 shards: 84,709 B, 402 allocs | 1.03x/1.15x/1.10x faster; 64 shards use 50 fewer heap bytes and one fewer allocation | Applies only to private snapshots of normalized topology; self, online, existence, and sorted-output filters are unchanged |
| Current pass | [Map-free replication routing snapshots](#map-free-replication-routing-snapshots), 2/4/64 shards | Node map: 1,722.5/3,304.5/45,159 ns; 3,360/5,440/84,704 heap B | Sorted nodes: 1,282.5/2,819.5/42,968 ns; 2,288/4,368/68,232 heap B | 1.34x/1.17x/1.05x faster; 1.47x/1.25x/1.24x lower heap; 2/2/4 fewer allocations | Uses the normalized sorted topology generation and immutable precomputed targets; routing, target order, election state, wire, and behavior are unchanged |
| Current pass | [Aligned replication shard state](#aligned-replication-shard-state), 2/16/64-shard snapshot plus hash route/targets | Three shard-ID maps: 1,360/10,153/44,388 ns construction; 75.385/95.83/89.315 ns routing | Three aligned slices: 923.4/8,794.5/38,234.5 ns construction; 70.88/70.07/65.025 ns routing | Construction 1.47x/1.15x/1.16x faster; routing 1.06x/1.37x/1.37x faster; 3/9/9 fewer construction allocations | Normalized shard order provides the index; complete hot routes preserve it through target lookup, while defensive by-ID access binary-searches the same sorted generation |
| Current pass | [Canonical replication owner slices](#canonical-replication-owner-slices), election snapshot and bucket route at 2/16/64 shards | Duplicate owners: 1,293.5/11,369/45,965.5 ns; 17/121/457 allocs; routes 82.935/89.095/97.16 ns | Leader candidates: 1,140.5/10,183/41,608 ns; 14/104/392 allocs; routes 77.80/84.16/90.555 ns | Construction 1.13x/1.12x/1.10x faster; 3/17/65 fewer allocations; routes 1.07x/1.06x/1.07x faster | Leader candidates were already immutable route output; election, owner order, targets, wire, storage, and behavior are unchanged |
| Current pass | [Sparse replication liveness exceptions](#sparse-replication-liveness-exceptions), healthy 2/16/64-shard snapshot and target membership | Active map: 1,104/10,692/40,087 ns; 1,312/14,680/57,560 B; membership 43.15 ns | Lazy inactive map: 895.8/9,248/36,527 ns; 1,056/13,696/54,016 B; membership 31.11 ns | Construction 1.23x/1.16x/1.10x faster; 256/984/3,544 fewer B; membership 1.39x faster | Offline, timeout, and maintenance construction is 1.01x-1.04x faster with equal or lower heap; election, target filtering, wire, storage, and behavior are unchanged |
| Current pass | [Adaptive replication target sorting](#adaptive-replication-target-sorting), healthy 2/16/64-shard snapshots | Reflective sort: 1,054/8,880/36,839 ns; 12/100/388 allocs | Generic sort through 16 targets: 932.4/6,856/29,815 ns; 10/52/196 allocs | 1.13x/1.30x/1.24x faster; 2/48/192 fewer allocations; 48/2,944/11,776 fewer heap B | Above 16 targets retains the original faster reflective sorter; full-replica 32/64-node CPU and memory are neutral |
| Current pass | [Borrowed replication topology generation](#borrowed-replication-topology-generation), healthy 2/16/64-shard snapshots | Clone normalized generation: 790.35/6,439.5/27,287.5 ns; 1,008/10,752/42,240 B | Borrow immutable generation: 541.65/5,294/22,112 ns; 672/7,552/30,208 B | 1.46x/1.22x/1.23x faster; 1.50x/1.42x/1.40x lower heap; 4/18/66 fewer allocations | Private snapshot only; `Set` replaces complete generations and public topology/routing APIs retain cloned ownership |
| Current pass | [Four-shard replication owner backing](#grouped-replication-owner-backing), healthy 2/16/64-shard snapshots | Per-shard owners: 591.9/5,183/24,017.5 ns; 6/34/130 allocs | Four-shard groups: 576.15/4,983/23,445 ns; 5/22/82 allocs | 1.03x/1.04x/1.02x faster; 1/12/48 fewer allocations with identical heap | Full-replica construction retains the prior one-shard path; capped candidate slices prevent cross-shard append aliasing |
| Current pass | [Combined replication owner/target backing](#combined-replication-owner-target-backing), healthy 2/16/64-shard snapshots | Separate targets: 544.35/4,994.5/22,196.5 ns; 5/22/82 allocs | Shared group capacity: 490.5/4,735.5/20,770.5 ns; 4/10/34 allocs | 1.11x/1.05x/1.07x faster; 1/12/48 fewer allocations with identical heap | Reuses the existing owner count instead of adding the extra capacity pass that disqualified standalone target grouping; full replica is unchanged |
| Current pass | [Adaptive replication bucket search](#adaptive-replication-bucket-search), complete route plus targets at 16/64/256 ranges | Linear ranges: 91.115/111.0/180.05 ns | Binary ranges: 77.825/87.37/98.92 ns | 1.17x/1.27x/1.82x faster; heap and allocations unchanged | Two through eight ranges retain linear lookup; normalized contiguous ranges above that threshold use binary search |
| Current pass | [Direct replication route membership](#direct-replication-route-membership), three-owner remote-source check | Materialize/filter/sort: 330.6 ns; 504 heap B; 4 allocs | Direct owner check: 42.775 ns; 0 heap B; 0 allocs | 7.73x faster; all timed heap and allocations eliminated | Private boolean validation only; source exclusion, online filtering, registered-node validation, explicit/fallback owners, wire, and routing behavior are unchanged |
| Current pass | [Normalized replication route owners](#direct-replication-route-membership), three-owner remote-source check | Direct plus node-index probe: 37.475 ns | Validated owner match: 29.865 ns | 1.25x faster; zero heap and allocations in both | Every private route owner comes from the validated normalized snapshot; source, online, owner fallback, wire, and behavior are unchanged |
| Current pass | [Binary outbox encoding](#binary-grouped-replication-outbox), 4 KiB job | JSON: 8,949 ns; 5,948 B | Binary: 4,123 ns; 4,412 B | 2.17x faster, 25.8% smaller | Binary records require project tooling to inspect |
| Current pass | [Binary outbox replay](#binary-grouped-replication-outbox), 10k jobs | JSON: 217.479 ms | Binary: 87.330 ms | 2.49x faster, 1.34x fewer allocs | Existing JSON records remain readable |
| Current pass | [Bounded lazy outbox restore](#binary-grouped-replication-outbox), 100k jobs | 466.884 ms; 100,000 resident jobs; 415.1 MB heap | 5.019 ms; 1,024 resident jobs; 3.52 MB heap | 93.03x faster, 97.66x fewer resident jobs, 118.0x lower heap | LevelDB pages are lazy; legacy whole-file JSON still loads its file snapshot |
| Current pass | [Outbox group commit](#binary-grouped-replication-outbox), 32 writers | JSON sync-each: 50.289 ms; 32 syncs | Binary grouped: 3.542 ms; 1 sync | 14.20x faster, 32x fewer syncs | Cumulative heap is 1.49x higher |
| Current pass | [Journal-backed outbox](#journal-backed-replication-outbox), 10k durable 4 KiB mutations | Full LevelDB jobs: 136.854 s; 20,993 heap B/op; 2 syncs/op | Journal references: 7.845 s; 26,094 heap B/op; 1 sync/op | 17.44x faster, 2x fewer syncs | Total encoded/disk bytes are effectively unchanged; cumulative heap is 1.24x higher |

## Rejected Optimization Index

This is the central inventory of every failed or reverted performance
experiment recorded in this document and in optimization-only revert history.
Ordinary tests that reject invalid input are not performance experiments. A
candidate appears here even when a later, materially different design solved
the same problem. Unless a row explicitly names the retained replacement, all candidate code was removed and therefore adds no runtime cost to the current
tree.

| Rejected candidate | Measured attraction | Disqualifying result | Final state / detail |
| --- | --- | --- | --- |
| Direct Unix telemetry clock | Avoid constructing cached `time.Time` values | SET/GET/INC/TTL were 1.05x/1.07x/1.02x/1.05x slower with no memory gain | Reverted; the [cached default trie clock](#cached-default-trie-clock) remains |
| Exact scalar command dispatch | INC improved 1.02x in the strict control | SET/GET/TTL were 1.02x/1.03x/1.005x slower; large-switch and GET-hoist variants also slowed GET | Removed; see [exact scalar mutation dispatch](#exact-scalar-mutation-dispatch) |
| Cgo call annotations | Intended to remove call overhead with `noescape`/`nocallback` | SET/GET/INC/TTL regressed 1.03x/1.10x/1.15x/1.03x | Removed; see [exact scalar mutation dispatch](#exact-scalar-mutation-dispatch) |
| Known-valid-key GET helper | Intended to skip redundant key validation | 121.7 ns versus 120.1 ns for the checked path | Removed; see [exact scalar mutation dispatch](#exact-scalar-mutation-dispatch) |
| Idempotent string assignment | Intended to skip an unchanged string-header write and reusable-index check | The refined one-check prototype made duplicates 1.27x slower and true replacements 1.07x slower | Removed before production; direct assignment remains; see [idempotent string assignment](#idempotent-string-assignment-rollback) |
| Temporary packed-map materialization | Reused the generic map JSON encoder | 1,499 ns, 488 B, and 5 allocations | Replaced by direct JSON at 511.4 ns, 24 B, and 1 allocation; see [packed small-map storage](#packed-small-map-storage) |
| Single-object storage-header group | Reduced empty cache construction from 25 to 7 allocations and was 1.06x faster | Go's 2,048-byte size class raised cumulative heap from 3,360 to 3,424 B | Replaced by the map-separated [grouped storage headers](#grouped-storage-headers), which retain the CPU/allocation gain with unchanged heap |
| Boxed packed-set reads | Avoided retaining interface payloads in packed pools | Two-member reads were 1.31x slower with 2x heap and 3x allocations | Removed; packed pools retain the faster interface payload layout; see [packed small string-set storage](#packed-small-string-set-storage) |
| Sentinel-encoded packed-slice length | Shrunk each two-value slice record from 40 to 32 bytes and lowered the 100,000-slice retained/timed heap 1.25x | The refined marker made pop/push 1.06x slower and shift/push 1.04x slower; the first marker design was 1.10x/1.09x slower | Reverted; the inline length byte remains; see [packed two-slice length](#packed-two-slice-length-rollback) |
| SetStorage-level promoted JSON dispatch | Enabled direct promoted-set encoding at the shared storage encoder | Packed one/two-string command reads became 1.11x/1.10x slower | Replaced by command-level promoted routing; packed reads are neutral or faster; see [packed small string-set storage](#packed-small-string-set-storage) |
| Priority-queue interface marker | Reached the desired 48-byte item layout | Generic dispatch slowed from 1.534 to 1.961 ns | Replaced by length dispatch; see [compact priority-queue items](#compact-priority-queue-items) |
| Priority-queue structured fallback scan | Kept the direct encoder string-only | A worst-case 100-item queue could scan every item before generic materialization and drift about 1% slower | Replaced by direct mixed-value encoding; see [compact priority-queue items](#compact-priority-queue-items) |
| Radix-node tag compaction | 1.125x lower retained heap and 1.04x faster build | String, stored-`nil`, and missing reads were 1.10x-1.16x slower | Reverted; see [radix-node tag compaction](#radix-node-tag-compaction-rollback) |
| Fully linked XOR peel order | Halved normal-build allocations and cumulative heap | Cache-random reverse traversal made the 4,096/65,536-item builders 1.03x/1.05x slower | Replaced by linking only the queue while retaining contiguous peel order; see [linked XOR-filter build queue](#linked-xor-filter-build-queue) |
| Direct staged-map XOR build | Removed the key-index allocation and made the separate 65,536-item build 1.35x faster with 1.43x lower heap | Same-binary 64-item and forced-retry builds were 1.06x and about 1.20x slower | Replaced by a compact seed-independent hash index; see [compact XOR-filter build hash index](#compact-xor-filter-build-hash-index) |
| Marker-only plain XOR staging | Removed 64 allocations and 1,024 cumulative bytes while making 64-item staging 1.17x faster | Pending snapshot creation shifted those costs later: 2 to 66 allocations, 3,456 to 4,480 bytes, and 1.23x slower | Reverted; staged values remain pre-boxed so snapshots stay cheap; see [XOR staging marker](#xor-staging-marker-rollback) |
| Inline sparse bitsets with generic search | Removed singleton/pair backing allocations and reduced retained objects | The added representation branch made 4,096-value array lookups 1.03x slower | Replaced by an inlineable typed binary search; promoted lookups are now 1.01x faster; see [inline sparse-bitset containers](#inline-sparse-bitset-containers) |
| Inline Roaring-container values | Made 50,000 singleton containers 1.38x faster to build and removed 50,000 backing allocations | Promoted 16-value lookups were 1.04x slower; large-array and bitmap controls were also about 1.01x-1.02x slower | Reverted; the original slice representation remains; see [inline Roaring-container rollback](#inline-roaring-container-rollback) |
| Local slice view over fixed Roaring bitmap | Preserved the 48-byte header while dense build/lookup measured 1.02x/1.09x faster | Paired bitmap remove/add was 4.594 versus 4.486 ns, or 1.024x slower | Reverted; direct fixed-pointer access remains 1.006x faster than the legacy slice in the longer control; see [compact Roaring-container headers](#compact-roaring-container-headers) |
| 40-byte Roaring field order | Lowered singleton retained and cumulative heap 1.21x/1.24x and made the 50,000-container build 1.11x faster | The best values-first layout made paired 4,097-value dense construction 1.018x and 1.044x slower in two nine-run confirmations; a pointer-first layout made lookup 1.039x slower | Reverted; the 48-byte operation-neutral layout remains; see [Roaring field-order compaction](#roaring-field-order-compaction-rollback) |
| HyperLogLog side allocation | Kept derived estimate fields outside the header | Go size-class rounding raised the 1,000-filter fixture heap by 12.47% | Replaced by an 8-byte header extension; see [incremental HyperLogLog estimates](#incremental-hyperloglog-estimates) |
| String-keyed Merkle pending set | Used direct strings instead of compact key hashes | The 16,384-key maintenance cycle slowed from 29.294 to 33.070 ms without reducing heap | Removed; the hash-keyed bounded set remains; see [hierarchical Merkle anti-entropy](#hierarchical-merkle-anti-entropy) |
| Merkle table occupancy sentinel | Removed the occupancy allocation, lowered complete-index retained heap 1.06x, and made direct update/delete 1.07x/1.17x faster | Direct hits/misses were 1.10x/1.40x slower and complete 10k index construction was 1.08x slower | Reverted; the dense byte occupancy table remains; see [Merkle table occupancy sentinel](#merkle-table-occupancy-sentinel-rollback) |
| Top-K one-item rewrite | Reduced transient heap | Complete read CPU was 1.06x slower | Removed; the former one-item path remains; see [multi-item Top-K reads](#multi-item-top-k-read-materialization) |
| Generic Top-K slice sorter | Removed one allocation and 24 transient bytes | Exact 16/100-item reads were 1.07x/1.12x slower and every generic row regressed | Reverted; see [multi-item Top-K reads](#multi-item-top-k-read-materialization) |
| Generic Top-K structured fallback scan | Preserved the string-only direct encoder while extending exact generic `GET` | A 16-item structured read was 1.06x slower after scanning before unchanged materialization, with no heap or allocation gain | Replaced by one-pass mixed-value encoding; see [multi-item Top-K reads](#multi-item-top-k-read-materialization) |
| Dedicated `GETTOPK` lock-release snapshot | Let writers proceed while caller-controlled JSON marshaling was blocked | The five-second 100-item structured read was 14,457 ns versus 13,776 ns legacy, or 1.05x slower, with identical 9,872 B and 5 allocations | Reverted for `GETTOPK`; the serial-neutral generic `GET` snapshot remains; see [multi-item Top-K reads](#multi-item-top-k-read-materialization) |
| Reservoir escaped-value exact sizing | Tried to pre-size the direct mixed JSON buffer exactly before writing escaped strings | The second full escape scan made exact encoded reads 3,489 ns versus 2,707 ns generic, or 1.29x slower | Removed; the retained writer uses the checked raw reservation and grows only when escaping requires it; see [reservoir sample reads](#reservoir-sample-read-materialization) |
| Reservoir sort outside cache lock | Shortened both dedicated and generic reservoir read lock holds without adding allocation | Default-capacity string/mixed generic reads were each 1.06x slower, and the dedicated 16-item read was 1.10x slower, with identical heap and allocation counts | Reverted; copy and sort retain their prior lock scope; see [reservoir sample reads](#reservoir-sample-read-materialization) |
| Mutation response encoding outside cache lock | Let unrelated writers proceed during caller-controlled `POPSLICE` and `POPPQ` JSON marshaling | Ordinary structured slice and priority-queue pops were each about 1.03x slower with unchanged heap and allocations | Reverted; mutation, accounting, and response encoding retain their prior single lock scope; see [mutation response lock release](#mutation-response-lock-release-rollback) |
| Generalized whole-sequence single-fallback scan | Directly validated a sole nested value at any position and retained the large sparse gain | Finding a second nested value made the 4,096-item unchanged fallback about 1.01x slower | Replaced by a trailing-only proof that never scans beyond the prior fallback boundary; see [flat scalar sequence validation](#flat-scalar-sequence-validation) |
| Shared-lock generic collection GET | Parallel map/slice/set reads improved 3.47x-5.13x with unchanged allocation counts | Serial reads were 1.06x-2.00x slower because the shared-read lookup's fixed cost outweighed concurrency for complete collection commands | Removed; scalar, priority-queue, Top-K, and reservoir shared reads remain; see [concurrent scalar reads](#concurrent-scalar-read-fast-path) |
| Top-K helper lookup | Centralized inline and map-backed lookup | Map-backed estimates were 1.62x-1.88x slower | Removed; the cardinality branch remains; see [lazy small Top-K indexes](#lazy-small-top-k-indexes) |
| Naive repeated-read scalar routing | Tried to send repeated reads through the native selector | 16 reads regressed 2.58x; a scan-only guard remained 1.08x slower | Replaced by resolve-once response copying; see [adaptive typed scalar execution](#adaptive-typed-scalar-execution) |
| Two-command native scalar routing | Lowered the initial native threshold | 629.5 ns versus 565.5 ns, or 1.11x slower | Removed; native routing starts at four commands; see [adaptive typed scalar execution](#adaptive-typed-scalar-execution) |
| 64 KiB WAL staging | Saved another 65,536 transient bytes over 128 KiB | It was 1.07x slower and required seven writes instead of four | Rejected; 128 KiB remains the measured balance; see [bounded WAL staging](#bounded-wal-staging-arena) |
| Online generational compaction | Shortened maximum reader pause 9.40x and reduced retained backing/heap 13.17x/5.36x | Total compaction was 1.54x slower, transient heap 6.80x higher, and allocations 2.67x higher | Reverted in `c3085d2`; see [online generational compaction](#online-generational-compaction-rollback) |
| Packed-string compaction | Reduced retained heap 3.79% and retained objects 800x | Cumulative allocation was 10.71x higher, peak RSS 1.30x higher, and forced GC 1.81x slower | Reverted in `0f4adc3`; see [string compaction allocation](#string-compaction-allocation-rollback) |
| Replication constructor flag | Avoided deriving invariant scan mode after construction | Added one 704-byte allocation | Removed; mode uses an existing byte; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Mixed-page compact descriptors | Tried to keep mixed SET/delete repair pages in the compact layout | Added 17% transient heap | Replaced by selecting generic compatibility storage before descriptor allocation; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Ten-page replication aggregation | Reduced request count beyond the retained two-page cap | Could stage ten unusually large pages before splitting | Removed to preserve bounded memory; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Copying replication arena | Shared value storage but copied/reconstructed every key during protobuf sizing and writing | The paired 10k end-to-end median was about 1.09x slower | Replaced by direct immutable key references; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Shared-loop single-target digest branch | Removed the target map for one target without a separate scan loop | The added per-key branch made the immediate four-target control 1.8% slower | Replaced by caller-level selection; the [direct single-target digest inventory](#direct-single-target-digest-inventory) leaves the multi-target function unchanged |
| Direct native packed scan | Focused preparation improved 1.10x with 1.15x lower heap | End-to-end CPU improved only 1.02x, below the 5% gate for a new C ABI | Reverted; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Single-pass legacy repair | Unordered transfer was 1.07x faster with 1.11x fewer allocations | Wire grew 1.15x; restoring deterministic order made CPU 1.075x slower | Both variants reverted; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Exact protobuf batch coalescing | Halved requests and sender allocations improved 1.44x | Receiver decode was 1.09x slower, largest body doubled, and combined CPU was 1.012x slower | Reverted; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Carried compact payload estimates | Isolated splitting improved 4.37x | Complete scan/serialize/split CPU was 0.36% slower with unchanged allocations | Reverted; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Specialized compact payload estimator | Focused splitting improved 1.92x | End-to-end CPU was 0.50% slower without memory, request, or wire gain | Reverted; see [replication descriptor optimizations](#replication-descriptor-optimizations) |
| Fully lazy snapshot mutation map | Removed one more map from snapshots without concurrent writes | First concurrent mark was 1.32x slower, 208 to 256 B, and one to two timed allocations | Rejected; captures retain a writer-ready initial map and use [selective snapshot mutation maps](#selective-snapshot-mutation-maps) only after drain |
| Unchecked normalized replication owners | Removed repeated trim, empty-ID, and missing-node checks after topology validation | Two-shard snapshot construction was 1.08x slower; four shards were neutral within about 1%; 64 shards improved only 1.03x with identical memory | Reverted; the checked normalized helper remains; see [normalized replication target precomputation](#normalized-replication-target-precomputation) |
| Topology-store large bucket dispatch | Binary search made 64/128/256-range routes 1.16x/1.29x/1.64x faster with unchanged allocation behavior | The added dispatch made the common two-range route 1.015x slower, 166.45 versus 164.05 ns | Removed from production; the unchanged linear topology-store route remains; see [topology-store large bucket dispatch](#topology-store-large-bucket-dispatch-rollback) |
| Standalone grouped replication target backing | One backing removed 63 allocations and made the isolated healthy 64-shard constructor 1.07x faster; four-shard groups kept heap flat and removed 48 allocations | One backing added 128/256 heap B at 8/16 shards; a separate group-capacity pass made healthy 2/16 and offline 8/32/64 construction slower | The [standalone design](#grouped-replication-target-backing-rollback) remains rejected; [combined owner/target backing](#combined-replication-owner-target-backing) later reused an already-required owner pass and removed the CPU cost |
| Single replication owner backing | Removed 63 owner allocations from a healthy 64-shard snapshot | Go size-class rounding added 128/256/128 cumulative heap B at 16/32/64 shards | Replaced by [four-shard replication owner backing](#grouped-replication-owner-backing), which keeps heap flat while removing 48 allocations at 64 shards |
| Combined-backing helper extraction | Kept the full-replica constructor's machine-code body smaller by moving multi-shard construction behind one helper call | Offline 32/64-shard paired medians became 1.01x slower, 11,881 to 12,006 ns and 25,061 to 25,226 ns | Reverted before commit; the measured-faster grouped loop remains inline; see [combined owner/target backing](#combined-replication-owner-target-backing) |
| Generic replication target sorting at every size | Removed three reflective allocations and 184 cumulative heap B for one large target slice | Complete paired 31/63-target construction was 1.03x/1.025x slower | Replaced by the measured 16-target cutoff; large sets retain the original sorter; see [adaptive replication target sorting](#adaptive-replication-target-sorting) |
| Dedicated packed scalar key fields | General distinct-key batches improved 1.09x with 1.44x fewer allocations and 1.56% less wire | Two added slice fields enlarged every decoded legacy request; the 10k legacy control used 31,947 more heap B, or 1.35%, even when the fields were absent | Removed before commit; [shared scalar-batch keys](#shared-scalar-batch-keys) reuse the existing key column, improve the target workload more, and leave the generated request layout unchanged |
| Reused streamed scalar responses | Could remove roughly three response/status allocations per envelope | gRPC's `SendMsg` contract forbids modifying a message after send because tracing and stats handlers may consume it lazily | Rejected before an unsafe prototype; every streamed response remains independently owned |
| gRPC shared transport buffers | Receive pooling and shared write buffers could reduce framing allocations | The APIs are experimental; receive pooling is disabled with stats/tracing and discouraged with compression, while shared write buffers use a global pool and add acquire/release work at every flush | Rejected as a no-tradeoff default before a product prototype; transport ownership and configuration remain unchanged |
| Combined structured-column materializer | Expanded shared keys and subkeys from one backing allocation | The larger helper stopped inlining and added one allocation per shared-key-only envelope: 2,636,371 to 2,746,424 heap B and 53,186 to 53,812 allocations per 10k commands | Replaced before commit by separate inlinable key/subkey materializers; the shared-key-only control returned exactly to its shipped heap and allocation counts |

<a id="delta-only-startup-persistence"></a>
### Delta-Only Startup Persistence

Persistent stores now commit an applied journal sequence in the same synced
LevelDB batch or Pebble generation-activation batch as the corresponding cache
state. On restart, journal replay starts after that sequence and the immediate
periodic-store pass writes only tracked changes. If the loaded database is
already current, it performs one metadata read and writes no records. Direct
trie mutations feed the same bounded dirty tracker used by HTTP, gRPC, journal
replay, TTL deletion, and local partitions.

Stores without sequence metadata retain the previous full-save migration path.
Loading an authoritative snapshot also forces a complete exact save so stale
database keys are removed. During sequence-aware persistence the journal barrier
prevents a sequence from advancing past data not represented by the commit;
this is required for non-idempotent commands such as `INC`.

The fixture first creates and reloads an exact 10,000-key Pebble generation.
Setup is outside the timer. The baseline performs the former second full
generation save; the final path checks an unchanged sequence and dirty set.

```sh
make bench-startup-persistence BENCHTIME=1x COUNT=7
```

| Seven-run median | Full startup rewrite | Delta-only checkpoint | Improvement |
| --- | ---: | ---: | ---: |
| Time | 16.285 ms | 16.460 us | 989x faster |
| Heap allocation | 4,635,616 B | 12,912 B | 359x lower |
| Allocations | 21,085 | 7 | 3,012x fewer |
| Persistent data writes | complete generation | none | eliminated when unchanged |

Baseline milliseconds were `14.460, 20.531, 17.039, 15.102, 16.285,
13.324, 76.895`; final reproducible microseconds were `10.031, 14.922,
16.460, 23.201, 17.210, 17.560, 8.411`. Raw current output is written to
`build/benchmarks/startup-persistence.txt`.

<a id="persistent-storage-backend-bakeoff"></a>
### Persistent Storage Backend Bakeoff

<a id="pebble-generation-full-save"></a>
#### Generation-Based Full Save

Pebble now streams a page-bounded cache capture into an external SST and
atomically activates it with one synced generation marker. The legacy baseline
materializes every encoded row in Go memory and commits it through Pebble's
WAL. Both paths return crash-durable state; only the generation path gives an
atomic complete-state switch without retaining a full-data WAL copy. Run:

```sh
make bench-pebble-generation BENCHTIME=3x COUNT=5
```

| Path, median of five | Time/op | Heap B/op | Allocs/op | Disk B/key | Table B/key | WAL B/key | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Legacy batch | 18.369 ms | 21,050,818 | 40,283 | 598.0 | 297.8 | 300.1 | baseline |
| Generation SST | 24.651 ms | 9,607,717 | 40,943 | 299.6 | 299.4 | 0.0281 | 2.19x lower heap, 2.00x smaller disk, 10,680x less WAL |

The generation path pays 1.34x latency and 1.02x allocations to build and
ingest the final table instead of only appending the full payload to a WAL.
Serialization occurs outside the global trie lock. Two optimistic direct-SST
captures avoid temporary spool I/O when writes are quiet; sustained concurrent
mutation uses a bounded disk spool and final mutation reconciliation. Raw output
is in `build/benchmarks/pebble-full-save-generation.txt`.

<a id="pebble-checkpoint-backup-bundles"></a>
#### Pebble Checkpoint Backup Bundles

The optional `pebble-checkpoint` backup mode saves an atomic complete
generation, compacts generation tombstones, and packages Pebble's native
checkpoint files. Snapshot remains the `auto` default because it has the better
CPU and bandwidth result. Checkpoint mode is intended for operators prioritizing
lower restore allocation and a directly reusable Pebble directory.

The fixture contains 10,000 keys with deterministic low-compressibility
256-byte values. Create includes point-in-time persistence, checksums, and the
tar.gz bundle. Restore includes streamed checksum verification, semantic store
validation, and extraction. Results are seven-run medians on the Ryzen 9 5950X
host; heap and allocations cover the complete timed operation.

```sh
make bench-pebble-backup BACKUP_BENCH_KEYS=10000 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Operation / metric | Snapshot default | Pebble checkpoint | Relative result |
| --- | ---: | ---: | --- |
| Create 10k keys | 76.885 ms | 103.100 ms | Snapshot is 1.34x faster |
| Create heap | 5,873,160 B | 9,807,928 B | Snapshot uses 1.67x less heap |
| Create allocations | 40,201 | 32,855 | Checkpoint uses 1.22x fewer allocations |
| Bundle size | 198.7 B/key | 210.5 B/key | Snapshot is 1.06x smaller |
| Restore 10k keys | 61.219 ms | 83.666 ms | Snapshot is 1.37x faster |
| Restore heap | 21,352,888 B | 13,352,912 B | Checkpoint uses 1.60x less heap |
| Restore allocations | 101,064 | 62,406 | Checkpoint uses 1.62x fewer allocations |
| Separate-process maximum RSS | 356,500 KiB | 357,172 KiB | Effectively tied; checkpoint is 1.002x higher |

Raw elapsed samples in milliseconds were:

| Operation | Seven samples |
| --- | --- |
| Snapshot create | `84.020, 83.170, 76.781, 76.885, 68.651, 65.555, 78.456` |
| Checkpoint create | `102.159, 119.602, 98.709, 103.100, 105.810, 113.213, 101.341` |
| Snapshot restore | `61.219, 57.184, 62.797, 67.518, 54.989, 58.396, 65.600` |
| Checkpoint restore | `102.694, 124.540, 127.511, 75.009, 79.085, 83.666, 76.773` |

The combined raw Go benchmark and process memory output is generated at
`build/benchmarks/pebble-checkpoint-backup.txt`. Separate RSS runs are generated
at `build/benchmarks/backup-snapshot/pebble-checkpoint-backup.txt` and
`build/benchmarks/backup-checkpoint/pebble-checkpoint-backup.txt`. Pebble log
replay diagnostics in the raw output are emitted while verification opens the
extracted checkpoint and are not benchmark failures.

<a id="content-addressed-incremental-backups"></a>
#### Content-Addressed Incremental Backups

The explicit `pebble-incremental` mode stores checkpoint files by SHA-256 and
publishes a content-derived manifest plus an atomic `latest` pointer. The first
backup is a full base. Later backups on the same Pebble storage generation save
only the dirty keys, checkpoint without full compaction, and reuse unchanged
SST objects. A generation or source-store identity change safely starts another
full base. Manifests and objects are checksum-verified during `doctor` and
restore; the default retention is 32 manifests.

The benchmark starts with 10,000 deterministic low-compressibility 256-byte
values, changes 100 keys, and compares the existing full `pebble-checkpoint`
tar.gz path with a subsequent repository backup. Written bytes include the full
bundle for the baseline and new objects, manifest, and `latest` pointer for the
repository. Results are seven-run medians on the Ryzen 9 5950X host.

```sh
make bench-incremental-backup BACKUP_BENCH_KEYS=10000 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Metric, 1% changed | Full checkpoint bundle | Incremental repository | Improvement |
| --- | ---: | ---: | ---: |
| Time | 98.602 ms | 14.659 ms | 6.73x faster |
| Timed heap | 9,810,048 B | 935,224 B | 10.49x lower |
| Allocations | 32,852 | 1,375 | 23.89x fewer |
| Bytes written / transferable delta | 2,104,489 B | 35,020 B | 60.09x fewer |
| Logical checkpoint bytes reused | 0% | 98.92% | 98.92 percentage points |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Full checkpoint bundle | `98.901, 100.787, 97.505, 96.340, 98.602, 104.923, 96.742` |
| Incremental repository | `14.659, 16.752, 15.183, 17.964, 13.520, 13.785, 13.786` |

The raw Go benchmark, including per-sample heap, allocation, logical-byte,
written-byte, and reuse metrics, is generated at
`build/benchmarks/incremental-backup-repository.txt`. The tradeoff is a
multi-file repository rather than one portable archive. Repository growth is
bounded by retention and content garbage collection, but retained checkpoints
can keep old SST objects live. The mode requires Pebble and an accurate dirty
tracker; `auto` intentionally remains the portable snapshot mode.

<a id="single-pass-staged-restore"></a>
#### Single-Pass Staged Restore

Bundle restore now extracts payload files once into a sibling staging
directory, verifies checksums and snapshot/Pebble semantics there, fsyncs files
and directories, then publishes the complete directory. Overwrite keeps the old
directory under a rollback name until the new directory rename and parent sync
succeed. Source/destination overlap, final symlinks, and symlinked parent path
components are rejected. Pebble validation is read-only, so verification does
not mutate the staged checkpoint.

The legacy baseline reproduces the previous verify-to-temporary-directory plus
second extraction/materialization. The candidate includes durability syncs and
publication, so this is not a relaxed durability comparison. The fixture has
10,000 deterministic low-compressibility 256-byte values. Results are seven-run
medians on the Ryzen 9 5950X host.

```sh
make bench-atomic-restore BACKUP_BENCH_KEYS=10000 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Restore path | Legacy | Staged single pass | Relative result |
| --- | ---: | ---: | --- |
| Snapshot time | 60.796 ms | 54.445 ms | 1.12x faster |
| Snapshot heap | 21,350,216 B | 21,213,376 B | 1.006x lower |
| Snapshot allocations | 101,063 | 101,020 | 43 fewer |
| Checkpoint time | 69.346 ms | 56.057 ms | 1.24x faster |
| Checkpoint heap | 13,176,560 B | 12,782,720 B | 1.03x lower |
| Checkpoint allocations | 62,234 | 61,790 | 444 fewer |
| Repository time | 30.892 ms | 33.623 ms | Legacy is 1.09x faster |
| Repository heap | 12,909,360 B | 12,651,624 B | 1.02x lower |
| Repository allocations | 61,640 | 61,558 | 82 fewer |
| Payload/materialization passes | 2 | 1 | 2x fewer |

Raw elapsed samples in milliseconds were:

| Path | Legacy | Staged single pass |
| --- | --- | --- |
| Snapshot | `70.384, 61.184, 58.647, 57.678, 59.325, 60.796, 65.346` | `56.701, 52.806, 54.445, 61.410, 52.399, 50.350, 55.976` |
| Checkpoint | `58.702, 68.016, 72.196, 74.766, 69.346, 78.855, 68.548` | `56.057, 56.566, 56.818, 52.374, 53.261, 55.948, 65.854` |
| Repository | `35.744, 30.892, 29.960, 30.349, 36.572, 35.553, 30.843` | `34.426, 32.890, 34.458, 33.623, 33.603, 32.240, 34.061` |

The raw benchmark with heap, allocation, source-byte, and pass-count metrics is
generated at `build/benchmarks/single-pass-atomic-restore.txt`. Small repository
checkpoints can lose elapsed time because one avoided local copy is cheaper than
the newly guaranteed fsyncs; the safety improvement and lower cumulative heap
still apply.

<a id="checkpoint-replica-bootstrap"></a>
#### Checkpoint Replica Bootstrap

A fresh Pebble follower now requests the leader's native checkpoint before it
opens its database. It checksum-stages the bundle once, durably publishes the
store and backend marker under a crash-recovery marker, installs the journal
checkpoint, persists pull state, and then opens and loads the store before it is
ready. Existing databases are not replaced and retain delta-first replication
with snapshot fallback.

The benchmark builds one 10,001-key source containing deterministic
low-compressibility 256-byte values. Both paths include local HTTP transfer,
durable follower storage, journal and pull-state persistence, and the final
store open/load. The snapshot baseline downloads the compressed snapshot,
replaces the complete trie, and performs a full Pebble save. The checkpoint path
downloads the native bundle and installs its already-built store. Leader-side
artifact generation is excluded from both paths. Results are seven-run medians
on the Ryzen 9 5950X host.

```sh
make bench-checkpoint-bootstrap BACKUP_BENCH_KEYS=10000 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Metric | Snapshot fallback | Pebble checkpoint | Relative result |
| --- | ---: | ---: | --- |
| Time | 146.369 ms | 84.246 ms | 1.74x faster |
| Cumulative heap | 36,662,200 B | 13,500,104 B | 2.72x lower |
| Allocations | 172,569 | 62,423 | 2.76x fewer |
| Wire bytes | 2,051,371 B | 2,103,717 B | Checkpoint is 2.55% larger |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Snapshot fallback | `148.576, 143.758, 146.369, 132.732, 185.331, 165.725, 139.255` |
| Pebble checkpoint | `84.246, 80.441, 84.483, 84.347, 77.442, 85.241, 81.993` |

The complete benchmark output, including each sample's heap, allocation, and
wire metrics, is generated at
`build/benchmarks/checkpoint-replica-bootstrap.txt`. The checkpoint path is the
default only for a missing Pebble database and can be disabled with
`JOURNAL_PULL_CHECKPOINT_BOOTSTRAP=false`. Snapshot fallback remains necessary
for existing databases, non-Pebble backends, incompatible storage formats, or a
leader without the checkpoint endpoint. The 2.55% wire increase comes from
transferring native Pebble files and their checksummed manifest instead of the
compact snapshot representation.

<a id="incremental-existing-replica-recovery"></a>
#### Incremental Existing-Replica Recovery

When a follower requests journal entries older than the leader retains, an
existing Pebble replica negotiates `/api/journal/recovery` before requesting
the full snapshot. The leader creates a journal-sequenced content-addressed
checkpoint manifest. The follower checksum-verifies its source-specific local
object cache, downloads only missing manifest objects, durably stages the
complete checkpoint, eagerly loads it into the live trie under the journal
replacement lock, and advances pull state last. Corrupt cached objects are
removed and downloaded again.

The fixture starts both paths with the same existing 10,001-key follower DB.
Values are deterministic low-compressibility 256-byte strings, 100 keys change,
and the repository path starts with the same cached base manifest on every
iteration. Both paths include leader-side artifact creation, local HTTP body
transfer, checksum/staging work, exact stale-key deletion, journal checkpoint
replacement, and a full save to the follower's open Pebble store. Setup of the
existing DB and cached base is excluded. Results are seven-run medians on the
Ryzen 9 5950X host.

```sh
make bench-existing-recovery BACKUP_BENCH_KEYS=10000 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Metric | Full snapshot recovery | Incremental repository recovery | Improvement |
| --- | ---: | ---: | ---: |
| Time | 169.906 ms | 104.739 ms | 1.62x faster |
| Cumulative heap | 38,683,024 B | 36,699,752 B | 1.05x lower |
| Allocations | 192,124 | 84,988 | 2.26x fewer |
| HTTP response body bytes | 2,047,776 B | 36,313 B | 56.39x smaller |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Full snapshot | `166.608, 169.906, 180.772, 164.741, 178.990, 144.945, 170.911` |
| Incremental repository | `104.676, 104.739, 110.637, 102.921, 106.768, 106.266, 102.605` |

The raw output is generated at
`build/benchmarks/existing-replica-recovery.txt`. HTTP headers are excluded, so
the candidate's manifest plus object request overhead is not represented in the
wire row. The path requires Pebble on both nodes, matching storage codecs, and a
cached base to realize delta bandwidth; its first recovery has full checkpoint
cost. Source and follower repositories consume disk, and staging temporarily
requires another checkpoint-sized directory. An unavailable endpoint, invalid
manifest/object, codec mismatch, non-Pebble follower, or disabled option
automatically retains the exact full-snapshot fallback.

<a id="active-recovered-pebble-generation"></a>
##### Active Recovered Pebble Generation

The first incremental-recovery implementation rewrote the complete restored
trie into the follower's already-open Pebble database after staging and loading
the native checkpoint. The current path stages on the active database
filesystem, closes the old Pebble handle under its existing save/database
locks, publishes the verified checkpoint at the stable configured path, and
reopens it through the same `PebbleStore` object. Background saving, compaction,
monitoring, memory spill, and shutdown therefore keep a stable store handle.

A deterministic `.recovery-old` directory makes publication recoverable. A
runtime validation/open failure moves the checkpoint back, restores the old
directory, and reopens it before returning. If a crash leaves the configured
path absent, startup restores `.recovery-old`; if the new path opens
successfully, startup removes the old directory. Pull results expose
`recovery_checkpoint_adopted` for operational confirmation.

The paired fixture is the same 10,000-key, 1%-changed workload above, rerun
after the single-representation string layout so both rows share the same
in-memory implementation. Seven-run medians are:

| Metric | Full local Pebble save | Checkpoint adoption | Improvement |
| --- | ---: | ---: | ---: |
| Recovery time | 112.016 ms | 107.014 ms | 1.05x faster |
| Cumulative heap | 34,157,368 B | 27,517,744 B | 1.24x lower |
| Allocations | 75,000 | 56,080 | 1.34x fewer |
| HTTP response body | 36,313 B | 36,313 B | unchanged |
| Local persistence | rewrite 10,001 records | directory metadata publication | full-record rewrite eliminated |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Full local save | `108.406, 140.600, 111.609, 111.449, 118.201, 112.016, 124.913` |
| Checkpoint adoption | `112.502, 107.014, 101.462, 104.427, 98.987, 107.320, 110.089` |

The downloaded and logical checkpoint size is unchanged. The active and staged
directories coexist until publication, and the old directory remains until the
new DB opens, so temporary disk capacity must cover both generations. Recovery
stages beneath the active DB parent; when the content-addressed repository is on
another filesystem, materialization copies its objects before the same atomic
directory publication. Non-Pebble stores and disabled incremental recovery keep
the existing full-save/snapshot paths.

The backend contract uses the same binary record codec and exercises a full
10,000-key save, 1,000 incremental operations (500 updates, 250 deletes, 250
inserts), a full materialized load, and manual compaction. Values are
deterministic incompressible 256-byte payloads. Each reported row is the median
of five samples with three complete fresh-directory cycles per sample. The
script builds the test binary first and runs each backend in its own process so
`/usr/bin/time` RSS excludes compiler memory.

```sh
make bench-storage-backends BENCHTIME=3x COUNT=5
```

| Phase / resource | LevelDB median | Pebble median | Pebble improvement |
| --- | ---: | ---: | ---: |
| Full cycle | 91.602 ms | 98.273 ms | 0.93x; LevelDB is 1.07x faster |
| Open | 5.381 ms | 13.555 ms | 0.40x; LevelDB is 2.52x faster |
| Full save | 2,197 ns/key | 2,441 ns/key | 0.90x; LevelDB is 1.11x faster |
| Incremental churn | 2,471 ns/op | 3,312 ns/op | 0.75x; LevelDB is 1.34x faster |
| Full load | 1,856 ns/key | 2,295 ns/key | 0.81x; LevelDB is 1.24x faster |
| Manual compact | 32.867 ms | 19.986 ms | 1.64x faster |
| Close | 47.521 us | 1.012 ms | 0.05x; LevelDB is 21.3x faster |
| Cumulative heap | 41,522,003 B/cycle | 20,521,051 B/cycle | 2.02x lower |
| Allocations | 97,272/cycle | 98,608/cycle | 0.99x; 1.01x higher |
| Peak RSS | 81,384 KiB | 82,684 KiB | effectively tied; Pebble is 1.02x higher |
| Live directory | 265.3 B/key | 285.7 B/key | 0.93x; LevelDB is 1.08x smaller |
| Table files | 265.1 B/key | 278.0 B/key | 0.95x; LevelDB is 1.05x smaller |
| Retained WAL | 0 B/key | 7.528 B/key | generation saves avoid the full-data WAL copy |

Raw five-sample output is written to
`build/benchmarks/storage-LevelDB.txt` and
`build/benchmarks/storage-Pebble.txt`; the corresponding `.time.txt` files
contain `/usr/bin/time -v` process metrics. The measured samples used above are:

| Engine | Sample | Cycle ms | Save ns/key | Churn ns/op | Load ns/key | Compact ms | Heap B/cycle | Allocs/cycle |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| LevelDB | 1 | 91.602 | 2,143 | 2,207 | 1,856 | 32.867 | 41,524,649 | 97,282 |
| LevelDB | 2 | 104.694 | 2,771 | 2,811 | 1,921 | 50.135 | 41,512,316 | 97,266 |
| LevelDB | 3 | 75.637 | 1,972 | 6,036 | 1,754 | 26.902 | 41,522,625 | 97,278 |
| LevelDB | 4 | 87.258 | 2,197 | 2,379 | 1,743 | 31.357 | 41,522,003 | 97,272 |
| LevelDB | 5 | 93.452 | 2,378 | 2,471 | 1,917 | 43.114 | 41,514,169 | 97,264 |
| Pebble | 1 | 81.037 | 2,441 | 3,248 | 2,601 | 17.820 | 20,459,844 | 98,608 |
| Pebble | 2 | 104.593 | 2,340 | 3,312 | 2,348 | 20.871 | 20,521,051 | 98,624 |
| Pebble | 3 | 98.273 | 2,418 | 3,266 | 2,295 | 19.986 | 20,534,888 | 98,601 |
| Pebble | 4 | 99.542 | 2,494 | 4,314 | 2,192 | 33.908 | 20,535,771 | 98,617 |
| Pebble | 5 | 80.124 | 2,503 | 4,229 | 2,277 | 19.088 | 20,492,964 | 98,583 |

Pebble is the default for a new `DB_BACKEND=auto` path because generation saves
provide atomic replacement, cumulative heap is 2.02x lower, and disk is now
within 1.08x of LevelDB. LevelDB remains a configurable fallback for
latency-sensitive deployments and short-lived tools.
Auto mode reads `<DB_PATH>.backend`; unmarked non-empty directories remain
LevelDB for backward compatibility. This benchmark measures one local NVMe
host and does not claim identical ratios for different filesystems, sync
latency, value compressibility, or long-running LSM compaction state.

<a id="parallel-cold-reference-hydration"></a>
### Parallel Cold-Reference Hydration

The fixture creates 32 distinct lazy references backed by a deterministic
250-microsecond delayed store. `Serialized` reads them one at a time, matching
the former global-lock behavior; `Parallel` issues simultaneous reads through
the new lock-free I/O phase. Five samples run five complete batches each:

```sh
make bench-cold-hydration BENCHTIME=5x COUNT=5
```

| Mode, median of five | Time/batch | Heap B/batch | Allocs/batch | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Serialized | 33.875 ms | 18,648 | 151 | baseline |
| Parallel singleflight | 1.174 ms | 30,166 | 272 | 28.85x faster |

Parallel scheduling costs 1.62x cumulative heap and 1.80x allocations in this
small synthetic batch. Backend latency overlaps instead of serializing under
the trie mutex, unrelated keys remain writable, and same-reference readers
share one backend call. Reference-token revalidation makes concurrent update,
delete, TTL metadata change, and slot reuse win over stale I/O. Raw output is
written to `build/benchmarks/cold-reference-hydration.txt`.

<a id="compact-lazy-reference-slab"></a>
### Compact Lazy-Reference Slab

The former slab retained the exported 88-byte `LevelDBReference` struct for
every cold key, including a repeated 16-byte store interface and 16-byte type
string header. The new internal record is 64 bytes, interns each store handle
once, encodes the finite value type as one byte, and keeps expiration fields
inline. Public `Get` still expands the same compatibility struct.

```sh
make bench-reference-slab BENCHTIME=3x COUNT=5
```

| Slab, median of five | Build 100k | Retained B/ref | Cumulative heap | Allocs | Improvement |
| --- | ---: | ---: | ---: | ---: | --- |
| Legacy public struct | 29.617 ms | 90.2 | 43,376,650 B | 30 | baseline |
| Compact internal record | 20.513 ms | 71.6 | 34,511,514 B | 30 | 1.44x faster, 1.26x lower retained/cumulative heap |

The retained reduction saves about 18.6 MB per million lazy references before
counting allocator fragmentation. The fixture uses the same shared key/type and
store in both modes so it isolates slab overhead; real unique key bytes and
optional key statistics are additional costs common to both. Raw output is in
`build/benchmarks/lazy-reference-slab.txt`.

### Incremental Anti-Entropy

Run the focused 10,000-key comparison:

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationDigestIncremental -benchmem -benchtime=1x -count=5'
```

Both nodes start with 10,000 deterministic, incompressible 1 KiB values. The
1%-changed case modifies 100 target values before each timed sync. The legacy
case rejects `INTERNALDIGESTV1`, then accepts a complete bounded transfer; it
therefore represents the current compatibility fallback rather than the former
invalid oversized request. Values are five-run medians on the AMD Ryzen 9
5950X host. Wire bytes include request and response bodies, but not HTTP
headers.

| State | Time/op | Requests/op | Wire B/op | Wire B/key | B/op | Allocs/op | CPU improvement vs full | Wire improvement vs full |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Equal digest | 22,129,470 ns | 1 | 215 | 0.0215 | 552,624 | 20,535 | 6.99x | 49,971x |
| 1% changed | 72,812,784 ns | 3 | 240,086 | 24.01 | 9,932,888 | 98,789 | 2.13x | 44.75x |
| Legacy full fallback | 154,735,234 ns | 20 | 10,743,774 | 1,074 | 113,891,072 | 148,955 | baseline | baseline |

Equal replicas use 206.1x less cumulative heap and 7.25x fewer allocations
than full transfer. At 1% changed, the digest path uses 11.47x less heap and
1.51x fewer allocations. The source does not retain a full key-digest map:
sorted source and target pages are merged into write batches capped at 1,024
keys, with the independent one MiB byte limit applied before transmission.

The 100,000-key equal-state architectural run is:

```sh
make bench-big-wins BIG_WINS_BENCH=BenchmarkBigWins/AntiEntropy BIG_WINS_KEYS=100000 BIG_WINS_OPS=100000 BENCHTIME=1x COUNT=5
```

Its median is 1,508 ns/key, one request, 205 wire bytes, and 3,440,960 B/op.
The previous blind-push baseline was 1,621 ns/key and 11,160,456 B/op, so the
digest check is 1.08x faster and uses 3.24x less cumulative heap even though
the benchmark now executes real scans on both source and target. Process RSS
is not directly comparable because the current fixture retains two tries.

The tradeoff is scan CPU: equality still requires hashing each eligible value
on both nodes. Mismatches also exchange per-key digests before changed values,
which is why a 1% repair sends more metadata than a hypothetical perfect change
log. xxHash64 plus encoded value length is probabilistic; an accidental digest
collision can defer repair until a later state changes the digest. Ordered
journal replication remains the primary catch-up path when a retained journal
tail is available.

### Persistent gRPC Command Stream

Run the 10,000-read architectural comparison:

```sh
make bench-big-wins BIG_WINS_BENCH=BenchmarkBigWins/UnaryCommand BIG_WINS_OPS=10000 BENCHTIME=1x COUNT=5
make bench-big-wins BIG_WINS_BENCH='BenchmarkBigWins/^StreamCommand$' BIG_WINS_OPS=10000 BENCHTIME=1x COUNT=5
make bench-big-wins BIG_WINS_BENCH='BenchmarkBigWins/^PipelinedStreamCommand$' BIG_WINS_OPS=10000 BENCHTIME=1x COUNT=5
make run CMD='HATRIE_BIG_WINS_OPS=10000 go test . -run none -bench "BenchmarkBigWins/(PipelinedStreamCommand|NativeBatchStreamCommand)" -benchtime=1x -count=5 -benchmem'
make bench-scalar-batch BIG_WINS_OPS=10000 BENCHTIME=1x COUNT=7
```

Sequential stream mode sends one request and receives its response before the
next command, measuring latency without pipelining. Pipelined mode uses one
sender and one receiver concurrently on the same ordered stream. Values are
five-run medians on the AMD Ryzen 9 5950X host.

| Mode | Time/10k | ns/command | B/10k | allocs/10k | CPU improvement | Heap improvement | Allocation improvement | Max RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unary `Command` | 590,396,372 ns | 59,040 | 106,952,376 | 1,900,978 | baseline | baseline | baseline | 34,344 KiB |
| Sequential `CommandStream` | 149,136,972 ns | 14,914 | 15,895,616 | 480,288 | 3.96x | 6.73x | 3.96x | 32,860 KiB |
| Pipelined `CommandStream` | 31,177,515 ns | 3,118 | 13,941,440 | 289,157 | 18.94x | 7.67x | 6.57x | 34,012 KiB |

Pipelining is another 4.78x faster than sequential streaming. Its peak RSS is
0.97% below unary in these separate benchmark processes, and cumulative heap
per 10,000 commands is 7.67x lower.

The native follow-up compares the same pipelined transport against
`CommandBatchStream` envelopes containing 16 reads. Wire bytes are the gRPC
stats handler's actual inbound plus outbound payload `WireLength`; values are
five-run medians from the same process configuration.

| Stream path | Time/10k | ns/command | Heap B/10k | Allocs/10k | Wire B/command | Messages/10k |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| One `CommandRequest` per message | 26.379 ms | 2,638 | 15,635,504 | 299,037 | 41.00 | 10,000 |
| `CommandBatchRequest`, 16 commands | 11.612 ms | 1,161 | 9,675,288 | 107,876 | 37.04 | 625 |

Native envelopes are 2.27x faster, use 1.62x less cumulative heap and 2.77x
fewer allocations, send 1.11x fewer measured wire bytes, and reduce stream
messages 16x. The client-selected batch size is the latency tradeoff: 16 is the
measured throughput point, while smaller envelopes reduce the time a command
waits for its batch to fill.

<a id="compact-typed-protobuf-scalar-batches"></a>
The direct scalar follow-up replaces repeated request/response messages with
packed operation, status, and value-kind columns plus one concatenated result
buffer. Both rows below use 16-command envelopes and are seven-run medians from
the same Ryzen 9 5950X checkout. The generic row is measured again alongside
the new path rather than copied from the older run above.

| 10,000 GET commands | Time/10k | ns/command | Heap B/10k | Allocs/10k | Wire B/command | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `CommandBatchStream` | 8.657 ms | 865.7 | 9,671,736 | 107,740 | 37.04 | baseline |
| `ScalarBatchStream` | 3.911 ms | 391.1 | 2,633,784 | 40,493 | 23.72 | 2.21x CPU, 3.67x heap, 2.66x allocations, 1.56x wire |

The direct path supports GET, EXISTS, string/counter set, counter increment,
and delete. It preserves ordered per-command statuses. Servers configured with
journaling, dirty persistence, replication, or leader enforcement route typed
columns through the existing transactional side-effect executor, retaining
correctness at the cost of some direct-path savings. The existing command batch
stream remains the fallback for non-scalar commands and older clients.
Raw output from the final combined branch is generated at
`build/benchmarks/scalar-protobuf-batch.txt`.

<a id="compact-typed-protobuf-structured-batches"></a>
### Compact Typed Protobuf Structured Batches

The structured follow-up applies the same columnar envelope to map, slice, set,
and priority-queue commands. The workload repeats `PUTMAP`/`PEEKMAP`,
`PUSHSLICE`/`POPSLICE`, `ADDSET`/`HASSET`, and `PUSHPQ`/`POPPQ` in 16-command
envelopes. Both rows are seven-run medians from one combined run on the Ryzen 9
5950X host.

```sh
make bench-structured-batch BIG_WINS_OPS=10000 BENCHTIME=1x COUNT=7
```

| 10,000 mixed structured commands | Time/10k | ns/command | Heap B/10k | Allocs/10k | Wire B/command | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `CommandBatchStream` | 27.743 ms | 2,774 | 10,610,848 | 119,693 | 60.41 | baseline |
| `StructuredBatchStream` | 19.909 ms | 1,991 | 3,586,800 | 77,686 | 33.22 | 1.39x CPU, 2.96x heap, 1.54x allocations, 1.82x wire |

The typed stream supports `PUT_MAP`, `PEEK_MAP`, `TAKE_MAP`, `PUSH_SLICE`,
`POP_SLICE`, `SHIFT_SLICE`, `HEAD_SLICE`, `TAIL_SLICE`, `ADD_SET`,
`REMOVE_SET`, `HAS_SET`, `GET_SET`, `PUSH_PRIORITY`, `PEEK_PRIORITY`,
`POP_PRIORITY`, and `GET_PRIORITY`. Requests carry one value per consuming
operation; clients can express a multi-value mutation as adjacent operations,
or use `CommandBatchStream` to retain the existing multi-value request shape.
Responses retain ordered statuses while packing byte results into one buffer.
Journaling, dirty persistence, replication, and leader enforcement use the
existing side-effect executor. Raw output is generated at
`build/benchmarks/structured-protobuf-batch.txt`.

<a id="bounded-structured-batch-execution"></a>
#### Bounded Structured Batch Execution

The typed stream now validates its columns once and executes map, slice, set,
and priority-queue operations directly. It holds the trie lock for at most four
adjacent commands and aggregates default global telemetry across the request,
reducing a representative 16-command batch from 19 clock reads to one. Four was
selected after measuring lock sizes 2, 4, 8, and 16. The larger sizes completed
the batch faster, but four keeps the critical section conservative while still
removing most lock and command-dispatch overhead.

The executor does not add retained trie state. Detailed or bounded per-key
statistics, local partitions, noncanonical keys/subkeys, journaling, dirty
persistence, replication, and leader-write enforcement retain the existing
command loop. Differential tests compare responses, stored entries, and global
statistics for every supported structured operation; fallback and race tests
cover the compatibility cases.

```sh
make run CMD='go test . -run StructuredBatchDirect -count=1'
make run CMD='go test -race . -run StructuredBatch -count=1'
make run CMD='go test . -run=^none -bench=StructuredBatchDirect -benchmem -benchtime=1s -count=7 -cpu=1'
make run CMD='go test . -run=^none -bench=StructuredBatchReaderPause -benchtime=10x -count=7 -cpu=2'
make bench-structured-batch BIG_WINS_OPS=10000 BENCHTIME=1x COUNT=7
```

| Seven-run median | Per-command command loop | Bounded executor, size 4 | Result |
| --- | ---: | ---: | ---: |
| Focused 8-command batch | 2,312 ns; 560 B; 19 allocs | 1,915 ns; 560 B; 19 allocs | 1.21x faster; heap unchanged |
| Focused 16-command batch | 4,339 ns; 904 B; 30 allocs | 3,383 ns; 904 B; 30 allocs | 1.28x faster; heap unchanged |
| End-to-end gRPC, per command | 1,724 ns | 1,503 ns | 1.15x faster |
| End-to-end gRPC heap / 10k | 3,586,784 B | 3,587,480 B | 1.0002x; measurement noise |
| End-to-end gRPC allocations / 10k | 77,681 | 77,686 | effectively unchanged |
| End-to-end gRPC wire / command | 33.22 B | 33.22 B | unchanged |
| 4,096-command runtime under reader load | 3.970 ms | 1.682 ms | 2.36x faster |
| Observed maximum reader pause | 141,904 ns | 106,013 ns | 1.34x shorter |
| Telemetry clock calls / 16 commands | 19 | 1 | 19x fewer |

The end-to-end baseline samples were `1,597, 1,764, 1,495, 1,764,
1,476, 1,724, 1,865` ns/command. The artifact-producing final samples were
`1,435, 1,512, 1,324, 1,503, 1,462, 1,528, 1,711` ns/command. Protobuf request
and response shapes are unchanged, so the optimization has no bandwidth or
client compatibility effect. Raw final output is in
`build/benchmarks/structured-protobuf-batch.txt` when generated locally.

<a id="shared-structured-batch-keys"></a>
#### Shared Structured-Batch Keys

Repeated same-collection envelopes previously serialized and decoded the key
once per operation. `StructuredBatchRequest.keys` now accepts either one key
per operation or one shared key for the complete envelope. This reuses the
existing protobuf column, so generated request size, field layout, persistent
formats, and distinct-key execution remain unchanged. A shared request expands
one request-local string-header slice after validation. The expansion uses an
explicit protobuf projection rather than copying the generated message and its
runtime mutex state.

Tests were added before validation accepted the compact form. They cover
ordered mixed `PUT_MAP`/`PEEK_MAP`/`TAKE_MAP` execution, journal replay, dirty
tracking, local partitions, the legacy expanded column, and malformed columns.
Focused repeated runs and the race detector passed.

```sh
make run CMD='env HATRIE_BIG_WINS_OPS=10000 go test . -run=NONE -bench="^BenchmarkBigWins/StructuredBatchStreamSharedKey(Repeated)?$$" -benchmem -benchtime=5x -count=15 -cpu=1'
```

Both rows are 15-run medians from the same binary, stream, batch size, server,
and 10,000-command same-key `PEEK_MAP` fixture on the Ryzen 9 5950X host.

| Key column | Time/10k | ns/command | Heap B/10k | Allocs/10k | Wire B/command | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Sixteen repeated key entries | 8.051 ms | 805.1 | 3,362,504 | 64,490 | 36.72 | baseline |
| One shared key entry | 7.186 ms | 718.6 | 2,636,371 | 53,186 | 18.91 | 1.12x CPU, 1.28x heap, 1.21x allocations, 1.94x wire |

The existing mixed structured benchmark retained identical heap and allocation
counts against the untouched revision. Its cross-process CPU samples overlap;
the distinct-key additions are two length comparisons and an allocation-free
return, so there is no credible common-path regression. Older servers reject
the compact form with the existing key-count error; mixed-version clients can
retry with expanded keys.

<a id="shared-structured-batch-subkeys"></a>
#### Shared Structured-Batch Subkeys

Map-heavy envelopes can also repeat the same field name once per map operation.
`StructuredBatchRequest.subkeys` now accepts one shared subkey when at least two
map operations consume it. This compact form is independent of key sharing and
broadcasts only across map operations, so slice, set, and priority-queue
operations may remain interleaved. Positional subkeys, protobuf fields, and
persistent formats are unchanged.

Tests were written against the rejecting validator before implementation. They
cover direct mixed map ordering, interleaved non-map operations with distinct
keys, stray-subkey rejection, journal replay, dirty tracking, local partitions,
and the legacy positional shape. Focused tests passed 20 repetitions and the
race fixture passed five repetitions.

```sh
make run CMD='env HATRIE_BIG_WINS_OPS=10000 go test . -run=NONE -bench="BenchmarkBigWins/StructuredBatchStreamShared(Key|Columns)\\z" -benchmem -benchtime=5x -count=15 -cpu=1'
```

Both rows are 15-run medians from one binary and the same 10,000-command,
16-command-envelope `PEEK_MAP` fixture. The baseline already sends one shared
key; only the subkey representation changes.

| Subkey column | ns/command | Heap B/10k | Allocs/10k | Wire B/command | Improvement |
| --- | ---: | ---: | ---: | ---: | --- |
| Sixteen repeated subkey entries | 721.8 | 2,636,371 | 53,186 | 18.91 | baseline |
| One shared subkey entry | 670.1 | 2,305,907 | 41,915 | 12.35 | 1.08x CPU, 1.14x heap, 1.27x allocations, 1.53x wire |

An eight-pair alternating `100x` control compared ordinary positional mixed
requests against untouched commit `e850892`. Medians were 963.0 ns/command for
the baseline and 953.4 ns/command for the final path; both used exactly 77,606
allocations and heap differed by only a few sampled bytes. The first prototype
combined both expansions in one larger helper, but lost compiler inlining and
added one allocation per shared-key-only envelope. Splitting the helpers
restored the shipped shared-key control exactly. Older servers reject a shared
subkey for multiple map operations; mixed-version clients can retry with the
expanded column.

<a id="shared-structured-batch-values"></a>
#### Shared Structured-Batch Values

Repeated-value envelopes can now send one `StructuredBatchRequest.values`
entry when at least two operations consume the same immutable bytes. The server
expands only the `[][]byte` headers; every entry references the one decoded
payload without copying it. This works across map writes, slice pushes, set
operations, and priority-queue pushes, including interleaved command families.
Positional requests, protobuf fields, value ownership, and persistent formats
are unchanged.

Tests were written against the rejecting validator before implementation. They
cover mixed command families, stray-value rejection, journal replay, dirty
tracking, local partitions, and positional values. Focused tests passed 20
repetitions; the shared-column race fixture passed five repetitions.

```sh
make run CMD='env HATRIE_BIG_WINS_OPS=10000 go test . -run=NONE -bench="BenchmarkBigWins/StructuredBatchStreamSharedValue(Repeated)?\\z" -benchmem -benchtime=5x -count=15 -cpu=1'
```

Both rows are 15-run medians from one binary and the same 10,000-command,
16-command-envelope `HAS_SET` fixture. Both already send one shared key; only
the value representation changes.

| Value column | ns/command | Heap B/10k | Allocs/10k | Wire B/command | Improvement |
| --- | ---: | ---: | ---: | ---: | --- |
| Sixteen repeated value entries | 721.6 | 2,731,184 | 60,051 | 15.16 | baseline |
| One shared value entry | 654.4 | 2,305,612 | 48,777 | 7.662 | 1.10x CPU, 1.18x heap, 1.23x allocations, 1.98x wire |

The existing shared-key/subkey control retained exactly 2,305,907 heap B and
41,915 allocations per 10k commands; its current 664.2 ns/command median
overlaps the prior 670.1 ns measurement. An eight-pair alternating `100x`
control compared ordinary positional requests with the exact `5ac1105` binary:
the baseline and final medians were 929.8 and 920.4 ns/command, both with
77,606 allocations and effectively identical heap. Older servers reject one
value for multiple consuming operations; mixed-version clients can retry with
the expanded column.

<a id="go-1265-toolchain-refresh"></a>
#### Go 1.26.5 Toolchain Refresh

The module minimum and Docker builder were updated from Go 1.20 and Go 1.22,
respectively, to Go 1.26.5. The isolated benchmark held `fastime` at v1.1.9 and
changed only the executing Go toolchain from 1.26.4 to 1.26.5.

| Seven-run median | Go 1.26.4 | Go 1.26.5 | Result |
| --- | ---: | ---: | ---: |
| Command string set | 192.9 ns | 182.3 ns | 1.06x faster |
| Command string get | 168.6 ns | 164.8 ns | 1.02x faster |
| Command counter increment | 243.1 ns | 239.0 ns | 1.02x faster |
| Command TTL refresh | 227.5 ns | 229.9 ns | 1.01x slower |
| Heap / allocations | unchanged | unchanged | no memory regression |

Raw output is generated under
`build/benchmarks/go1.26.4-fastime1.1.9/fastime.txt` and
`build/benchmarks/go1.26.5-fastime1.1.9/fastime.txt`.

<a id="latest-fastime-refresh"></a>
#### Latest Fastime Refresh

With the toolchain fixed at Go 1.26.5, `fastime` was updated from v1.1.9 to
v1.1.10. The newer release uses typed atomic fields and allows its refresh
daemon to stop immediately when its context is canceled. The application API
and 5 ms global refresh cadence are unchanged.

The machine varied between separate benchmark processes, so each production
default is also normalized against the `time.Now` control from the same
process. That ratio isolates the cached-clock effect more reliably than the
absolute median alone.

| Seven-run median | v1.1.9 default / `time.Now` | v1.1.10 default / `time.Now` | Normalized result |
| --- | ---: | ---: | ---: |
| Command string set | 182.3 / 215.1 ns | 195.6 / 227.5 ns | Advantage 1.18x to 1.16x |
| Command string get | 164.8 / 207.7 ns | 165.5 / 209.4 ns | Advantage 1.26x to 1.27x |
| Command counter increment | 239.0 / 275.9 ns | 236.7 / 277.9 ns | Advantage 1.15x to 1.17x |
| Command TTL refresh | 229.9 / 362.3 ns | 224.5 / 376.1 ns | Advantage 1.58x to 1.68x |
| `fastime.Now` clock read | 1.472 ns | 1.520 ns | 1.03x slower |
| `fastime.UnixNanoNow` clock read | 1.385 ns | 1.346 ns | 1.03x faster |
| Heap / allocations | unchanged | unchanged | no memory regression |

Raw v1.1.10 output is generated under
`build/benchmarks/go1.26.5-fastime1.1.10/fastime.txt`.

<a id="cached-default-trie-clock"></a>
#### Cached Default Trie Clock

The default trie clock now uses `github.com/kpango/fastime` v1.1.10. The package
refreshes one process-global timestamp every 5 ms. Trie tests can still replace
`HatTrie.now`, so deterministic expiration and telemetry tests retain exact
control.

This substitution covers cache telemetry, expiration checks, snapshot cutoffs,
and persistence paths that call `HatTrie.currentTime`. It intentionally does
not replace `time.Since`, operation timers, authentication deadlines, election
leases, retry scheduling, or other monotonic elapsed-time decisions. Cached
wall time has no monotonic component and using it there would quantize short
durations or make clock adjustments observable.

```sh
make bench-fastime BENCHTIME=1s COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
make run CMD='go test . -run FastimeClock -count=10'
```

| Seven-run median | `time.Now` | `fastime.Now` | Result |
| --- | ---: | ---: | ---: |
| Clock read | 39.62 ns | 1.511 ns | 26.22x faster |
| Command string set | 228.3 ns | 177.6 ns | 1.29x faster |
| Command string get | 210.8 ns | 162.9 ns | 1.29x faster |
| Command counter increment | 273.1 ns | 226.5 ns | 1.21x faster |
| Command TTL refresh | 365.2 ns | 240.8 ns | 1.52x faster |
| Heap / allocations | unchanged | unchanged | zero clock-path allocation |

The package adds one shared ticker goroutine rather than per-trie state. In the
separate command-process check, maximum RSS was 35,312 KiB before and 35,232
KiB after for string set, which is measurement noise rather than retained
growth. A repeated end-to-end counter run improved from the 432.5 ns baseline
to 405.2 ns (1.07x). Raw clock and same-process A/B output is generated at
`build/benchmarks/fastime.txt`.

The production-default benchmark case also guards the constructor path. A
follow-up attempt to call `fastime.UnixNanoNow` directly for default global
telemetry was rejected and reverted: it preserved heap behavior but regressed
all four seven-run command medians.

| Rejected direct Unix telemetry read | Cached `time.Time` path | Direct Unix-nanosecond path | Result |
| --- | ---: | ---: | ---: |
| Command string set | 178.8 ns | 187.0 ns | 1.05x slower |
| Command string get | 157.7 ns | 168.0 ns | 1.07x slower |
| Command counter increment | 241.1 ns | 245.5 ns | 1.02x slower |
| Command TTL refresh | 213.5 ns | 225.2 ns | 1.05x slower |
| Heap / allocations | unchanged | unchanged | no memory benefit |

Remaining standard-library clock reads are intentional. They protect
monotonic elapsed measurements, authentication expiry, election leases,
retries, circuit breakers, or sub-refresh-interval operational duration
reporting; replacing them would introduce a correctness or precision cost for
no meaningful throughput gain.

Run the same sequential transport comparison across representative command
families:

```sh
make bench-hatrie-transport-features HATRIE_TRANSPORT_BENCH='^BenchmarkCommandTransportFeature/(GRPC|GRPCStream)/(StringSet|StringGet|CounterInc|MapPut|MapPeek)$' BENCHTIME=1000x
```

| Command feature | Unary ns/op | Stream ns/op | Speedup | Unary B/op | Stream B/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| String set | 67,028 | 18,123 | 3.70x | 10,872 | 1,665 |
| String get | 60,266 | 15,316 | 3.94x | 10,697 | 1,584 |
| Counter increment | 64,068 | 17,464 | 3.67x | 10,740 | 1,622 |
| Map put | 64,308 | 19,956 | 3.22x | 11,653 | 2,414 |
| Map peek | 62,284 | 15,495 | 4.02x | 10,724 | 1,607 |

Both RPCs call the same command executor. The stream removes repeated unary RPC
setup and permits HTTP/2 flow-control-bounded pipelining; it does not weaken
command ordering or durability acknowledgements.

<a id="exact-scalar-mutation-dispatch"></a>
#### Exact Scalar Mutation Dispatch Rollback

An exact uppercase fast path for `SETSTR` without expiration, `INC`, and
`EXPIRE` was tested before command normalization. The candidate called the
same checked mutations and returned the same responses as the generic switch;
focused parity, overflow, TTL, and key-validation tests passed ten times.

The strict performance control compiled both test binaries from the same tree,
including the candidate helper and tests. The baseline disabled only the
production helper call. Nine 300 ms benchmark pairs alternated baseline-first
and candidate-first order. This removed the test-layout and process-order bias
that had made earlier separate binaries look 1.08x to 1.11x faster.

| Nine-run median | Generic dispatch | Exact helper | Result |
| --- | ---: | ---: | ---: |
| String set, cached clock | 200.2 ns | 203.8 ns | 1.02x slower |
| String get control, cached clock | 181.1 ns | 185.9 ns | 1.03x slower |
| Counter increment, cached clock | 277.3 ns | 272.4 ns | 1.02x faster |
| TTL refresh, cached clock | 238.1 ns | 239.2 ns | 1.005x slower |
| String set, `time.Now` | 243.4 ns | 243.2 ns | neutral |
| String get control, `time.Now` | 232.0 ns | 227.6 ns | 1.02x faster |
| Counter increment, `time.Now` | 312.3 ns | 318.4 ns | 1.02x slower |
| TTL refresh, `time.Now` | 415.8 ns | 428.4 ns | 1.03x slower |
| Heap / allocations | unchanged | unchanged | no memory benefit |

The raw cached-clock samples were:

| Command | Generic ns/op | Exact-helper ns/op |
| --- | --- | --- |
| String set | 219.8, 200.2, 181.9, 197.2, 197.1, 225.6, 191.0, 225.6, 200.5 | 203.8, 171.6, 218.5, 206.6, 196.1, 204.4, 198.5, 219.1, 188.0 |
| String get control | 187.0, 162.5, 180.4, 201.1, 172.1, 181.1, 202.9, 182.2, 169.6 | 193.6, 171.9, 161.9, 200.5, 166.6, 187.7, 185.1, 185.9, 198.7 |
| Counter increment | 270.0, 281.7, 269.9, 278.9, 289.7, 272.3, 273.2, 277.3, 314.2 | 249.7, 272.4, 246.6, 278.5, 315.3, 245.3, 269.6, 277.8, 314.7 |
| TTL refresh | 239.1, 212.9, 223.0, 250.5, 228.2, 244.6, 267.6, 238.1, 233.4 | 260.1, 241.6, 209.3, 249.0, 231.9, 233.3, 297.2, 231.5, 239.2 |

Two earlier layouts were also rejected. Adding the cases to the large exact
command switch slowed its GET control from 166.1 to 180.8 ns. Hoisting GET
ahead of that switch still slowed the control from 166.2 to 175.5 ns. Cgo
`noescape`/`nocallback` annotations separately regressed the set/get/inc/TTL
medians by 1.03x/1.10x/1.15x/1.03x, and a known-valid-key GET helper measured
121.7 ns against the checked path's 120.1 ns. All production candidates and
their temporary tests were removed, so this pass retains no runtime tradeoff.

<a id="complete-tagged-structured-storage"></a>
### Complete Tagged Structured Storage

Binary database, snapshot, replication-value, and binary-journal records now
always use the version-2 tagged value tree for dynamic structured fields. The
codec adds direct varints for signed and unsigned integers, raw byte payloads,
a staged-XOR representation, and direct JSON-equivalent encoding for the public
priority-queue value type. Other JSON-marshalable concrete Go values normalize
once before tagged encoding. Version-1 tagged values and legacy inner-JSON
payloads remain readable; selecting the top-level JSON formats remains the
operator-controlled fallback.

The focused fixture encodes and decodes seven records covering a small map,
nested public priority queues inside map/queue/Top-K/radix/reservoir values, and
an unbuilt XOR filter with a staged value. Values are seven-run medians on the
Ryzen 9 5950X host.

```sh
make bench-structured-storage-codec BENCHTIME=1000x COUNT=7
```

| Seven-record cycle | Inner-JSON selection | Complete tagged binary | Improvement |
| --- | ---: | ---: | ---: |
| Encode time | 3,846 ns | 2,551 ns | 1.51x faster |
| Encode heap | 1,856 B | 768 B | 2.42x lower |
| Encode allocations | 24 | 14 | 1.71x fewer |
| Decode time | 11,651 ns | 5,743 ns | 2.03x faster |
| Decode heap | 11,352 B | 5,904 B | 1.92x lower |
| Decode allocations | 139 | 91 | 1.53x fewer |
| Stored record bytes | 674 B | 410 B | 1.64x smaller (39.2%) |

Raw nanosecond samples were:

| Path | Seven samples |
| --- | --- |
| Inner-JSON encode | `4468, 4673, 4317, 3724, 3846, 3750, 3826` |
| Tagged encode | `2690, 2524, 2316, 2597, 3349, 2551, 2284` |
| Inner-JSON decode | `11651, 12030, 11390, 11780, 11052, 11440, 12900` |
| Tagged decode | `7303, 5743, 5719, 8061, 5395, 6097, 5254` |

The reproducible command writes the current raw output to
`build/benchmarks/structured-storage-codec.txt`.

<a id="adaptive-native-bucket-size-classes"></a>
### Adaptive Native Bucket Size Classes

The native `ahtable` previously called `realloc` to the exact used size for
every insertion and every deletion from a nonempty hash slot. Slots now keep a
contiguous capacity side array. A growth reserves exactly one additional record
of the size just appended, avoiding broad percentage slack when keys are large
or uneven. Empty slots are freed immediately; a nonempty slot shrinks with a
6.25% reserve after utilization falls below one third.

The isolated fixture inserts 100,000 deterministic keys into 4,096 slots,
deletes half, inserts 50,000 replacements, verifies every retained value, and
reports native used/capacity bytes plus process RSS. Values are seven-run
medians on the Ryzen 9 5950X host.

```sh
make bench-native-ahtable-allocator NATIVE_AHTABLE_KEYS=100000 NATIVE_AHTABLE_SLOTS=4096 COUNT=7
```

| Isolated native table | Exact resize | Adaptive class | Improvement |
| --- | ---: | ---: | ---: |
| Insert 100k | 24.458 ms | 23.711 ms | 1.03x faster |
| Delete/reinsert 50k | 21.053 ms | 17.460 ms | 1.21x faster |
| Insert resizes | 100,000 | 51,021 | 1.96x fewer |
| Total resizes | 200,000 | 58,925 | 3.39x fewer |
| Live slot bytes | 3,900,000 B | 3,900,000 B | unchanged |
| Retained slot capacity | 3,900,000 B | 4,172,719 B | 7.0% higher |
| Maximum RSS | 5,684 KiB | 5,940 KiB | 4.5% higher |

Raw isolated milliseconds were:

| Path | Seven samples |
| --- | --- |
| Exact insert | `24.292, 23.668, 28.229, 26.489, 24.458, 19.431, 25.565` |
| Adaptive insert | `22.144, 24.618, 23.807, 23.782, 22.193, 21.717, 23.711` |
| Exact churn | `21.053, 20.232, 24.110, 22.775, 20.885, 17.806, 26.004` |
| Adaptive churn | `20.320, 16.278, 16.394, 17.460, 21.249, 18.566, 15.718` |

The end-to-end guard uses the existing 100,000 insert/90,000 delete cache
fixture, including Go backing pools, Merkle tracking, cgo calls, and GC. Fifteen
sequential samples compare commit `a119dfd` with the final allocator:

```sh
make bench-big-wins BIG_WINS_BENCH='^BenchmarkBigWins/ChurnRetentionBaseline$' BIG_WINS_KEYS=100000 BENCHTIME=1x COUNT=15
```

| Full cache cycle | Exact resize | Adaptive class | Change |
| --- | ---: | ---: | ---: |
| Median time | 255.777 ms | 247.534 ms | 1.03x faster |
| Maximum process RSS | 75,816 KiB | 76,336 KiB | 0.7% higher |
| Go heap/allocations | 34.71 MB / 381.2k | 34.71 MB / 381.2k | unchanged |

The isolated raw output is written to
`build/benchmarks/native-ahtable-allocator.txt`. The capacity side array and
bounded slack are the explicit memory cost; the full-process run shows their
impact after the Go runtime and backing pools are included.

<a id="grouped-storage-headers"></a>
### Grouped Storage Headers

Each `HatTrie` previously called 19 public typed-storage constructors. Their
empty slice and reusable-index fields are valid zero values, but every returned
header escaped as an independent heap object. A non-race allocation test was
added first requiring no more than eight allocations for
`CreateHatTrieWithDiskDir` plus `Destroy`; it failed at 25. A forced-GC test
also pins the interior storage pointers and a live string operation.

The internal constructor now allocates one 1,784-byte backing object for 18
storage headers and retains the same typed pointers to its fields. `MapStorage`
remains separate: its 184-byte header plus the group use the 192-byte and
1,792-byte Go size classes, exactly matching the prior 1,984 storage-header
bytes. Putting all 19 headers in one 1,968-byte object rounded to the 2,048-byte
class. That first prototype was 1.06x faster and reduced 25 allocations to 7,
but raised total constructor heap by 64 bytes, so it was rejected.

```sh
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkHatTrieConstruction -benchmem -benchtime=10000x -count=10'
```

| Empty cache construction | Separate headers | Map-separated group | Improvement |
| --- | ---: | ---: | ---: |
| Median time | 16,148 ns | 15,320 ns | 1.05x faster |
| Cumulative heap | 3,360 B | 3,360 B | unchanged |
| Allocations | 25 | 8 | 3.13x fewer allocations |

Public `Create*Storage` APIs retain their independent allocation and empty
slice behavior. Internal command code still reads the same typed pointers;
there is no added branch or indirection and no value, GC-lifetime, compaction,
wire, snapshot, or persistence format change.

<a id="deferred-optional-maps"></a>
### Deferred Optional Maps

After storage-header grouping, every empty trie still allocated an expiration
index and a per-key telemetry map. Both are optional: TTL metadata is only
written by the first future expiration, and per-key telemetry defaults to off.
The allocation test was tightened first from eight to six allocations and a
lifecycle test required nil default maps, first-use activation, off-mode
release, memory compaction, and successful reactivation. Both failed against
the eager constructor before production code changed.

The constructor now leaves both maps nil. Enabling bounded or full telemetry
allocates its writable map before any tracked command. The first distinct TTL
allocates the expiration index; ordinary commands and updates to existing TTLs
retain their prior paths. Disabling telemetry releases its map, and memory
compaction releases empty expiration or disabled telemetry metadata while
preserving a writable map for telemetry that remains enabled.

```sh
make run CMD='go test . -run="TestHatTrieConstructionUsesGroupedStorageBacking|TestHatTrieDefersOptionalMapsUntilEnabled" -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkHatTrieOptionalMapLifecycle -benchmem -benchtime=100000x -count=7'
make run CMD='go test . -run=NONE -bench=BenchmarkHatTrieOptionalMapExpirationScheduling -benchmem -benchtime=10000x -count=20'
```

CPU rows below are same-binary medians against eager controls. Heap and
allocation rows are from the direct before/after lifecycle fixtures, excluding
the control benchmark's closure overhead.

| Complete lifecycle | Eager maps | Deferred maps | Improvement |
| --- | ---: | ---: | ---: |
| Empty construction | 14,976 ns; 3,360 B; 8 allocs | 14,721 ns; 3,264 B; 6 allocs | 1.02x faster, 96 B lower, 1.33x fewer allocations |
| Construction plus first TTL | 16,230 ns; 3,696 B; 12 allocs | 15,666 ns; 3,648 B; 11 allocs | 1.04x faster, 48 B lower, one fewer allocation |
| Construction plus telemetry activation/write | 14,511 ns; 3,696 B; 11 allocs | 14,411 ns; 3,648 B; 10 allocs | CPU neutral within 0.7%, 48 B lower, one fewer allocation |
| Schedule 10,000 distinct TTLs | 530.5 ns/key; 271 B/key | 529.4 ns/key; 271 B/key | CPU-neutral within 0.2%; heap and allocations unchanged |

The default path retains no replacement state or background work. TTL, key
statistics, compaction, snapshot, restore, partition, wire, and persistent
format behavior are unchanged.

<a id="single-representation-string-storage"></a>
### Single-Representation String Storage

String values previously shared `BytesStorage` with raw bytes. Every occupied
string slot retained a `[]byte` copy, the original `string`, both parallel slice
descriptors, and a validity byte. Every insertion or replacement therefore
copied the complete payload and allocated a byte backing array even though
ordinary reads returned the stored string.

Strings and bytes now have independent reusable-index pools selected by the
existing `HatValue` type. A string slot retains only the immutable string;
`GetString` remains zero-allocation. `GetBytes` intentionally materializes the
same caller-owned clone it returned before. Memory compaction has independent
string and byte remap tables, and snapshot generation swaps transfer both pools.

The dedicated fixture inserts or replaces 100,000 unique 256-byte strings whose
input backing is allocated before measurement. Values are seven-run medians on
the Ryzen 9 5950X host.

```sh
make bench-string-storage STRING_STORAGE_BENCH_KEYS=100000 BENCHTIME=1x COUNT=7
```

| Operation | Mirrored string/bytes | Dedicated string pool | Improvement |
| --- | ---: | ---: | ---: |
| Insert 100k x 256 B | 236.169 ms | 187.566 ms | 1.26x faster |
| Incremental retained heap | 303.5 B/key | 18.87 B/key | 16.08x lower |
| Insert cumulative heap | 48,008,280 B | 8,923,504 B | 5.38x lower |
| Insert allocations | 100,080 | 28 | 3,574x fewer |
| Replace 100k x 256 B | 43.317 ms | 24.978 ms | 1.73x faster |
| Replace cumulative heap | 25,600,000 B | 0 B | eliminated |
| Replace allocations | 100,000 | 0 | eliminated |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Mirrored insert | `239.665, 229.609, 235.570, 246.151, 226.826, 236.169, 241.039` |
| Dedicated insert | `187.572, 186.007, 186.056, 183.112, 188.237, 193.496, 187.566` |
| Mirrored replace | `40.692, 43.317, 38.757, 48.597, 47.551, 48.366, 41.749` |
| Dedicated replace | `25.219, 25.539, 24.839, 24.874, 24.098, 30.694, 24.978` |

The existing tiny-value fixture also falls from 63.57 to 18.92 retained
B/key (3.36x lower), while short `SETSTR` improves from 413.2 to 393.0 ns and
drops from one allocation to zero. On atomic restore of 100,000 256-byte
strings, cumulative heap falls from 108,816,416 to 69,731,720 B (1.56x lower),
allocations fall from 500,117 to 400,067 (1.25x fewer), and median time improves
from 273.039 to 243.218 ms (1.12x faster). Raw dedicated-fixture output is
generated at `build/benchmarks/string-storage.txt`.

<a id="live-string-slot-replacement"></a>
#### Live String-Slot Replacement

Cache string replacement previously called the exported `StringStorage.Put`,
which assigns the slot and then asks the reusable-index bitset to revive that
index. The four cache callers reach this operation only through a live string
`HatValue`; a live trie entry cannot point at a deleted storage slot, so the bit
lookup was invariantly false. They now use a private live-slot replacement that
performs the same bounds check and assignment without touching reusable state.
The exported `Put` is unchanged and still revives a deleted non-tail index.

Tests were added before the private method. They cover public delete/revival,
stale reusable-stack handling, live replacement with another reusable slot,
invalid indexes, duplicate upsert TTL clearing, write accounting, stable
storage indexes, and append/prepend behavior. The focused and complete-path
benchmarks execute the old and new paths in alternating order in one binary:

```sh
make run CMD='go test . -run=Test\(StringStoragePutRevivesDeletedIndex\|StringStorageReplaceActiveLeavesReusableIndexesUnchanged\|LiveStringReplacementPreservesCacheBehavior\) -count=1 -v'
make run CMD='go test . -run=NONE -bench=BenchmarkStringStorageReplaceActiveAlternating -benchtime=50x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkStringUpsertCheckedAlternating -benchtime=20x -count=9 -cpu=1'
```

| String replacement, nine-run median | Public `Put` control | Live-slot path | Result |
| --- | ---: | ---: | ---: |
| Primitive duplicate | 3.057 ns | 1.416 ns | 2.16x faster |
| Primitive true replacement | 2.731 ns | 1.532 ns | 1.78x faster |
| Complete duplicate `UpsertStringChecked` | 118.8 ns | 117.1 ns | 1.015x faster |
| Complete true replacement | 110.3 ns | 109.0 ns | 1.012x faster |

All rows remain allocation-free. Inserts still use `Add`; external or recovery
code that intentionally revives a deleted slot still uses `Put`. Reads, string
ownership, retained memory, TTL, telemetry, snapshots, journals, persistence,
replication, and wire formats are unchanged.

<a id="idempotent-string-assignment-rollback"></a>
#### Idempotent String Assignment Rollback

An audit candidate tried to avoid `StringStorage.Put` when an existing string
slot already contained the requested immutable value. The refined prototype
used one bounds check, compared the slot directly, and otherwise performed the
same assignment and reusable-index update as production. No cache code was
changed before the storage primitive cleared its CPU gate.

The test-first behavior fixture confirmed that a duplicate string upsert must
retain its storage index and value while still clearing TTL and incrementing
write accounting. A temporary same-binary benchmark timed 262,144 writes per
block, alternated candidate/control order, covered both identical values and
alternating real replacements, and repeated each case nine times on one
logical CPU.

| String slot update, nine-run median | Direct assignment | Equality first | Result |
| --- | ---: | ---: | ---: |
| Duplicate value | 2.623 ns | 3.332 ns | Candidate 1.27x slower |
| True replacement | 2.811 ns | 3.017 ns | Candidate 1.07x slower |

Writing the same two-word string header is cheaper than checking equality
first, even before complete command overhead. The prototype, behavior fixture,
and benchmark control were removed. Production retains direct assignment, so
there is no added branch, code path, memory, format, or runtime cost.

<a id="packed-small-map-storage"></a>
### Packed Small-Map Storage

The previous map pool allocated a Go map header and first bucket for every map
key, so one through eight fields retained the same approximately 354.5 B/map.
One- and two-field maps now live in a packed two-entry pool. The existing
`HatValue` map type is unchanged; a private negative backing index selects the
packed pool, and the third distinct field promotes once to the unchanged Go-map
pool. Full replacement can move a map back to the packed representation.

Correctness coverage exercises nested-value ownership, update/take/reinsert,
promotion, full replacement, sparse packed-pool compaction, JSON escaping,
snapshot round-trip, and the existing command, LevelDB, monitoring, and memory
compaction paths. Wire, snapshot, journal, and persistent record formats remain
logical maps and do not expose packed indexes.

The retained-memory fixture inserts 100,000 maps. Input keys and field maps are
created before measurement. Each result is the median of three one-pass runs on
the Ryzen 9 5950X host.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkMapStorageLayout100k -benchtime=1x -count=3 -benchmem'
```

| Fields/map | Baseline ns/map | Packed ns/map | Baseline retained B/map | Final retained B/map | Result |
| ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 2,749 | 2,625 | 354.5 | 84.00 | 1.05x faster, 4.22x lower retained heap |
| 2 | 3,108 | 2,715 | 354.5 | 84.00 | 1.14x faster, 4.22x lower retained heap |
| 4 | 3,280 | 2,948 | 354.5 | 354.5 | Large-map representation and retention unchanged |
| 8 | 3,079 | 3,026 | 354.5 | 354.5 | Large-map representation and retention unchanged |
| 16 | 5,053 | 4,491 | 1,258 | 1,258 | Large-map representation and retention unchanged |

For one-field maps, retained objects fall from 2.000 to 0.00025/map.
Timed insertion allocation falls from 42,203,568 B and 200,064 allocations to
40,478,848 B and 29 allocations: 1.04x lower cumulative heap and 6,899x fewer
allocations. Two-field results are equivalent.

Operation medians use five 500 ms samples with the same test binary. The
full-map JSON row uses seven two-second samples at `-cpu=1` against a clean
clone of the preceding commit.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkMapStorageOperations -benchtime=500ms -count=5 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/MapGet -benchtime=2s -count=7 -benchmem -cpu=1'
```

| Operation | Baseline | Final | Improvement |
| --- | ---: | ---: | ---: |
| Update existing small field | 354.3 ns; 0 allocs | 239.7 ns; 0 allocs | 1.48x faster |
| Peek small field | 99.23 ns; 0 allocs | 90.77 ns; 0 allocs | 1.09x faster |
| Take then reinsert small field | 1,088 ns; 336 B; 2 allocs | 487.4 ns; 0 B; 0 allocs | 2.23x faster; allocation eliminated |
| Replace two fields then promote third | 1,030 ns; 336 B; 2 allocs | 1,007 ns; 336 B; 2 allocs | 1.02x faster; cost unchanged |
| Update existing eight-field map | 340.7 ns; 0 allocs | 256.0 ns; 0 allocs | 1.33x faster |
| Peek eight-field map | 104.0 ns; 0 allocs | 90.39 ns; 0 allocs | 1.15x faster |
| Full one-field map JSON command | 1,148 ns; 152 B; 3 allocs | 511.4 ns; 24 B; 1 alloc | 2.24x faster, 6.33x lower heap, 3x fewer allocs |

Exact plain-string `PUTMAP` previously converted the new string to an escaping
interface before replacing a packed field, even when the stored string was
identical. The typed packed-map path now compares the immutable strings first
and reuses the stored interface for an idempotent write. It still records the
same write and returns the same response. Different strings, generic values,
and promotion retain normal ownership and replacement semantics; promoted Go
maps explicitly delegate to the prior generic operation.

```sh
make run CMD='go test . -run TestAdaptiveMapPlainStringDuplicateAndReplacement -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/MapPut -benchtime=1000000x -count=9 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkMapPlainStringPutAdaptiveAlternating -benchtime=200x -count=9 -cpu=1'
```

| Plain-string write, median | Generic control | Typed packed path | Improvement |
| --- | ---: | ---: | ---: |
| Complete duplicate `PUTMAP` | 258.2 ns; 16 B; 1 alloc | 198.8 ns; 0 B; 0 allocs | 1.30x faster; allocation eliminated |
| Packed storage duplicate | 31.88 ns | 6.981 ns | 4.57x faster |
| Packed storage replacement | 32.00 ns | 29.93 ns | 1.07x faster |
| Promoted-map duplicate | 44.81 ns | 44.57 ns | Neutral within 0.6% |
| Promoted-map replacement | 45.89 ns | 45.76 ns | Neutral within 0.3% |

Map indexes, values, output bytes, telemetry, snapshot, journal, database,
replication, and wire formats are unchanged.

Exact `PEEKMAP` previously serialized a non-string field while holding the
exclusive cache lock. A deterministic blocking-marshaler test proved that a
field replacement could not complete during a 100 ms gate and remained blocked
until response encoding was released. Stored nested maps and slices are cloned
on write, and field updates replace the stored interface, so the exact command
now captures that stable point-in-time reference, records the read, unlocks,
and only then serializes it. Strings return from the same captured reference
without encoding.

The same-binary benchmark runs both orderings to cancel host drift. The rows
are pooled medians from seven 500 ms samples per order on one logical CPU:

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkMapPeekCommandLockScope -benchmem -benchtime=500ms -count=7 -cpu=1'
```

| Exact `PEEKMAP` core | Lock-held encoding | Encode after unlock | Result |
| --- | ---: | ---: | ---: |
| String field | 34.86 ns; 0 B; 0 allocs | 29.68 ns; 0 B; 0 allocs | 1.17x faster; memory unchanged |
| Structured field | 508.4 ns; 152 B; 3 allocs | 456.4 ns; 152 B; 3 allocs | 1.11x faster; memory unchanged |

The response remains linearized at the successful field lookup. A concurrent
replacement can complete while the prior response is still encoding, but it
cannot mutate the captured nested value. Missing fields, cold references,
read accounting, output JSON, persistence, and public ownership are unchanged.

The full-map path initially materialized a temporary Go map and measured 1,499
ns, 488 B, and 5 allocations. That candidate was not retained. The final path
writes the two-entry JSON object directly with generic nested-value fallback;
its output is regression-tested against the existing encoder, including control
characters, HTML-sensitive bytes, Unicode separators, integer bounds, and
nested structures.

<a id="packed-small-string-set-storage"></a>
### Packed Small String-Set Storage

The previous small-set representation allocated one slice backing array plus
one interface payload object per member. Empty, one-, and two-member
plain-string sets now use separate packed pools selected by private negative
backing indexes.
Adding a third distinct member or a non-string value promotes once to the
unchanged generic set representation. Full replacement can move a set back to
a packed pool. Logical values remain the only representation exposed through
commands, monitoring, snapshots, LevelDB, and memory compaction.

Tests were added before the storage change for duplicate add, remove/reinsert,
promotion, mixed nested values, clone ownership, compaction, and snapshot
round-trip. Additional representation tests cover pool reuse, packed-index
remapping, packed snapshot restore, and JSON-escaped string identities. The
latter caught and fixed an older raw-quote identity shortcut for promoted sets.

The retained-memory fixture inserts 100,000 sets from inputs allocated before
measurement. Results are medians of three one-pass runs on the Ryzen 9 5950X.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkSetStorageLayout100k -benchtime=1x -count=3 -benchmem'
```

| Members/set | Baseline ns/set | Final ns/set | Baseline retained B/set | Final retained B/set | Baseline retained objects/set | Final retained objects/set | Result |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 2,262 | 1,622 | 94.36 | 18.87 | 2.000 | 0.00026 | 1.39x faster; 5.00x lower heap; about 7,692x fewer objects |
| 2 | 2,369 | 1,667 | 142.4 | 36.98 | 3.000 | 0.00026 | 1.42x faster; 3.85x lower heap; about 11,538x fewer objects |
| 3 | 2,682 | 2,408 | 430.4 | 430.5 | 5.001 | 5.001 | Generic retention unchanged within noise; 1.11x faster |
| 8 | 3,397 | 2,938 | 510.4 | 510.4 | 10.00 | 10.00 | Generic retention unchanged; 1.16x faster |
| 16 | 4,825 | 4,359 | 1,542 | 1,543 | 20.00 | 20.00 | Retention within measurement noise; 1.11x faster |

For one-member sets, timed insertion falls from 30,247,776 B and 400,063
allocations to 8,923,504 B and 28 allocations: 3.39x lower cumulative heap and
about 14,288x fewer allocations. Two-member insertion falls from 38,252,544 B
and 600,073 allocations to 17,764,960 B and 29 allocations: 2.15x lower heap
and about 20,692x fewer allocations. The two additional empty pool descriptors
cost 160 fixed bytes per cache instance; about three one-member or two
two-member sets recover that fixed cost.

Operation medians use five 500 ms samples with the same test binary:

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkSetStorageOperations -benchtime=500ms -count=5 -benchmem'
```

| Operation | Baseline | Final | Improvement |
| --- | ---: | ---: | ---: |
| Duplicate add, one member | 229.1 ns; 32 B; 3 allocs | 109.5 ns; 0 B; 0 allocs | 2.09x faster; allocations eliminated |
| Membership, two members | 135.5 ns; 16 B; 2 allocs | 67.57 ns; 0 B; 0 allocs | 2.01x faster; allocations eliminated |
| Remove then add, one member | 634.9 ns; 64 B; 6 allocs | 402.7 ns; 16 B; 1 alloc | 1.58x faster; 4x lower heap; 6x fewer allocs |
| Replace two then promote third | 982.1 ns; 497 B; 11 allocs | 812.1 ns; 392 B; 8 allocs | 1.21x faster; 1.27x lower heap; 1.38x fewer allocs |
| Membership, eight members | 150.3 ns; 32 B; 2 allocs | 131.9 ns; 16 B; 1 alloc | 1.14x faster; 2x lower heap and allocs |
| Read two sorted members | 114.5 ns; 32 B; 1 alloc | 92.73 ns; 32 B; 1 alloc | 1.23x faster; allocation cost unchanged |

Command GET previously expanded every packed set into a temporary public `Set`
before JSON encoding it. Packed strings now write the same sorted JSON array
directly through the existing canonical string escaper. Tests compare empty,
one-, two-, and promoted-set output with the generic encoder, including control
bytes, HTML-sensitive strings, invalid UTF-8, Unicode, and U+2028/U+2029.

Nine alternating 300 ms A/B pairs used otherwise identical test binaries
pinned to one CPU:

```sh
make run CMD='go test . -run none -bench BenchmarkPackedStringSetCommandGet -benchmem -benchtime=500ms -count=9 -cpu=1'
```

| Command GET, nine-run median | Temporary public set | Direct packed JSON | Improvement |
| --- | ---: | ---: | ---: |
| Empty | 245.3 ns; 64 B; 3 allocs | 76.35 ns; 0 B; 0 allocs | 3.21x faster; allocation-free |
| One string | 338.3 ns; 88 B; 4 allocs | 134.9 ns; 16 B; 1 alloc | 2.51x faster; 5.50x lower heap; 4x fewer allocs |
| Two strings | 379.1 ns; 112 B; 4 allocs | 153.1 ns; 16 B; 1 alloc | 2.48x faster; 7.00x lower heap; 4x fewer allocs |
| Three-string promoted control | 636.4 ns; 184 B; 5 allocs | 616.2 ns; 184 B; 5 allocs | CPU 1.03x faster within noise; memory unchanged |

A later promoted-set follow-up keeps the required sorted JSON-key scratch but
writes stored values directly in that order instead of allocating and cloning
a second public `Set`. Tests added before production changes cover generic
nested one/two-member sets, promoted mixed values, removal to empty, canonical
ordering/escaping, and a stored marshaler that starts failing after insertion.
Baseline and final promoted rows are medians of seven 500 ms runs on one
logical CPU:

```sh
make run CMD='go test . -run=NONE -bench="^BenchmarkPackedStringSetCommandGet/Promoted" -benchmem -benchtime=500ms -count=7 -cpu=1'
```

| Promoted command GET | Sorted keys plus cloned values | Sorted keys plus direct values | Improvement |
| --- | ---: | ---: | ---: |
| Three strings | 582.0 ns; 184 B; 5 allocs | 355.6 ns; 72 B; 2 allocs | 1.64x faster; 2.56x lower heap; 2.50x fewer allocs |
| Sixteen strings | 2,016 ns; 760 B; 5 allocs | 1,637 ns; 448 B; 2 allocs | 1.23x faster; 1.70x lower heap; 2.50x fewer allocs |
| Four mixed/nested values | 1,385 ns; 752 B; 10 allocs | 1,056 ns; 264 B; 4 allocs | 1.31x faster; 2.85x lower heap; 2.50x fewer allocs |

The first placement put promoted dispatch inside `SetStorage.jsonString` and
regressed existing packed one/two-string reads from 130.6/150.4 ns to
144.9/165.9 ns, or 1.11x/1.10x slower. It was rejected. The retained command-
level route restores the shared packed encoder byte-for-byte; detached-baseline
controls measured packed empty/one/two reads at 75.61/130.6/150.4 ns versus
73.16/123.9/147.9 ns final, with identical zero/one allocations.

The returned bytes, lexical member order, read telemetry, storage layout,
snapshots, journals, replication payloads, and wire schema are unchanged. The
public `GetSet` clone path remains unchanged.

The first candidate boxed packed strings during reads and made two-member
reads 1.31x slower with 2x heap and 3x allocations; it was not retained. The
final pools preserve interface payloads, so read ownership and allocation cost
match the baseline. Wire and persistence formats are unchanged.

<a id="packed-small-slice-storage"></a>
### Packed Small-Slice Storage

Fresh empty, one-, and two-value slices now use two packed pools selected by
private negative backing indexes. The third value promotes once to the existing
ring deque. A promoted key stays generic on later replacement, even when it
shrinks, which prevents alternating two/three-value workloads from repeatedly
converting representations. Deletion and recreation select the packed layout
again. Logical values, wire encoding, snapshots, LevelDB records, monitoring,
and compaction formats are unchanged.

Behavior tests were added before the storage change for nil versus non-nil
empty slices, nested-value ownership, push/pop/shift ordering, pool reuse,
slot clearing, packed-to-generic promotion, compaction remapping, and snapshot
restore. The retained-memory fixture inserts 100,000 values from preallocated
inputs. Baseline results use detached commit `efacc3d`; final results use the
candidate in the same environment. Figures are stable layout metrics from five
one-pass runs.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkSliceStorageLayout100k -benchtime=1x -count=5 -benchmem -cpu=1'
```

| Workload, 100k keys | Baseline retained B/value | Final retained B/value | Baseline retained objects/value | Final retained objects/value | Retained improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| Upsert empty | 46.23 | 27.39 | 0.00025 | 0.00025 | 1.69x lower heap; objects unchanged |
| Upsert one value | 62.23 | 27.39 | 1.000 | 0.00025 | 2.27x lower heap; about 4,000x fewer objects |
| Upsert two values | 78.23 | 46.23 | 1.000 | 0.00025 | 1.69x lower heap; about 4,000x fewer objects |
| Push one value into new key | 110.2 | 27.39 | 1.000 | 0.00025 | 4.02x lower heap; about 4,000x fewer objects |
| Push two values into new key | 110.2 | 46.23 | 1.000 | 0.00025 | 2.38x lower heap; about 4,000x fewer objects |
| Upsert 3 / 8 / 16 values | 94.23 / 174.2 / 302.2 | 94.23 / 174.2 / 302.2 | 1.000 | 1.000 | Byte-for-byte unchanged generic retention |

| Workload, 100k keys | Baseline timed heap / allocs | Final timed heap / allocs | Improvement |
| --- | ---: | ---: | ---: |
| Upsert empty | 22,216,352 B / 29 | 12,969,960 B / 28 | 1.71x lower heap |
| Upsert one value | 23,816,352 B / 100,029 | 12,969,960 B / 28 | 1.84x lower heap; about 3,572x fewer allocs |
| Upsert two values | 25,416,352 B / 100,029 | 22,216,352 B / 29 | 1.14x lower heap; about 3,449x fewer allocs |
| Push one value into new key | 28,616,352 B / 100,029 | 12,969,960 B / 28 | 2.21x lower heap; about 3,572x fewer allocs |
| Push two values into new key | 28,616,352 B / 100,029 | 22,216,352 B / 29 | 1.29x lower heap; about 3,449x fewer allocs |
| Promote two to three values | 31,816,352 B / 200,029 | 28,616,400 B / 100,030 | 1.11x lower heap; 2.00x fewer allocs |

The distinct-key promotion control retains the same 110.2 B/value on both
sides. Its median is neutral within 0.5%: 3,193 ns/value before and 3,176
ns/value after. The candidate allocates the final deque once instead of first
allocating a two-slot deque and then growing it. Fresh values above two entries
and already-generic keys bypass adaptive helpers and use the prior deque path.

Focused operation controls compare packed and generic representations in the
same binary. Median results use five or seven one-second samples.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkSlicePackedOperationPairs -benchtime=1s -count=5 -benchmem -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSlicePackedCommandChurn -benchtime=1s -count=7 -benchmem -cpu=1'
```

| Operation | Generic deque | Packed | Result |
| --- | ---: | ---: | ---: |
| Head plus tail, two values | 163.3 ns; 0 B; 0 allocs | 164.0 ns; 0 B; 0 allocs | Neutral within 0.4% |
| Read two values | 160.0 ns; 32 B; 1 alloc | 142.7 ns; 32 B; 1 alloc | 1.12x faster; allocation unchanged |
| Command push then pop | 349.1 ns; 16 B; 1 alloc | 330.7 ns; 16 B; 1 alloc | 1.06x faster; allocation unchanged |
| Public push then pop from empty | 522.7 ns; 64 B; 1 alloc | 468.2 ns; 0 B; 0 allocs | 1.12x faster; allocation eliminated |

Packed command GET also previously expanded the internal slots into a temporary
public `Slice` before JSON encoding. The direct path preserves `nil` as `null`,
non-nil empty as `[]`, and writes one/two arbitrary values through the same
scalar and nested-value encoder used by packed maps. In that initial pass,
promoted positive indexes retained the prior deque branch.

Parity tests cover nil and non-nil empty containers, nil elements, escaped and
HTML-sensitive strings, invalid UTF-8, Unicode separators, booleans, signed
integer bounds, nested maps/slices, and promoted values. Seven alternating
300 ms A/B pairs used otherwise identical binaries pinned to one CPU:

```sh
make run CMD='go test . -run none -bench BenchmarkPackedSliceCommandGet -benchmem -benchtime=500ms -count=7 -cpu=1'
```

| Command GET, seven-run median | Temporary public slice | Direct packed JSON | Improvement |
| --- | ---: | ---: | ---: |
| Nil slice | 222.8 ns; 40 B; 2 allocs | 80.56 ns; 0 B; 0 allocs | 2.77x faster; allocation-free |
| Non-nil empty slice | 253.2 ns; 64 B; 3 allocs | 80.05 ns; 0 B; 0 allocs | 3.16x faster; allocation-free |
| One string | 345.5 ns; 88 B; 4 allocs | 141.7 ns; 16 B; 1 alloc | 2.44x faster; 5.50x lower heap; 4x fewer allocs |
| Two strings | 376.1 ns; 112 B; 4 allocs | 150.9 ns; 16 B; 1 alloc | 2.49x faster; 7.00x lower heap; 4x fewer allocs |
| Two nested values | 1,110 ns; 608 B; 9 allocs | 716.1 ns; 200 B; 5 allocs | 1.55x faster; 3.04x lower heap; 1.80x fewer allocs |
| Promoted deque control | 459.3 ns; 136 B; 4 allocs | 428.5 ns; 136 B; 4 allocs | CPU 1.07x faster within noise; memory unchanged |

A later follow-up removed the promoted-deque control's temporary public clone.
The command path now estimates response capacity with checked arithmetic and
writes values directly in logical ring order through the same scalar/nested
encoder. It preserves wrapped order, `null` versus `[]`, marshal errors,
escaping, and invalid-UTF-8 replacement. Tests were added before production
changes for mixed nested values, wrapped storage, an emptied promoted deque,
and a stored marshaler that starts failing after insertion. Baseline and final
rows below are medians of seven 500 ms runs on one logical CPU:

```sh
make run CMD='go test . -run=NONE -bench="^BenchmarkPackedSliceCommandGet/Promoted" -benchmem -benchtime=500ms -count=7 -cpu=1'
```

| Promoted command GET | Temporary public clone | Direct ring-order JSON | Improvement |
| --- | ---: | ---: | ---: |
| Three strings | 513.4 ns; 136 B; 4 allocs | 167.4 ns; 24 B; 1 alloc | 3.07x faster; 5.67x lower heap; 4.00x fewer allocs |
| Sixteen strings | 1,153 ns; 504 B; 4 allocs | 491.9 ns; 192 B; 1 alloc | 2.34x faster; 2.63x lower heap; 4.00x fewer allocs |
| Four mixed/nested values | 1,250 ns; 688 B; 9 allocs | 750.4 ns; 264 B; 4 allocs | 1.67x faster; 2.61x lower heap; 2.25x fewer allocs |

Command response bytes, read telemetry, public clone ownership, retained
storage, snapshots, journals, replication payloads, and wire schema are
unchanged.

The two additional empty pool descriptors cost 160 fixed bytes per cache
instance. About five direct one- or two-value upserts recover that fixed cost;
new-key push workloads recover it after about two one-value or three two-value
slices. No configurable format or migration is required.

<a id="packed-two-slice-length-rollback"></a>
### Packed Two-Slice Length Rollback

The two-value pool's `uint8` length leaves seven alignment bytes after two
interface slots, making each record 40 bytes. It cannot simply be removed:
`POP` and `SHIFT` deliberately leave drained and one-value slices in the same
pool, so the field distinguishes lengths zero, one, and two, including a real
`nil` element.

A behavior test was added before the experiment for two/one/empty transitions,
`nil` values, refill without changing the backing index, and failed pops from
an empty slice. The candidate encoded unused slots with private process-global
markers, reducing the record to 32 bytes without changing external values,
indexes, allocation counts, wire bytes, or persistence formats. A refined
variant used one typed marker in the second slot so every length read required
only one type test.

```sh
make run CMD='go test . -run="TestPacked(TwoSliceStateTransitionsPreserveNilValues|TwoSliceLayoutIsBounded)" -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSliceStorageLayout100k/UpsertValues2 -benchmem -benchtime=1x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkPackedTwoSliceStateTransitions -benchmem -benchtime=1s -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkPackedTwoSliceLengthEncoding -benchmem -benchtime=1s -count=5 -cpu=1'
```

| Metric | Inline length baseline | First marker | Refined typed marker | Result |
| --- | ---: | ---: | ---: | --- |
| Record size | 40 B | 32 B | 32 B | Candidate is 1.25x smaller |
| 100k retained heap | 46.23 B/slice | 36.98 B/slice | 36.98 B/slice | Candidate is 1.25x lower |
| 100k timed heap | 22,216,976 B | 17,765,632 B | 17,765,632 B | Candidate is 1.25x lower |
| 100k build CPU | 1,838 ns/slice | 1,859 ns/slice | 1,859 ns/slice | Candidate is 1.01x slower, within run noise |
| Pop then push | 13.45 ns | 14.84 ns | 14.30 ns | Refined candidate is 1.06x slower |
| Shift then push | 14.18 ns | 15.43 ns | 14.81 ns | Refined candidate is 1.04x slower |
| Same-binary pop/push encoding | 5.021 ns | not retained | 5.829 ns | Refined candidate is 1.16x slower |
| Same-binary shift/push encoding | 5.062 ns | not retained | 5.651 ns | Refined candidate is 1.12x slower |

Both state-transition controls remained allocation-free. The repeated mutation
regression outweighed the per-record memory reduction under the no-drawback
gate. The five-run same-binary controls isolate the encoding work from host
frequency and build drift and confirm the regression. Both marker variants were
removed. The final 40-byte representation keeps its directly loaded length
byte; only the new behavior, layout, and benchmark guards remain.

<a id="string-compaction-allocation-rollback"></a>
### String Compaction Allocation Rollback

The packed-string compactor copied every live payload into 256 KiB arenas while
rebuilding typed storage. That reduced allocator object count, but made a
memory-reclamation operation temporarily allocate approximately the complete
live string payload again. The current compactor only densifies the string
header/index slice and preserves immutable live string backing.

```sh
make bench-string-compaction STRING_STORAGE_BENCH_KEYS=100000 BENCHTIME=1x STRING_COMPACTION_GC_BENCHTIME=20x COUNT=7
```

The audit uses 100,000 strings varying from 33 through 512 bytes. Cumulative
heap and retained values are seven-run medians. Peak RSS is one isolated process
sample per implementation; forced-GC time is the median of seven runs with 20
collections per run. Packed results use commit `c3085d2`; dense-remap results
use the implementation before `b8a7375`, which the current rollback restores.

| Metric | Packed live payloads | Dense remap | Rollback result |
| --- | ---: | ---: | ---: |
| Cumulative compaction heap | 30,071,824 B | 2,808,848 B | 10.71x lower |
| Peak process RSS | 121,848 KiB | 93,516 KiB | 1.30x lower, 28,332 KiB reclaimed |
| Retained heap after GC | 28,866,344 B | 30,003,640 B | 3.94% higher |
| Retained heap objects | 127 | 100,021 | allocator objects no longer collapsed |
| Subsequent forced GC | 1,012,367 ns | 1,830,839 ns | 1.81x slower |
| Compaction allocations | 100,128 | 100,024 | 104 fewer |

The paired audit measured 235.4 ms for packing and 213.9 ms for dense remapping,
but subsequent wall-clock samples varied enough with host load that CPU is not
used as the rollback criterion. At those paired medians, packing needed roughly
26 future forced collections to repay its extra compaction CPU. The immediate
10.71x cumulative allocation and 30% peak-RSS increase were paid on every run,
which conflicts with invoking compaction under memory pressure. Current raw
output is written to `build/benchmarks/string-compaction.txt`.

### Per-Key Telemetry Modes

The bounded telemetry implementation uses compact exact counters/timestamps, a
fixed key replacement ring, and five-candidate least-recently-active sampling.
These medians use the baseline command above with only the three per-key memory
rows selected (`COUNT=3`).

| Workload | Mode | Tracked keys | Retained B/cache key | Memory comparison | Median fill time/key |
| --- | --- | ---: | ---: | ---: | ---: |
| 100,000 keys | Pre-change unlimited baseline | 100,000 | 242.5 B | baseline | not recorded |
| 100,000 keys | `bounded` (opt-in) | 100,000 | 213.5 B | 1.14x efficiency, 12.0% lower | 2.08 us |
| 100,000 keys | `full` | 100,000 | 194.5 B | 1.25x efficiency, 19.8% lower | 2.22 us |
| 100,000 keys | `off` (default) | 0 | 63.57 B | 3.81x efficiency, 73.8% lower | 1.70 us |
| 250,000 keys | `bounded` (opt-in) | 100,000 | 136.6 B | 1.57x vs full, 36.3% lower | 2.38 us |
| 250,000 keys | `full` | 250,000 | 214.5 B | comparison | 1.98 us |
| 250,000 keys | `off` (default) | 0 | 62.62 B | 3.43x vs full, 70.8% lower | 1.23 us |

The 250,000-key bounded fill spends more CPU selecting replacement candidates
after reaching 100,000 tracked keys. Normal cache reads and values remain
unchanged; only detailed per-key telemetry is replaced. Cache-wide counters
remain exact in all three modes.

### Atomic Cache-Wide Telemetry

With per-key telemetry off by default, hits, misses, writes, deletes,
expirations, and monotonic last-operation timestamps now use cache-wide atomic
state instead of the per-key telemetry mutex. Reads are derived from exact hit
and miss counters, so snapshots cannot observe an inconsistent
`reads != hits + misses` total. Enabling bounded or full key telemetry retains
the existing serialized plain counters and synchronizes representations when
the mode changes.

```sh
make run CMD='BIG_WINS_OPS=100000 go test -run xnomatch -bench BenchmarkBigWins/GlobalTelemetry -benchmem -benchtime 5x -count 7'
```

Each row performs 100,000 successful `GetString` calls. Baselines are five-run
medians from the pre-change implementation; final off rows are five-run
medians, and final full rows are seven-run medians on the same Ryzen 9 5950X
host.

| Key stats mode | Readers | Baseline ns/read | Final ns/read | Improvement |
| --- | ---: | ---: | ---: | ---: |
| `off` (default) | 1 | 199.8 | 171.7 | 1.16x |
| `off` (default) | 2 | 124.1 | 122.7 | 1.01x |
| `off` (default) | 4 | 121.8 | 100.6 | 1.21x |
| `off` (default) | 8 | 179.5 | 123.7 | 1.45x |
| `off` (default) | 16 | 206.7 | 103.3 | 2.00x |
| `off` (default) | 32 | 222.0 | 93.21 | 2.38x |
| `full` | 1 | 185.8 | 182.3 | 1.02x |
| `full` | 2 | 122.8 | 107.2 | 1.15x |
| `full` | 4 | 115.1 | 97.05 | 1.19x |
| `full` | 8 | 158.8 | 136.5 | 1.16x |
| `full` | 16 | 189.0 | 187.3 | 1.01x |
| `full` | 32 | 241.3 | 227.6 | 1.06x |

The atomic state adds 64 fixed bytes per cache and no per-operation
allocation. Wire and storage formats are unchanged. `SaveStats`, `LoadStats`,
failed public-batch rollback, exact counters, and timestamps remain preserved
across telemetry-mode transitions.

### Concurrent Scalar Read Fast Path

Ordinary in-memory `Get`, `Exists`, string, counter, and bytes reads now use the
trie's shared lock. Exact command `GET` uses the same path. Expired values and
lazy LevelDB references retry under the exclusive lock for cleanup or hydration.
Telemetry updates use a separate short critical section and remain exact.

| Workload | Baseline median | Optimized median | Improvement |
| --- | ---: | ---: | ---: |
| 100,000 `GetString` reads, 32 logical CPUs | 1,528 ns/read | 632.4 ns/read | 2.42x faster, 58.6% lower latency |

The optimized median is from three one-iteration runs with the same 100,000-key
and 100,000-operation fixture as the architectural baseline.

A later candidate extended exact generic `GET` under the same shared lock to
maps, slices, and sets. JSON bytes, heap, and serial allocation counts were
unchanged, and parallel reads improved substantially, but complete serial
commands consistently regressed:

| Collection fixture | Exclusive serial | Shared serial | Serial result | Exclusive parallel | Shared parallel | Parallel gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Packed two-field map | 249.3 ns | 350.8 ns | 1.41x slower | 323.2 ns | 76.07 ns | 4.25x faster |
| Three-field map | 691.3 ns | 791.7 ns | 1.15x slower | 798.5 ns | 155.8 ns | 5.13x faster |
| Packed two-value slice | 510.1 ns | 642.4 ns | 1.26x slower | 588.2 ns | 124.4 ns | 4.73x faster |
| 16-value slice | 961.4 ns | 1,096 ns | 1.14x slower | 1,048 ns | 259.3 ns | 4.04x faster |
| Packed two-member set | 111.8 ns | 223.0 ns | 2.00x slower | 232.8 ns | 67.03 ns | 3.47x faster |
| 16-member set | 1,806 ns | 1,910 ns | 1.06x slower | 2,067 ns | 453.3 ns | 4.56x faster |

The packed-map serial result was repeated in isolated exact-first and
generic-second runs: 350.8 versus 249.3 ns, confirming that combined benchmark
ordering was not the cause. The shared lookup must validate read-only fallback
conditions before it knows the stored type; avoiding that fixed work would
require either weakening TTL/cold-reference correctness or changing the
already optimized scalar path. The collection extension and its test-only
benchmark were removed, leaving no runtime tradeoff. Their existing direct
packed encoders remain in the ordinary exclusive command path.

### Striped Existing Counter Writes

The optional counter write path holds the trie's shared structural lock while a
key-hashed stripe protects an existing scalar value. This permits independent
counter updates to overlap without making the C trie or typed value pools
concurrently mutable. The default remains `0` (off); enable the measured
64-stripe policy with `COUNTER_WRITE_STRIPES=64` or
`-counter-write-stripes 64`.

```sh
make bench-big-wins BIG_WINS_BENCH=BenchmarkBigWins/ConcurrentWrite BIG_WINS_KEYS=65536 BIG_WINS_OPS=100000 BENCHTIME=3x COUNT=5
```

Each row updates 100,000 preallocated counter keys. Values are five-run medians
from three timed iterations per run on the Ryzen 9 5950X host.

| Writers | Global lock, ns/write | 64 stripes, ns/write | Improvement | Retained stripe memory |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 281.1 | 267.0 | 1.05x | 1,536 B |
| 2 | 362.8 | 209.7 | 1.73x | 1,536 B |
| 4 | 365.2 | 235.2 | 1.55x | 1,536 B |
| 8 | 386.7 | 262.9 | 1.47x | 1,536 B |
| 16 | 384.5 | 290.5 | 1.32x | 1,536 B |

The stripe slice is allocated once when enabled; writes add no per-operation
allocation. Wire bytes and storage bytes are unchanged because only in-memory
locking changes. Exact cache-wide write statistics remain enabled. Missing or
non-counter keys, TTL counters, detailed per-key telemetry, active snapshot or
Merkle tracking, and LevelDB spill accounting use the existing exclusive path.
This optimization is not keyspace sharding and does not change backup or scan
semantics.

<a id="local-hat-trie-partitions"></a>
### Local HAT-Trie Partitions

The opt-in `LOCAL_PARTITIONS` setting hashes each key with XXH64 into an
independent in-process HAT trie. The default is `0` and preserves the original
single lock and single trie. Counts must be powers of two from 2 through 256;
power-of-two masking avoids division on each keyed operation. The command
dispatcher and direct value APIs route keyed work, while scans, monitoring,
snapshots, persistence, compaction, expiration, statistics, and replication
merge the child state.

```sh
make bench-big-wins BIG_WINS_BENCH='^BenchmarkBigWins/LocalPartitions/' BIG_WINS_KEYS=65536 BIG_WINS_OPS=100000 BENCHTIME=1x COUNT=7
```

Each row preallocates 65,536 counter keys, then 16 workers perform 100,000
independent writes. Values are paired seven-run medians on the Ryzen 9 5950X
host. Heap and allocation columns cover the timed write phase. Maximum RSS was
measured in separate benchmark processes with the same fixture.

| Configuration | Seconds / 100k writes | ns/write | Timed heap B/op | Timed allocs/op | Maximum RSS | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `LOCAL_PARTITIONS=0` | 0.029147 | 291.5 | 12,144 | 78 | 51,588 KiB | baseline |
| `LOCAL_PARTITIONS=16` | 0.012992 | 129.9 | 6,608 | 45 | 54,096 KiB | 2.24x CPU, 1.84x timed heap, 1.73x timed allocs; RSS costs 1.05x |

Raw paired elapsed times in milliseconds were `32.228, 26.475, 34.555,
31.504, 27.617, 28.652, 29.147` with partitioning off and `19.322, 12.645,
12.411, 15.795, 14.764, 12.483, 12.992` with 16 partitions. Raw timed heap
bytes were `14752, 12816, 13072, 10512, 4256, 12144, 8736` and `6016, 11680,
9936, 6496, 9376, 1808, 6608`; raw timed allocations were `98, 78, 83, 70,
47, 78, 64` and `43, 51, 55, 44, 50, 35, 45`.

The gain comes from replacing one contended structural mutex with 16
independent locks. It is not a single-thread latency optimization. Fixed memory
increases because each partition owns a C trie and typed storage headers.
Whole-keyspace operations perform a k-way merge; snapshot and Pebble generation
capture hold all child write locks for point-in-time consistency but stream
records without materializing all values twice. Cold-value spilling first
measures each partition and water-fills the configured global cap across the
current hot-byte distribution, avoiding unnecessary spills under skew. Wire and
persistent record formats are unchanged, and snapshots remain portable between
partitioned and unpartitioned processes.

<a id="partition-parallel-whole-keyspace"></a>
#### Partition-Parallel Whole-Keyspace Operations

The original partition implementation scanned all 16 child tries serially,
concatenated their output, and globally sorted the complete result. The current
path scans children concurrently up to `GOMAXPROCS`, requests sorted child
results, preallocates the exact output size, and performs a deterministic k-way
merge. Monitoring inventory and replication pages inherit the entry path.
Independent Merkle rebuild, expiration cleanup, and memory compaction tasks use
the same CPU-bounded worker. Snapshot and persistence capture use per-partition
producers with one buffered entry each while retaining the all-partition lock
barrier required for a point-in-time image.

The fixture preloads 100,000 deterministic string keys across 16 local
partitions. The serial controls reproduce the old child-scan plus global-sort
implementation in the benchmark file; candidate rows call the public
`Keys(true)` and `Entries(true)` paths. Results are seven-run medians on the
Ryzen 9 5950X host.

```sh
make bench-partition-whole-keyspace PARTITION_SCAN_BENCH_KEYS=100000 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Operation | Serial merge | Parallel k-way merge | Relative result |
| --- | ---: | ---: | --- |
| Sorted keys time | 36.990 ms | 8.722 ms | 4.24x faster |
| Sorted keys cumulative heap | 18,432,704 B | 12,206,272 B | 1.51x lower |
| Sorted keys allocations | 101,330 | 101,343 | 13 more (0.013%) |
| Sorted entries time | 49.800 ms | 9.346 ms | 5.33x faster |
| Sorted entries cumulative heap | 24,496,800 B | 15,616,048 B | 1.57x lower |
| Sorted entries allocations | 101,333 | 101,342 | 9 more (0.009%) |

Raw elapsed samples in milliseconds were:

| Operation | Serial | Parallel |
| --- | --- | --- |
| Sorted keys | `35.694, 39.600, 36.819, 38.492, 36.990, 34.815, 37.191` | `11.347, 10.804, 8.568, 8.770, 8.592, 8.411, 8.722` |
| Sorted entries | `41.913, 49.197, 48.966, 49.800, 50.500, 50.911, 50.031` | `9.783, 10.478, 9.346, 8.925, 9.293, 9.566, 9.237` |

The full output is generated at
`build/benchmarks/partition-whole-keyspace.txt`. Parallelism activates only when
local partitions are explicitly enabled; `LOCAL_PARTITIONS=0` remains the sane
default and has no worker overhead. The result ordering, snapshot bytes, wire
encoding, and persistent layout are unchanged. The tradeoff is a transient
worker/result slice per enabled partition and a negligible allocation-count
increase in exchange for lower total allocation volume and much shorter scans.

### Durable Journal Group Commit

Mutating commands now enter a bounded journal worker. The default zero-wait
mode yields once and batches already queued callers, preserving serial latency;
a positive configurable window can trade latency for larger batches. Commands
are applied and acknowledged only after their batch `fsync` succeeds. Rejected
commands are truncated, and any later batch suffix is re-appended and synced
before execution.

| Workload | Baseline median | Group-commit median | Improvement |
| --- | ---: | ---: | ---: |
| 100 serial durable writes | 915,191 ns/write | 829,990 ns/write | 1.10x faster, 9.3% lower latency |
| 100 durable writes, 16 callers | 878,909 ns/write | 73,286 ns/write | 11.99x faster, 91.7% lower latency |

The concurrent result is about 13,645 acknowledged durable writes/second on the
benchmark filesystem. A deterministic 16-caller test with a 20 ms collection
window records exactly one `fsync` and verifies that neither response nor trie
mutation occurs before that sync completes.

### Durable Public Batches

This benchmark compares 10,000 individually journaled `SETSTR` commands with
the same commands in three public `BATCH` requests. The journal uses binary
records and `GroupCommitMaxBatch=1`, isolating the public batch's one-sync
commit from background group commit.

```sh
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkPublicScalarBatchJournalDurability10K -benchtime=1x -count=5 -benchmem'
```

| Mode | Time/10k writes | Journal syncs | Heap B/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| Individual durable commands | 9.821 s | 10,000 | 4,809,288 | 40,052 | baseline |
| Public batches, max 4,096 items | 29.051 ms | 3 | 5,771,560 | 40,310 | 338x faster, 3,333x fewer syncs |

The batch path uses 1.20x cumulative heap and 0.64% more allocations to retain
responses, rollback state, and side effects until durability succeeds. There is
no remote-wire comparison because this fixture measures local journal
durability; a client additionally saves up to 1,000 HTTP/gRPC round trips. A
journal write or sync failure rolls back journal bytes and in-memory mutations.
An ordinary failing subcommand preserves prior successful items, matching
public pipeline semantics.

<a id="native-c-command-batching"></a>
### Native C Command Batching

The native path packs 4,096 keys and operations into one C call while retaining
one Go trie lock. The baseline uses the previous locked Go loop and crosses cgo
once per trie operation. Both modes are prewarmed so trie-owned scratch growth
is excluded from steady-state allocation metrics.

```sh
make bench-native-command-batch BENCHTIME=20x COUNT=5
```

| Family, median of five | Go loop | Native C | Heap B/batch | Allocs/batch | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4,096 `SETINT` | 1.137 ms | 0.998 ms | 262,147 both | 1 both | 1.14x faster |
| 4,096 `GET` | 1.123 ms | 0.979 ms | 277,412 both | 3,997 both | 1.15x faster |

The native route activates at 32 commands, where fixed cgo setup begins to
amortize. It handles read, string-set, counter-set, counter-increment, and
delete families. Mixed commands, TTL-dependent keys, cold-reference-sensitive
increments, smaller batches, and journal executor interception retain the Go
path. Ordered C results are reconciled in Go for backing-store cleanup,
telemetry, mutation tracking, overflow errors, and response formatting. Raw
output is in `build/benchmarks/native-c-command-batch.txt`.

<a id="exact-batch-telemetry-aggregation"></a>
### Exact Batch Telemetry Aggregation

Default global telemetry previously called `time.Now` and updated atomic
counters after every successful item in native public batches and direct typed
gRPC scalar batches. CPU profiles attributed 19.2% of the 4,096-item native
GET path and 8.1% of the complete scalar gRPC process to the clock. The final
path accumulates exact hit, miss, write, and delete totals under the existing
trie lock, then publishes each total and the batch-completion timestamp once.
Explicit bounded or full per-key telemetry keeps its prior per-item timestamps
and counters.

Tests first fixed the clock to a counted function and failed with 32 calls for
a 32-item native batch and seven calls for a seven-item mixed scalar batch.
Both now make one call while retaining exact aggregate counters; detailed key
telemetry still makes and records all 32 per-item updates.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkBatchTelemetry -benchmem -benchtime=2s -count=7'
make run CMD='go test . -run=NONE -bench=BenchmarkBigWins/ScalarBatchStreamCommand -benchmem -benchtime=8s -count=1'
```

| Seven-run median | Per-item telemetry | Batch telemetry | Improvement |
| --- | ---: | ---: | ---: |
| Native 4,096 string reads | 1,011,781 ns; 262,144 B; 1 alloc | 857,091 ns; 262,144 B; 1 alloc | 1.18x faster; heap and allocations unchanged |
| Direct typed scalar, 16 reads | 3,903 ns; 736 B; 12 allocs | 3,328 ns; 736 B; 12 allocs | 1.17x faster; heap and allocations unchanged |
| Direct typed scalar, 256 reads | 55,274 ns; 7,648 B; 20 allocs | 47,116 ns; 7,648 B; 20 allocs | 1.17x faster; heap and allocations unchanged |

The complete 1,000-command scalar gRPC stream spot check improved from 450.1
to 384.1 ns/command (1.17x) with the same 23.64 measured wire B/command.
Cumulative heap changed from 248,742 to 249,327 B (0.24%, treated as noise)
and allocations fell from 3,970 to 3,961. There is no new field, retained
buffer, background worker, configuration, wire change, or storage-format
change. A cached-clock package was not adopted because its updater goroutine
and reduced timestamp precision add costs that aggregation avoids.

<a id="adaptive-typed-scalar-execution"></a>
### Adaptive Typed Scalar Execution

The direct typed protobuf batch previously returned to Go for every command.
The selected path validates the complete request once, executes eligible
distinct and mixed scalar commands in ordered native chunks of 64, and
reconciles the results in Go for storage ownership, exact telemetry, and typed
response columns. Repeated `GET` operations for one key are resolved once and
their result is copied into every response slot. This preserves same-key
ordering across `SET_STRING`, `GET`, `SET_COUNTER`, `INC`, `EXISTS`, `DELETE`,
and later reads.

Tests were added before routing the typed batch through the native adapter.
They initially observed zero native calls, then covered same-key ordering,
64-command boundaries, repeated hits and misses, TTL fallback, exact counters,
and oversized-key scratch release. The first naive repeated-read candidate was
rejected: 16 cached reads regressed from 885.1 to 2,279 ns (2.58x slower). A
scan-only guard was still 1.08x slower at 957.2 ns. Resolving the repeated key
once produced 378.5 ns in the selection run and 396.5 ns in the final
confirmation. Native mixed size two was also
rejected at 629.5 versus 565.5 ns (1.11x slower), so native routing starts at
four commands.

```sh
make bench-scalar-native-batch BENCHTIME=2s COUNT=7
make run CMD='go test . -run=ScalarBatch -count=1'
```

| Seven-run median | Go-loop baseline | Adaptive final | Improvement |
| --- | ---: | ---: | ---: |
| Four distinct reads | 937.2 ns | 849.7 ns | 1.10x faster |
| Four mixed commands | 1,004 ns | 894.2 ns | 1.12x faster |
| 16 distinct reads | 3,320 ns; 736 B; 12 allocs | 2,438 ns; 736 B; 12 allocs | 1.36x faster; heap and allocations unchanged |
| 16 mixed commands | 4,006 ns; 608 B; 20 allocs | 3,050 ns; 592 B; 17 allocs | 1.31x faster; 1.03x lower heap; 1.18x fewer allocs |
| 16 repeated reads | 885.1 ns; 736 B; 12 allocs | 396.5 ns; 512 B; 5 allocs | 2.23x faster; 1.44x lower heap; 2.40x fewer allocs |
| 256 distinct reads | 43,510 ns; 7,648 B; 20 allocs | 35,029 ns; 7,648 B; 20 allocs | 1.24x faster; heap and allocations unchanged |
| 256 mixed commands | 53,293 ns; 6,368 B; 152 allocs | 41,320 ns; 6,048 B; 91 allocs | 1.29x faster; 1.05x lower heap; 1.67x fewer allocs |

The complete 1,000-command scalar gRPC stream improved from the prior telemetry
pass's 384.1 to 356.6 ns/command (1.08x), from 249,327 to 234,750 heap B
(1.06x lower), and from 3,961 to 3,516 allocations (1.13x fewer), with the
same 23.64 wire B/command. Against the original pre-telemetry path, the two
passes together improve 450.1 to 356.6 ns/command (1.26x).

The repeated-key path retains no native scratch. A 16-command native batch
retains 1,664-1,680 B and a 256-command mixed request retains 6,720 B because
only one 64-command chunk is cached. The key arena is capped at 64 KiB for
direct batches and is released after oversized-key requests; operation and
result arrays are capped at 64 entries. TTL keys, lazy storage references,
local partitions, batches smaller than four, and journal, dirty-tracker,
replicator, or leader-enforcement interception retain the previous behavior.
There is no wire, persistence, configuration, or background-worker change.
Raw final output is in `build/benchmarks/scalar-native-batch.txt`.

<a id="shared-scalar-batch-keys"></a>
### Shared Scalar-Batch Keys

Repeated-key scalar envelopes previously serialized and decoded the same key
once per command. `ScalarBatchRequest.keys` now accepts either one key per
operation or one shared key for the complete envelope. The compact form needs
no protobuf schema field, generated-request growth, unsafe string conversion,
configuration, or retained state. Distinct-key requests keep their original
layout and direct indexed execution.

Tests were added before the server accepted the compact form. They cover mixed
same-key operation ordering, the direct resolve-once read path, journal replay,
dirty tracking, local partitions, legacy expanded columns, malformed columns,
and exact telemetry. Shared mixed, intercepted, and partitioned requests expand
one request-local string-header slice. Shared all-`GET` requests are consumed
directly and allocate no expansion.

```sh
make run CMD='env HATRIE_BIG_WINS_OPS=10000 go test . -run=NONE -bench="^BenchmarkBigWins/ScalarBatchStreamCommand(RepeatedKeys)?$$" -benchmem -benchtime=5x -count=15 -cpu=1'
```

Both rows are 15-run medians from the same binary, stream, batch size, server,
and 10,000-command repeated-key fixture on the Ryzen 9 5950X host.

| Key column | Time/10k | ns/command | Heap B/10k | Allocs/10k | Wire B/command | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Sixteen repeated key entries | 3.943 ms | 394.3 | 2,368,864 | 34,823 | 23.72 | baseline |
| One shared key entry | 3.161 ms | 316.1 | 1,684,878 | 23,020 | 11.54 | 1.25x CPU, 1.41x heap, 1.51x allocations, 2.06x wire |

An allocation profile reduced protobuf string-slice decoding from 12,508
objects in the original profiled run to 1,043 objects with one key per message.
The profile still includes setup and framework decoding outside the timed
region, so the complete benchmark table remains the acceptance measurement.
Older servers reject a shared key for a multi-command envelope with the
existing key-count error; mixed-version clients can retry with expanded keys.

The first prototype instead added `packed_keys` and `key_ends` protobuf fields.
It improved its same-binary control 1.09x and removed 1.44x allocations, but
the two slice fields enlarged every decoded legacy request: the untouched-head
10k control used 2,368,864 heap B while the enlarged legacy request used
2,400,811 B, a 1.35% regression even when the fields were absent. That design
was removed. Reusing response objects was also rejected before implementation:
gRPC permits tracing and stats handlers to consume a sent message lazily, so a
stream cannot safely mutate or pool the response immediately after `SendMsg`.

### Segmented WAL Compaction

The daemon now rotates an active journal into ordered sidecar files instead of
rescanning and rewriting the complete WAL after every successful snapshot.
Rotation happens between durable batches. Each new active file starts with a
checkpoint, so it remains independently readable; cross-file scanning validates
every sequence and rejects a torn archived segment. A torn active tail is still
truncated to its last complete record on restart.

```sh
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkCommandJournalCompaction100K -benchtime=1x -count=5 -benchmem'
```

Both modes start with the same 100,000-record binary active file. These are
five-run medians from one matched run on the Ryzen 9 5950X host.

| Mode | Time/compaction | Heap B/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: |
| Single-file rewrite | 31,462,304 ns | 20,810,464 | 500,033 | baseline |
| Segment rotate | 1,844,510 ns | 22,256 | 56 | 17.06x CPU, 935x heap, 8,929x allocations |

The server defaults to 64 MiB segments and 16 retained closed files. This
delays full-snapshot fallback for lagging replicas and bounds historical disk
use to roughly 1 GiB plus the active file. A batch may cross the byte target,
and rotation pays file rename, checkpoint `fsync`, and directory metadata
`fsync`; those fixed costs explain the run-to-run latency variance. Setting
`JOURNAL_SEGMENT_MAX_BYTES=0` restores the prior single-file rewrite path.

<a id="binary-journal-catch-up-wire"></a>
### Binary Journal Catch-up Wire

Journal pull now negotiates a native binary tail envelope by default. Records
are encoded directly into one response buffer and decoded into their final
preallocated slice. Immutable response bytes back scalar strings during apply,
avoiding per-record payload and string allocations. The source still returns
JSON to ordinary `GET /api/journal` clients, and a binary-preferring follower
automatically accepts JSON from older sources. Operators can force JSON with
`JOURNAL_PULL_WIRE_FORMAT=json` or CLI `journal -wire-format json`.

The fixture encodes and decodes 10,000 `SETINT` records with deterministic
keys. It measures both sides of serialization and the complete response body,
but excludes HTTP framing and command application. Before application, fields
that cache structures may retain are cloned away from the shared response
buffer; that command-specific ownership transfer is outside this serialization
fixture. Values are seven-run medians from one complete encode/decode per sample
on the Ryzen 9 5950X host.

```sh
make bench-journal-wire BENCHTIME=1x COUNT=7
```

| Metric | JSON | Binary default | Improvement |
| --- | ---: | ---: | ---: |
| Encode + decode time | 6.182 ms | 1.197 ms | 5.16x faster |
| Cumulative heap | 11,178,528 B | 2,383,920 B | 4.69x lower |
| Allocations | 10,042 | 4 | 2,510.50x fewer |
| Response body | 808,943 B | 289,886 B | 2.79x smaller |

Raw elapsed samples in milliseconds were:

| Format | Seven samples |
| --- | --- |
| JSON | `10.678, 6.182, 9.885, 8.171, 5.599, 5.341, 5.650` |
| Binary | `1.291, 1.823, 1.399, 1.195, 1.134, 1.002, 1.197` |

The full raw output, including heap, allocation, and `wire_B/op` metrics for
every sample, is generated at `build/benchmarks/journal-tail-wire.txt`. Binary
is less convenient to inspect manually and is project-specific; JSON remains
the compatibility and diagnostics option. `JOURNAL_FORMAT` independently
controls durable on-disk journal records.

<a id="selective-journal-wire-ownership"></a>
### Selective Journal Wire Ownership

Binary decoding intentionally borrows scalar strings from the response body.
The follower previously cloned every key, value, and subkey before applying the
tail, even when a command only parsed an integer and the HAT-trie copied its key
into native storage. Plain `SETINT` and `INC` records now borrow those fields
through synchronous WAL append and apply, after which the response body can be
released. Plain deletes and persists receive the same treatment. `SETSTR`
continues to clone its stored value.

The ownership policy remains conservative. TTL-bearing writes, structured or
unknown commands, local partitions, active snapshot mutation tracking, enabled
per-key stats, persistent dirty-key tracking, and active LevelDB spill/hot-byte
indexes clone any key they may retain. Stored or potentially retained
value/subkey strings are also cloned. JSON input is unchanged because its
decoder already owns its strings.

The paired fixture decodes the same 10,000-record binary `SETINT` response and
then either applies the previous clone-all ownership policy or the selective
default. It measures decode and ownership transfer but excludes WAL and command
application. Values are seven-run medians on the Ryzen 9 5950X host.

```sh
make bench-journal-wire BENCHTIME=1x COUNT=7
```

| Metric | Clone all fields | Selective default | Improvement |
| --- | ---: | ---: | ---: |
| Decode + ownership | 0.956 ms | 0.696 ms | 1.37x faster |
| Cumulative heap | 2,216,240 B | 2,056,240 B | 1.08x lower |
| Allocations | 20,003 | 3 | 6,667.67x fewer |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Clone all fields | `1.281, 0.826, 1.028, 0.952, 0.956, 0.996, 0.861` |
| Selective ownership | `0.731, 0.693, 0.572, 1.070, 0.896, 0.684, 0.696` |
| Selective plus dirty keys | `0.929, 1.369, 0.638, 0.764, 0.742, 0.874, 1.440` |

The same `build/benchmarks/journal-tail-wire.txt` artifact now includes this
paired ownership benchmark alongside JSON/binary serialization results. With a
persistent dirty tracker, the median is 0.874 ms with 10,003 allocations and
2,216,240 cumulative heap bytes: keys remain owned by the tracker, while the
10,000 parsed textual counter values still avoid cloning.

<a id="compact-scalar-journal-tails"></a>
### Compact Scalar Journal Tails

The binary pull decoder previously allocated one full `CacheCommandRequest` for
every record. That public request carries slices, maps, optional pointers, and
batch fields even when catch-up only needs a command code plus key/value. A
homogeneous tail of plain `SET`/`SETSTR` or plain `SETINT` records now uses an
internal 48-byte record containing sequence, borrowed key/value strings, and a
compact command code. WAL encoding reconstructs one request on the stack, then
large runs apply under one trie lock using the same prefix bookkeeping and
rollback boundaries as the full path.

TTL-bearing, mixed-family, and structured tails automatically restart through
the full decoder; malformed binary input is rejected. Public/direct binary
decode still returns normal full requests. The compact path owns stored string
values and keys used by stats, snapshots, persistent dirty tracking, LevelDB
indexes, or partitions. Runs shorter than 32 retain serial command application.
If a mixed tail changes family after its first record, the abandoned compact
candidate arena is a temporary fallback cost; a first-record mismatch avoids
that arena entirely.

The decoder-only fixture compares full and compact representations of one
10,000-record binary `SETINT` body. The durable fixture includes binary decode,
ownership transfer, local sequence assignment, bounded WAL encoding/write, one
`fsync`, and cache application. Values are seven-run medians on the Ryzen 9
5950X host.

```sh
make bench-journal-wire BENCHTIME=1x COUNT=7
make bench-journal-apply BENCHTIME=1x COUNT=7
```

| Metric | Full requests | Compact scalar default | Improvement |
| --- | ---: | ---: | ---: |
| Decode time | 0.556 ms | 0.522 ms | 1.07x faster |
| Decode cumulative heap | 2,056,240 B | 778,288 B | 2.64x lower |
| Durable decode + WAL + apply | 8.074 ms | 5.864 ms | 1.38x faster |
| Durable cumulative heap | 2,720,000 B | 1,442,048 B | 1.89x lower |
| Durable allocations | 6 | 6 | unchanged |
| Wire bytes | 349,886 B | 349,886 B | unchanged |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Full decode | `0.650, 0.442, 0.554, 0.606, 0.832, 0.556, 0.451` |
| Compact decode | `0.515, 0.511, 0.593, 0.440, 0.539, 0.522, 0.798` |
| Full durable pull | `8.074, 8.367, 7.359, 8.500, 7.458, 8.110, 7.987` |
| Compact durable pull | `5.645, 5.864, 5.582, 5.322, 402.709, 202.875, 7.050` |

The two compact durable outliers are included rather than discarded; both are
filesystem stalls, and the seven-run median remains lower. Full raw output is
generated at `build/benchmarks/journal-tail-wire.txt` and
`build/benchmarks/journal-pull-batch-apply.txt`. Compact records do not alter
wire format, on-disk WAL format, fsync count, or recovery compatibility.

<a id="bounded-wal-staging-arena"></a>
### Bounded WAL Staging Arena

The coalesced batch writer previously allowed up to a 1 MiB staging arena. A
10,000-record compact pull therefore reserved enough capacity for its complete
429,873-byte WAL even though only one chunk is needed at a time. Full-request,
compact-scalar, and group-commit batch writers now share a 128 KiB default.
Ordinary records flush before crossing that boundary; an individual record
larger than the boundary is still written intact. Every chunk remains part of
one append transaction with one final `fsync`.

The focused fixture applies 10,000 predecoded compact `SETINT` records while
varying only the internal chunk limit. It reports cumulative Go heap, write
calls, and identical WAL bytes. Results are seven-run medians on the Ryzen 9
5950X host.

```sh
make bench-journal-apply JOURNAL_APPLY_BENCH='^BenchmarkJournalWALChunkSize10K$' BENCHTIME=1x COUNT=7
```

| Chunk limit | Time / 10k | Cumulative heap | Writes | WAL bytes | Relative to old 1 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 KiB | 5.788 ms | 107,120 B | 7 | 429,873 B | 5.59x lower heap, 1.01x slower |
| **128 KiB default** | **5.416 ms** | **172,656 B** | **4** | **429,873 B** | **1.05x faster, 3.47x lower heap** |
| 256 KiB | 5.413 ms | 303,728 B | 2 | 429,873 B | 1.05x faster, 1.97x lower heap |
| Previous 1 MiB | 5.705 ms | 598,640 B | 1 | 429,873 B | baseline |

The 256 KiB median is within 0.1% of 128 KiB but uses 1.76x more cumulative
heap. The 64 KiB candidate saves another 65,536 bytes but is 1.07x slower than
128 KiB. This makes 128 KiB the measured latency/memory balance rather than an
arbitrary minimum.

The complete binary decode + durable WAL + apply fixture also improved from a
fresh 7.191 ms, 1,442,048 B/op baseline to 5.920 ms and 1,007,872 B/op: 1.21x
faster and 1.43x lower cumulative heap. Both sides perform six allocations,
transfer 349,886 wire bytes, and use one final sync. Raw elapsed samples were:

| Path | Seven samples |
| --- | --- |
| Previous 1 MiB pull | `6.674, 7.003, 7.176, 7.269, 7.191, 7.457, 7.718` |
| 128 KiB pull | `5.888, 5.842, 5.920, 6.532, 12.684, 7.006, 5.775` |

`build/benchmarks/journal-pull-batch-apply.txt` contains the reproducible
chunk-size rows and current full-path rows. Filesystem sync variance affects
elapsed time, so the exact heap reduction and interleaved chunk-size fixture
are the stronger sizing signals. Tests inject a failure after an earlier chunk
has reached the file and verify that the journal truncates to its original
offset, resets its sequence, and applies no cache mutation.

<a id="coalesced-journal-batch-append"></a>
### Coalesced Journal Batch Append

After receiving a journal tail, the follower previously allocated and wrote
every durable record separately before its one final `fsync`. Binary records
now append directly into one reusable arena, and a compact `uint32` size table
retains each rollback boundary. The same writer coalesces ordinary group-commit
jobs. The arena starts at no more than 128 KiB and flushes when it reaches that
threshold; a large batch therefore remains bounded while all chunks still share
one final durability sync. Standalone binary records remain byte-for-byte
identical.

The fixture assigns local journal sequences, encodes and writes 10,000
`SETINT` records, performs one `fsync`, and applies all commands to the trie.
Both paths produce the same 439,873-byte WAL. Results are seven-run medians on
the Ryzen 9 5950X host.

```sh
make bench-journal-apply BENCHTIME=1x COUNT=7
```

| Metric | Per-record write baseline | Coalesced default | Improvement |
| --- | ---: | ---: | ---: |
| Durable batch apply | 20.935 ms | 7.364 ms | 2.84x faster |
| Cumulative heap | 1,686,384 B | 606,832 B | 2.78x lower |
| Allocations | 30,004 | 5 | 6,000.80x fewer |
| WAL bytes | 439,873 B | 439,873 B | unchanged |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Per-record writes | `24.328, 20.615, 20.281, 20.935, 20.666, 21.617, 21.073` |
| Coalesced append | `14.211, 7.452, 7.364, 6.817, 6.574, 6.905, 7.466` |

The full current output, including heap, allocation, record-count, and WAL-byte
metrics, is generated at `build/benchmarks/journal-pull-batch-apply.txt`. In a
secondary 100-write, 16-caller group-commit run, coalescing moved the median
from 87.335 to 82.283 microseconds per write (1.06x), reduced heap from 95,352
to 93,384 B (1.02x), and reduced allocations from 703 to 603 (1.17x). JSON WAL
mode also coalesces file writes but retains its per-record JSON encoding
allocations. Encode, write, or sync failure still rolls the complete batch back;
an apply-time rejection keeps the successful prefix and durably truncates the
rejected record and suffix.

<a id="single-lock-journal-scalar-apply"></a>
### Single-Lock Journal Scalar Apply

After the pulled WAL batch is durable, the follower previously called the
public command parser separately for every record. Homogeneous runs of at least
32 plain `SET`/`SETSTR` or `SETINT` records now validate and mutate under one
trie lock. Cache-wide write counts use one additive update, detailed key stats
share one telemetry lock, and snapshot/Merkle/storage bookkeeping still visits
every successful key before releasing the trie lock. A rejected record records
the exact successful prefix before the journal truncates the rejected entry and
suffix.

TTL-bearing writes, `INC` (which may overflow), mixed command families, short
runs, and opt-in local partitions retain the existing serial path. This avoids
speculative suffix mutation and leaves partition routing unchanged. An existing
native-C batch adapter was measured but not selected for this path: its bridge
operation/result arenas raised the 10k fixture to 1,663,600 cumulative heap
bytes and did not improve durable latency.

The paired durable fixture assigns local sequences, encodes and writes 10,000
`SETINT` records, performs one `fsync`, and then selects either serial or
single-lock apply. The application-only fixture excludes WAL and setup so the
CPU effect is visible independently of filesystem latency. Values are seven-run
medians on the Ryzen 9 5950X host.

```sh
make bench-journal-apply BENCHTIME=1x COUNT=7
```

| Metric | Serial apply | Single-lock default | Improvement |
| --- | ---: | ---: | ---: |
| Application CPU | 4.189 ms | 2.603 ms | 1.61x faster |
| Durable WAL + apply | 8.907 ms | 7.744 ms | 1.15x faster |
| Durable cumulative heap | 606,832 B | 606,832 B | unchanged |
| Durable allocations | 5 | 5 | unchanged |
| WAL bytes | 439,873 B | 439,873 B | unchanged |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Durable serial apply | `19.266, 9.664, 8.417, 7.451, 8.081, 8.907, 9.666` |
| Durable single-lock apply | `7.744, 7.892, 7.410, 7.563, 8.048, 8.196, 6.925` |
| Application-only serial | `4.189, 5.229, 4.015, 3.622, 4.316, 4.036, 4.304` |
| Application-only single-lock | `2.603, 2.204, 2.853, 2.713, 2.518, 2.500, 2.620` |

The complete paired output is generated at
`build/benchmarks/journal-pull-batch-apply.txt`. The durable improvement is
smaller because the required final `fsync` dominates on this host; durability
is not relaxed or made configurable by this optimization.

### Point-in-Time Snapshot Capture

Snapshots now copy a consistent point-in-time entry set while holding the trie
lock, then release the lock before binary/JSON encoding, gzip compression, and
file or network output. Journal snapshots capture the journal sequence and trie
state under the same short barrier; later commands proceed during output and
remain ordered journal deltas. Full LevelDB saves similarly run record visitors
and database diff work after capture. Unchanged lazy LevelDB records retain their
exact bytes.

These nine-run medians compare `c549fb7` with the optimized implementation on
the same host and use 100,000 string keys, the default gzip-best-binary format,
and one snapshot per run:

| Metric | Blocking output | Captured output | Change |
| --- | ---: | ---: | ---: |
| Maximum concurrent read pause | 528,624,130 ns | 142,374,086 ns | 3.71x shorter, 73.1% lower |
| Total snapshot duration | 531,989,731 ns | 561,312,675 ns | 1.06x time, 5.5% higher |
| Heap allocation/snapshot | 27,396,816 B | 72,075,424 B | 2.63x, 163.1% higher |
| Allocations/snapshot | 509,437 | 1,304,616 | 2.56x, 156.1% higher |
| Benchmark process peak RSS | 93,616 KiB | 136,336 KiB | 1.46x, 45.6% higher |

The pause is the immutable capture itself, not output latency. The memory cost
is temporary and scales with captured key/value state. This is an availability
tradeoff: slow disks, compression, and blocked network clients no longer extend
the command pause, at the cost of enough memory headroom for one in-flight
capture. Snapshot jobs on one journal are serialized to prevent overlapping
captures and compaction races. Captured entries use fixed 4,096-entry pages so
no allocation trusts an unbounded reported key count or requires one dataset-
sized contiguous block.

### Bounded-Page Snapshot Capture

The follow-up capture scans 256 entries per lock acquisition and tracks writes
between pages, then reconciles changed keys at the journal barrier. This keeps
the point-in-time guarantee while bounding individual lock holds.

```sh
make bench-big-wins BIG_WINS_BENCH=BenchmarkBigWins/Snapshot BIG_WINS_KEYS=100000 BIG_WINS_OPS=100000 BENCHTIME=1x COUNT=5
```

These are five-run medians from the same current checkout and the pre-change
`baf19d6` worktree.

| Metric | Whole-capture lock | Bounded pages | Change |
| --- | ---: | ---: | ---: |
| Maximum concurrent read pause | 61,740,215 ns | 2,821,866 ns | 21.88x shorter, 95.4% lower |
| Total snapshot duration | 165,941,910 ns | 167,254,026 ns | 0.8% higher |
| Heap allocation/snapshot | 47,546,264 B | 47,507,544 B | 0.1% lower |
| Benchmark process peak RSS | 92,136 KiB | 95,492 KiB | 3.6% higher |

Snapshot bytes and format are unchanged. The added mutation tracker is bounded
by keys changed during capture rather than total data size; a write-heavy
capture can therefore retain additional temporary key metadata until the final
barrier.

<a id="bounded-partition-snapshot-locking"></a>
### Bounded Partition Snapshot Locking

Local partition snapshots previously acquired every child write lock before
capture and held all of them through the complete sorted scan. The bounded path
briefly installs one shared mutation tracker on all children, scans each child
in generation-checked 256-entry pages, and k-way merges those pages without
holding the other child locks. Dirty keys are recaptured in bounded chunks. The
tracker's dirty map is swapped in constant time while the final journal and
child-lock barrier is held, then processed after releasing that barrier.

The fixture writes the same default gzip-best-binary snapshot for 100,000
strings across 16 local partitions while a concurrent reader measures its
longest blocked call. The probe key is precomputed so its own allocations do
not distort the available path. Both rows are seven-run medians from the same
Ryzen 9 5950X host; the baseline is commit `df83747` with the corrected probe.

```sh
make bench-partition-snapshot PARTITION_SNAPSHOT_BENCH_KEYS=100000 PARTITION_SNAPSHOT_COUNT=16 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Metric | Whole partition-set lock | Bounded child pages | Change |
| --- | ---: | ---: | ---: |
| Maximum concurrent read pause | 154,398,310 ns | 2,299,512 ns | 67.14x shorter, 98.5% lower |
| Total snapshot duration | 241,261,897 ns | 259,973,634 ns | 7.8% higher |
| Cumulative heap | 77,707,496 B | 79,021,848 B | 1.7% higher |
| Allocations | 501,031 | 501,067 | 36 more, below 0.01% |

Raw total-duration samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Whole partition-set lock | `240.785, 238.869, 239.128, 241.262, 270.915, 255.158, 262.435` |
| Bounded child pages | `225.688, 281.117, 264.527, 245.527, 275.626, 259.974, 249.562` |

The candidate output is written to
`build/benchmarks/partition-snapshot-capture.txt`. Materialized snapshots,
compact streamed snapshots, and Pebble generation capture share this path.
Concurrent update, delete, insert, structured-value, tail-key, and empty-key
tests verify reconciliation for both materialized and streamed restores. The
snapshot version, wire/storage bytes, ordering, and journal checkpoint contract
are unchanged. Temporary dirty-key memory scales with writes during capture;
the page buffers scale with the configured local partition count.

<a id="parallel-partition-restore"></a>
### Parallel Partition Restore

Snapshot replay and persistent-store startup previously held every local
partition lock for atomic visibility but decoded and applied all records on one
goroutine. The current path retains that all-partition barrier while routing
prepared records through partition-stable FIFO workers. Concurrency is bounded
by the smaller of `GOMAXPROCS` and the configured partition count, with eight
queued records per worker. Stale-key cleanup and error rollback also run across
independent children in parallel. Iterator-backed binary byte values are
detached before dispatch, while cold references pass only their precomputed
record length and checksum instead of copying the full encoded record.

The fixture restores 100,000 identical 256-byte strings into a 16-partition
cache containing one stale key. Snapshot and Pebble inputs are built once and
excluded from timed work; restore includes decoding, per-partition application,
and exact stale-key deletion. Results are seven-run medians on the Ryzen 9
5950X host. The serial baseline is commit `dab6490`.

```sh
make bench-partition-restore PARTITION_RESTORE_BENCH_KEYS=100000 PARTITION_RESTORE_COUNT=16 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Input | Serial restore | Bounded parallel restore | Improvement | Heap change | Allocation change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Binary snapshot | 258.183 ms | 202.398 ms | 1.28x faster | 181,088,464 B to 181,158,640 B (+0.04%) | 902,664 to 902,813 (+0.02%) |
| Pebble store | 213.948 ms | 181.435 ms | 1.18x faster | 120,322,480 B to 120,440,400 B (+0.10%) | 502,703 to 502,875 (+0.03%) |

Raw elapsed samples in milliseconds were:

| Input | Serial samples | Bounded parallel samples |
| --- | --- | --- |
| Binary snapshot | `261.572, 262.279, 258.183, 240.788, 287.909, 246.711, 233.910` | `205.431, 208.462, 199.254, 202.398, 204.957, 202.284, 196.342` |
| Pebble store | `210.970, 192.237, 220.928, 213.948, 216.138, 214.704, 205.143` | `181.435, 181.995, 182.816, 183.244, 177.557, 170.405, 181.300` |

The bounded-parallel result is retained as the legacy two-pass control in the
current `build/benchmarks/partition-restore.txt` output. Mixed
scalar/structured/byte values, lazy cold references, stale-key deletion,
snapshot mismatch, and invalid persistent records have correctness and race
coverage. A worker error cancels dispatch and restores every changed partition.
This historical optimization applies only when local partitions are enabled;
the generation restore below replaces its rollback phase for snapshot input.
Pebble startup still uses the bounded parallel path described here.

<a id="atomic-generation-snapshot-restore"></a>
### Atomic Generation Snapshot Restore

Portable snapshot restore previously decoded the complete file twice. The first
pass retained and sorted every active key. The second pass decoded values again,
mutated the live trie while holding its write lock, retained old values for
rollback, tracked newly created keys, and finally scanned for stale-key
deletion. A late validation or disk error replayed that rollback state.

The default path now decodes once into an isolated trie generation with the
same key-stat, counter-stripe, disk-spill, and local-partition configuration.
Partitioned staging retains the existing partition-stable workers, but they
write only private children and therefore need no live partition barrier or
rollback records. Complete EOF, metadata, duplicate-key, TTL, and value
validation finishes before cutover. The live object then exchanges C roots,
typed backing pools, expiration metadata, key-stat slots, spill indexes, and
Merkle state while holding its existing write locks. Runtime locks,
configuration, global telemetry, and routing objects remain stable.

Disk-spilled values are written below the configured disk root in a private
`.snapshot-restore-*` generation. Failure removes that generation without
touching live files. Success transfers ownership during cutover and removes the
old generation afterward. Repeated restore therefore remains beneath the same
backup boundary. Snapshot version, binary/JSON/gzip formats, journal sequence,
and public APIs are unchanged.

The fixture restores 100,000 identical 256-byte strings into a target containing
one stale key. Inputs and target construction are excluded. The unpartitioned
fixture is the default configuration; the second fixture uses 16 local
partitions. Legacy controls execute the previous two-pass implementation in the
same binary. Values are seven-run medians on the Ryzen 9 5950X host.

```sh
make bench-partition-restore PARTITION_RESTORE_BENCH_KEYS=100000 PARTITION_RESTORE_COUNT=16 BENCHTIME=1x COUNT=7
```

| Default single trie | Legacy two pass | Staged single pass | Improvement |
| --- | ---: | ---: | ---: |
| Restore time | 385.364 ms | 234.900 ms | 1.64x faster |
| Cumulative heap | 217,686,896 B | 108,816,416 B | 2.00x lower |
| Allocations | 901,188 | 500,117 | 1.80x fewer |
| Cutover lock | full live apply | 1.720 us | staged work stays outside live lock |

| 16 local partitions | Legacy two pass | Staged single pass | Improvement |
| --- | ---: | ---: | ---: |
| Restore time | 190.590 ms | 130.342 ms | 1.46x faster |
| Cumulative heap | 181,155,840 B | 100,628,976 B | 1.80x lower |
| Allocations | 902,806 | 501,351 | 1.80x fewer |
| Cutover lock | full live apply | 5.630 us | all child roots exchange under one barrier |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Default legacy | `380.682, 388.597, 382.380, 381.792, 385.364, 385.685, 396.974` |
| Default staged | `253.439, 226.548, 224.750, 231.660, 234.900, 255.946, 249.795` |
| 16-partition legacy | `189.585, 193.450, 187.956, 197.735, 205.121, 190.590, 185.895` |
| 16-partition staged | `124.467, 122.650, 127.362, 130.342, 133.837, 135.903, 133.651` |

`build/benchmarks/partition-restore.txt` contains the current raw rows,
including `cutover_ns/op`. Cumulative allocation falls because active-key
materialization, the second decode, rollback snapshots, and created-key maps
are removed. Peak resident state can still include both complete generations
until cutover; that is the cost of leaving live readers on an unchanged state
while staging and of rejecting failures atomically.

### Compact Streaming Snapshot Capture

The snapshot writer now serializes each scanned value immediately into compact
binary records held in pages bounded by 1 MiB or 4,096 records. It no longer
retains one wide `snapshotEntry` object for every key. The writer streams those
records through binary or gzip output after the final journal barrier and merges
only keys changed during capture. Plain JSON output retains unchanged lazy
LevelDB JSON records byte-for-byte; other records are decoded only when JSON was
explicitly selected.

```sh
make bench-big-wins BIG_WINS_BENCH=BenchmarkBigWins/Snapshot BIG_WINS_KEYS=100000 BIG_WINS_OPS=100000 BENCHTIME=1x COUNT=5
```

These are five-run medians from `f118fc8` and the compact writer on the same
Ryzen 9 5950X host.

| Metric | Materialized entries | Compact record pages | Improvement |
| --- | ---: | ---: | ---: |
| Total snapshot duration | 182,220,989 ns | 151,347,870 ns | 1.20x faster, 16.9% lower |
| Heap allocation/snapshot | 47,607,920 B | 24,565,080 B | 1.94x lower, 48.4% lower |
| Allocations/snapshot | 675,574 | 642,458 | 1.05x fewer, 4.9% lower |
| Benchmark process peak RSS | 97,152 KiB | 63,104 KiB | 1.54x lower, 35.0% lower |
| Maximum concurrent read pause | 2,997,292 ns | 3,235,198 ns | 7.9% higher |

Wire bytes, snapshot version, old-format loading, atomic replacement, and
point-in-time semantics are unchanged. Encoding inside each 256-key scan page
accounts for the small pause increase. A single value larger than 1 MiB owns a
dedicated page because its payload cannot be subdivided without changing the
record format.

<a id="selective-snapshot-mutation-maps"></a>
#### Selective Snapshot Mutation Maps

Snapshot, streamed snapshot, Pebble generation, and local-partition capture all
install a mutation tracker while scanning. The tracker previously allocated an
empty replacement map before knowing whether any write raced the scan and
installed another empty dirty map when the final drain found no work. Captures
now return nil directly on an empty drain and create the replacement map only
after observing dirty keys. Nonempty drains retain the old writer-ready map
replacement and lock sequence, while replacement maps are sized from the first
dirty batch.

The test-first lifecycle fixture proves an empty drain releases its map, a
nonempty drain preserves sorted keys and installs an empty writer-ready map,
`take` transfers ownership without aliasing later writes, and completed capture
does not retain an unused map. Concurrent stream, paged, and local-partition
mutation reconciliation tests pass ten times.

```sh
make run CMD='go test . -run="TestSnapshotMutationTrackerReleasesEmptyAndRetainsWriterReadyMap|TestSnapshotCaptureReleasesUnusedMutationMap|TestSnapshotStreamCaptureReconcilesConcurrentMutations|TestSnapshotCaptureReconcilesMutationsBetweenScanPages|TestLocalPartition.*Snapshot" -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkSnapshotMutationTrackingCycle -benchmem -benchtime=100000x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSnapshotNoMutationTracking -benchmem -benchtime=2000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSnapshotMutationTrackerFirstMark -benchmem -benchtime=100000x -count=10 -cpu=1'
```

| Tracking cycle, seven-run median | Eager reset/replacements | Selective final | Improvement |
| --- | ---: | ---: | ---: |
| Zero mutations | 177.4 ns; 160 B; 4 allocs | 65.44 ns; 64 B; 2 allocs | 2.71x faster; 2.50x lower heap; 2x fewer allocations |
| One mutation | 594.6 ns; 912 B; 8 allocs | 598.1 ns; 912 B; 8 allocs | CPU neutral within 0.6%; heap and allocations identical |
| 64 mutations | 24,402 ns; 35,600 B; 89 allocs | 20,533 ns; 32,192 B; 82 allocs | 1.19x faster; 1.11x lower heap; seven fewer allocations |

An actual empty capture improved from 8.351 to 7.838 us (1.07x), 15,153 to
15,057 heap B, and seven to five allocations. A one-entry capture also removes
exactly 96 B and two allocations; its CPU samples are intentionally not used
because the 1.29 MiB capture-page allocation makes that microbenchmark
GC-sensitive.

A fully lazy prototype also removed the initial dirty map, but moved its cost
onto the first concurrent writer: first-mark median rose from 87.8 to 116.0 ns
(1.32x slower), from 208 to 256 B, and from one to two timed allocations. That
part was rolled back. The retained initial map means writer latency, snapshot
semantics, lock boundaries, output, storage, wire bytes, and formats are
unchanged.

### Delete-Churn Memory Compaction

Deleted typed values leave reusable holes when survivors occupy later indexes,
and Go slice capacity plus the activated Merkle table retain their prior high
water marks. `CompactMemory` prepares dense in-memory typed pools, duplicates
the C trie, rewrites every compact index in the duplicate, and atomically swaps
the complete state under the trie write lock. Disk-spill indexes stay stable to
preserve unique file ownership. It also rebuilds TTL and bounded key-stat
metadata without changing values, expiration, statistics, or Merkle roots.

```sh
make bench-big-wins BIG_WINS_BENCH='^BenchmarkBigWins/(ChurnRetentionBaseline|ChurnRetentionCompacted)$' BIG_WINS_KEYS=100000 BENCHTIME=1x COUNT=5
```

The fixture activates Merkle tracking, inserts 100,000 string keys, deletes
90,000, retains every tenth key, forces a Go collection, and measures live heap
and deterministic outer backing. Backing bytes include typed pool slices,
reusable-index metadata, and Merkle table/leaves/scratch; they exclude nested
payloads, allocator metadata, and C allocator pages. Values are five-run medians
on the Ryzen 9 5950X host.

| Metric | No compaction | Compacted | Improvement / cost |
| --- | ---: | ---: | ---: |
| Retained backing | 9,679,075 B | 704,912 B | 13.73x lower, 92.7% reclaimed |
| Retained Go heap | 9,850,096 B | 884,600 B | 11.13x lower, 91.0% reclaimed |
| Full insert/delete cycle | 226,957,849 ns | 239,289,284 ns | 5.4% slower with compaction |
| Compaction pause | 0 | 8,801,699 ns | one exclusive rebuild |
| Cumulative heap/cycle | 49,003,944 B | 50,204,120 B | 2.4% more transient allocation |
| Allocations/cycle | 481,254 | 491,287 | 2.1% more transient allocations |

The daemon keeps periodic compaction off by default. Set
`MEMORY_COMPACTION_INTERVAL` to a positive duration to opt in; unchanged ticks
are skipped. The peak during a rebuild temporarily includes both C tries,
compaction remap arrays, and both generations of outer pool slices, so operators
should schedule it with enough memory headroom and outside latency-sensitive
windows.

<a id="single-pass-expiration-index-compaction"></a>
#### Single-Pass Expiration-Index Compaction

The nonempty TTL compaction path previously copied every expiration index entry
into a right-sized map, then called `rebuildExpirationHeapLocked`. That rebuild
creates another right-sized map and derives every authoritative position while
restoring heap order, so the first complete map and all of its insertions were
discarded immediately.

The existing heap/index consistency test was extended first to compact after
100 deadline inserts, updates, and removals, then verify every map position and
heap parent. A focused benchmark compacts a complete trie with 10,000 live
string keys and reverse-ordered deadlines. Values are ten-run medians on the
Ryzen 9 5950X host.

```sh
make run CMD='go test . -run=TestExpirationHeapIndexesStayConsistentAcrossUpdatesAndRemovals -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkCompactMemoryExpirationIndex10k -benchmem -benchtime=1x -count=10'
```

| Full 10k-TTL compaction | Redundant map copy | Single heap rebuild | Improvement |
| --- | ---: | ---: | ---: |
| Median time | 8,254,494 ns | 6,120,101 ns | 1.35x faster |
| Cumulative heap | 1,562,256 B | 1,125,320 B | 1.39x lower; 436,936 B removed |
| Allocations | 10,095 | 10,060 | 35 fewer allocations |

The final path constructs the index once from the expiration heap. TTL values,
deadline updates, removals, heap ordering, compaction locking, retained memory,
wire bytes, snapshots, and persistent formats are unchanged.

<a id="linear-expiration-index-rebuild"></a>
##### Linear Expiration-Index Rebuild

The first single-pass version still fed the already-valid expiration heap back
through `expirationHeap.Push`. In heap-array order every parent is seen before
its children, so each insertion only rechecked an invariant that was already
true and rewrote positions that can be derived directly from the array index.

The pre-change correctness guard preserves the exact heap order and every
deadline across compaction, in addition to validating each index and parent
relationship. The final implementation clones the heap into
the same right-sized backing used before and writes one map entry per fixed
position. It does not retain the old high-water backing.

```sh
make run CMD='go test . -run=TestExpirationHeapIndexesStayConsistentAcrossUpdatesAndRemovals -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkCompactMemoryExpirationIndex10k -benchmem -benchtime=10x -count=10'
```

Ten complete compactions ran per sample. The committed heap-`Push` control was
restored and measured between candidate runs; the conservative post-control
candidate repeat is reported here.

| Repeated full 10k-TTL compaction | Heap `Push` control | Linear final | Improvement |
| --- | ---: | ---: | ---: |
| Median time | 6,033,008 ns | 5,964,272 ns | 1.01x faster |
| Cumulative heap | 1,125,278 B | 1,125,278 B | identical |
| Allocations | 10,058 | 10,058 | identical |

The comparison has identical heap and allocations. Compaction policy, lock
scope, retained capacity, TTL behavior, snapshots, wire, and persistence are
unchanged.

<a id="validated-bounded-key-stat-compaction"></a>
#### Validated Bounded Key-Stat Compaction

Bounded telemetry maintains one slot per tracked key plus reusable holes from
deletes. Compaction already cloned the key-to-stat map, then always allocated a
second full-sized `seen` map to walk the slots, remove holes or duplicates, and
append any map entries missing from the slot array. Healthy runtime state has a
stronger invariant: each nonempty slot's stat points back to that exact slot,
and the number of nonempty slots equals the map cardinality.

The pre-change test creates evictions and holes, records slot order and the
eviction hand, compacts, and verifies every resulting slot index. A second test
deliberately installs duplicate and missing internal slots to prove the prior
repair fallback remains available. The common path now validates slot pointers
and cardinality without allocating; only inconsistent state constructs the
`seen` map and runs the original repair algorithm.

```sh
make run CMD='go test . -run="TestCompactMemory(Preserves|Repairs).*BoundedKeyStatsSlots" -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkCompactMemoryBoundedKeyStats100k -benchmem -benchtime=1x -count=10'
```

The benchmark compacts a complete trie with 100,000 live string keys and a
bounded telemetry capacity of 100,000. Values are ten-run medians on the Ryzen
9 5950X host.

| Full bounded-telemetry compaction | Seen-map baseline | Validated final | Improvement |
| --- | ---: | ---: | ---: |
| Median time | 169,162,035 ns | 166,163,998 ns | 1.02x faster |
| Cumulative heap | 11,405,896 B | 7,910,752 B | 1.44x lower; 3,495,144 B removed |
| Allocations | about 100,577 | 100,317 | about 260 fewer |

Tracked statistics, slot order, eviction hand, deletion holes, full/off modes,
compaction locking, snapshots, wire, and persistence are unchanged. The
validation adds no retained state and runs only during explicit or periodic
compaction.

<a id="online-generational-compaction-rollback"></a>
#### Online Generational Compaction Rollback

Commit `30bc334` tested a staged replacement generation to reduce the exclusive
compactor's reader pause. It scanned under bounded 256-key page locks, rebuilt
off-lock, replayed tracked mutations, and performed only the final swap under
the write lock. Correctness tests covered concurrent mutation, TTL, telemetry,
Merkle state, lazy disk references, snapshots, and failed staging. Commit
`c3085d2` reverted the design after its complete-path resource costs failed the
acceptance gate.

The historical pause fixture compared baseline `0a7f582` and `30bc334` on the
same 100,000 insert/90,000 delete workload with an active reader. Values are
seven-run medians on the Ryzen 9 5950X host.

| Compaction metric | Exclusive rebuild | Online generation | Result |
| --- | ---: | ---: | ---: |
| Maximum reader pause | 10,257,679 ns | 1,091,339 ns | 9.40x shorter |
| Total compaction | 10,236,288 ns | 15,785,538 ns | 1.54x slower |
| Cumulative heap | 909,472 B | 6,180,888 B | 6.80x higher |
| Allocations | 10,030 | 26,753 | 2.67x higher |

The matching retention fixture kept every tenth key and forced a Go
collection. The staged candidate reduced retained backing from 2,372,824 B to
180,224 B (13.17x) and retained Go heap from 2,384,040 B to 445,168 B (5.36x).
Those steady-state savings did not offset the immediate rebuild cost: the full
churn cycle was 8.7% slower, allocated 22.8% more cumulative heap, and allocated
2.7% more objects. The rollback restored the exclusive compactor, so none of
the staged generation, mutation replay, or dual-generation transient overhead
remains. Periodic compaction remains off by default.

### Pipelined Live gRPC Replication

One long-lived gRPC stream per target now has a dedicated sender/receiver loop
and a configurable acknowledgement window (32 by default). Acknowledgements are
matched by sequence, while replay safety is scoped per key so unrelated writes
may complete out of order without being discarded.

Live callers targeting the same node are also coalesced into bounded wire
batches. The default groups at most 32 commands, uses the existing 1 MiB byte
limit, and performs no timed wait. Sixteen scheduler yields give concurrent
callers an allocation-free opportunity to enter the queue; one server ack is
then fanned out to every grouped caller. Set the command limit to 1 for the
previous one-command-per-batch path.

```sh
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationLiveTransport10K -benchtime=1x -count=5 -benchmem'
make bench-live-replication BENCHTIME=1x COUNT=7
```

The fixture replicates 10,000 unique writes from 32 callers to one local target.
Both rows complete with all 10,000 target keys; values are five-run medians.

| Transport | Time/10k | Wire B/op | Heap B/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| HTTP/protobuf | 178,078,879 ns | 1,868,894 | 352,841,520 | 3,634,481 | baseline |
| Pipelined gRPC stream | 167,797,441 ns | 1,081,746 | 315,918,328 | 2,818,945 | 1.06x CPU, 1.73x wire, 1.12x heap, 1.29x allocations |

The pre-change 32-caller path delivered only 4,402 of 10,000 keys in the same
correctness fixture because a global source sequence rejected valid out-of-order
writes, so it has no valid performance row. The new path's main win is complete,
bounded concurrent delivery; HTTP remains the configurable fallback. Raising
the window can increase in-flight message memory and should be load-tested
against the target's latency and HTTP/2 flow-control limits.

The micro-batch comparison starts from the valid pipelined implementation after
atomic telemetry was enabled. Both sides deliver all 10,000 keys. Values are
medians from five baseline runs and seven final runs on the Ryzen 9 5950X host.

| Live gRPC mode | Time/10k | Batches/op | Wire B/op | Heap B/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| One command/batch | 193,298,572 ns | 10,000 | 1,081,747 | 392,904,432 | 2,827,049 | baseline |
| Zero-wait micro-batch | 149,681,659 ns | 2,910 | 368,252 | 383,959,736 | 2,051,047 | 1.29x CPU, 3.44x batches, 2.94x wire, 1.38x allocations |

At one caller, where no grouping is possible, the zero-wait yields changed the
median from 599,117,778 ns to 608,728,390 ns for 10,000 commands, a 1.6%
throughput cost. A 25 us window reduced the 32-caller median further to 1,332
batches and 182,533 wire bytes, but slowed execution to 183,434,195 ns and
raised cumulative heap to 421,507,920 bytes. The default therefore remains
zero wait. Use a positive window only when bandwidth matters more than latency
and after measuring the deployment.

The next pass keeps that zero-wait behavior but delays protobuf request and
key/value-slice allocation until compatible queued jobs have been grouped.
Topology fingerprints and the fingerprint-verification decision are also
cached when a validated topology is installed instead of cloning, normalizing,
and sorting the topology for every live command. Both sides delivered all
10,000 keys. Values are seven-run medians from the same 32-caller fixture.

| Live gRPC allocation path | Time/10k | Batches/op | Wire B/op | Heap B/op | Allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Request per caller before grouping | 154,264,762 ns | 2,959 | 374,858 | 353,626,600 | 2,037,671 | baseline |
| Group payload views, then allocate | 126,892,996 ns | 2,305 | 298,577 | 303,233,152 | 940,900 | 1.22x CPU, 1.28x batches, 1.26x wire, 1.17x heap, 2.17x allocations |

The request owns its key/value slice headers while referencing immutable job
payload bytes until the acknowledgement is delivered. This removes duplicate
per-caller protobuf envelopes without copying values. Topology reloads pay the
fingerprint cost once in `Set`; ordinary reads remain protected by the store's
read lock.

<a id="lazy-grpc-session-maps"></a>
#### Lazy gRPC Session Maps

Both live and anti-entropy gRPC sessions previously allocated target and
fallback maps at construction, then allocated another empty target map during
close. Target lookup and fallback lookup already support nil maps. Sessions now
allocate the target map immediately before publishing the first successfully
opened stream, allocate fallback state only when a sticky sync session actually
falls back, and clear the target map to nil during close. Because live sessions
do not retain sticky fallback decisions, live sessions never allocate sticky
fallback state.

The test-first lifecycle fixture proves new live/sync maps are nil, an actual
sync fallback initializes only fallback state, a live fallback initializes
neither map, and close leaves target state nil. Existing stream, fallback,
digest-repair, batching, acknowledgement, and shutdown tests cover active
sessions ten times.

```sh
make run CMD='go test . -run="TestReplicationGRPC(SessionDefersOptionalMaps|Stream)" -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationGRPCSessionLifecycle -benchmem -benchtime=100000x -count=10 -cpu=1'
```

| Unused session create plus close, ten-run median | Eager maps | Lazy maps | Improvement |
| --- | ---: | ---: | ---: |
| Live session | 170.3 ns; 208 B; 4 allocs | 57.10 ns; 64 B; 1 alloc | 2.98x faster; 3.25x lower heap; 4x fewer allocations |
| Sync session | 175.7 ns; 208 B; 4 allocs | 57.88 ns; 64 B; 1 alloc | 3.04x faster; 3.25x lower heap; 4x fewer allocations |

The first active target still creates the same required map under the existing
session mutex, after connection establishment and before publication. Sticky
fallback creates one map only on the existing error path. No extra steady-state
branch, retained target, goroutine, connection, request, wire byte,
configuration, or compatibility behavior was added.

<a id="direct-single-target-grpc-sync-dispatch"></a>
#### Direct Single-Target gRPC Sync Dispatch

Anti-entropy commonly produces one gRPC task group. The sync-session dispatcher
previously built a target-key string, a map of target indexes, and a target-order
slice before discovering that one group cannot run concurrently with another
target. It now writes that group's result directly into the already-required
single result slot. Two or more groups retain the existing target grouping,
same-target serialization, bounded fanout, and result ordering.

The test-first fixture compares direct execution with the one-group wrapper and
checks the queued flag, result count, target metadata, and error result. Existing
parallel, bounded-fanout, stream, fallback, digest-repair, and shutdown tests
cover the unchanged multi-group and active-stream behavior.

```sh
make run CMD='go test . -run=TestReplicationGRPCSyncSessionSingleTaskGroupMatchesDirectExecution -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationGRPCSingleTaskGroupPlanning -benchmem -benchtime=300000x -count=10 -cpu=1'
```

| Ten-run median | Generic grouping | Direct one-group dispatch | Improvement |
| --- | ---: | ---: | ---: |
| One group | 569.1 ns; 808 B; 8 allocs | 426.8 ns; 384 B; 4 allocs | 1.33x faster; 2.10x lower heap; 2x fewer allocations |
| Two groups, same target | 890.2 ns; 984 B; 12 allocs | 881.1 ns; 984 B; 12 allocs | 1.01x faster; heap and allocations unchanged |
| Four distinct targets | 1,516 ns; 1,392 B; 21 allocs | 1,512 ns; 1,392 B; 21 allocs | CPU neutral within 0.3%; heap and allocations unchanged |

The fixture intentionally uses an invalid empty sync payload so it isolates
dispatch planning from network, protobuf, and goroutine scheduling. The branch
does not change request construction, wire bytes, storage, retries, fallback,
timeouts, configuration, or public behavior.

<a id="normalized-topology-store-routing"></a>
#### Normalized Topology-Store Routing

`TopologyStore` validates, clones, and sorts nodes, shards, replicas, and bucket
ranges whenever topology is installed. Its per-key `Route` method nevertheless
called the generic `ClusterTopology.RouteForKey`, which cloned and sorted every
shard again before selecting one. Store-backed routing now selects directly
from the immutable normalized order under the existing read lock and clones
only the returned shard. Public `ClusterTopology.RouteForKey` remains unchanged
because standalone topology values can still be unsorted.

The test-first ownership fixture compares store routing with public routing,
mutates the returned replica, owner, and bucket storage, and proves a later
route is unchanged. A separate 4,096-key sweep proves exact route equivalence
for full-replica, one/four-shard hash routing, and compact virtual-bucket ranges.
The alternating benchmark runs the new selector and the exact former
lock-plus-generic-route implementation in both orders inside one binary.

```sh
make run CMD='go test . -run="TestTopologyStore(RouteMatchesPublicRouting|RouteReturnsOwnedNormalizedRoute|RoutesVirtualBucketRanges|RoutesFullReplicaMode|ValidatesNormalizesAndRoutes)" -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkTopologyStoreRoute -benchmem -benchtime=100000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkTopologyStoreRouteAlternating -benchtime=100000x -count=10 -cpu=1'
```

| Ten-run median | Clone and sort normalized backing | Select and clone one shard | Improvement |
| --- | ---: | ---: | ---: |
| Full replica, two nodes | 401.25 ns; 440 B; 6 allocs | 138.5 ns; 48 B; 2 allocs | 2.90x faster; 9.17x lower heap; 3x fewer allocations |
| Sharded, one shard | 241.4 ns; 120 B; 4 allocs | 134.3 ns; 48 B; 2 allocs | 1.80x faster; 2.50x lower heap; 2x fewer allocations |
| Sharded, four shards | 505.3 ns; 488 B; 9 allocs | 142.55 ns; 80 B; 2 allocs | 3.54x faster; 6.10x lower heap; 4.50x fewer allocations |
| Virtual buckets, four shards | 515.8 ns; 492 B; 10 allocs | 159.1 ns; 84 B; 3 allocs | 3.24x faster; 5.86x lower heap; 3.33x fewer allocations |

CPU values are the alternating same-binary medians. Heap and allocation values
come from the unchanged route fixture before and after implementation. The
selected shard's replica slice and the route's owner slice remain separate
copies, while bucket metadata remains caller-owned. Topology update locking,
hashes, bucket selection, owner order, configuration, wire, persistence, and
public behavior are unchanged; the read-lock hold is strictly shorter.

<a id="direct-election-key-routing"></a>
#### Direct Election-Key Routing

`ElectionStore.LeaderForKey` previously cloned the complete topology, called
the generic clone-and-sort router, allocated an active-state map for every
topology node, and finally scanned the selected shard's candidates. It now
takes an owned selected route plus the matching immutable normalized node view
under the topology read lock, releases that lock, and checks only primary and
replica election records under the election read lock. Node lookup uses the
already-sorted topology without allocating.

The test-first fixture compares 4,096 keys per topology with the exact former
snapshot implementation after a primary is marked offline, mutates returned
route and candidate slices, and verifies later reads are unchanged. Existing
tests cover healthy primary selection, timeout, all-offline unavailability,
full-replica promotion, and persisted maintenance. A concurrent topology
update, heartbeat, and route fixture also passes the race detector.

```sh
make run CMD='go test . -run="TestElectionStore(LeaderForKeyMatchesSnapshotControl|LeaderForKeyDuringTopologyAndHeartbeatUpdates|KeepsHealthyPrimaryAndPromotesReplica|TimesOutHeartbeats|ReportsUnavailableWhenAllCandidatesOffline|FullReplicaLeaderUsesSelfThenReplica|ExcludesPersistedMaintenanceNode)" -count=10'
make run CMD='go test -race . -run=TestElectionStoreLeaderForKeyDuringTopologyAndHeartbeatUpdates -count=3'
make run CMD='go test . -run=NONE -bench=BenchmarkElectionStoreLeaderForKey -benchmem -benchtime=100000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkElectionStoreLeaderForKeyAlternating -benchtime=100000x -count=10 -cpu=1'
```

| Ten-run median | Topology snapshot plus active map | Selected route plus direct candidates | Improvement |
| --- | ---: | ---: | ---: |
| Full replica, healthy | 729.95 ns; 936 B; 10 allocs | 231.8 ns; 80 B; 3 allocs | 3.15x faster; 11.70x lower heap; 3.33x fewer allocations |
| Full replica, primary offline | 731.05 ns; 936 B; 10 allocs | 258.65 ns; 80 B; 3 allocs | 2.83x faster; 11.70x lower heap; 3.33x fewer allocations |
| One shard, healthy | 643.95 ns; 680 B; 10 allocs | 226.15 ns; 80 B; 3 allocs | 2.85x faster; 8.50x lower heap; 3.33x fewer allocations |
| One shard, primary offline | 673.2 ns; 680 B; 10 allocs | 255.8 ns; 80 B; 3 allocs | 2.63x faster; 8.50x lower heap; 3.33x fewer allocations |
| Four shards, healthy | 1,337.5 ns; 1,688 B; 18 allocs | 263.35 ns; 128 B; 3 allocs | 5.08x faster; 13.19x lower heap; 6x fewer allocations |
| Four shards, primary offline | 1,354 ns; 1,688 B; 18 allocs | 274.85 ns; 128 B; 3 allocs | 4.93x faster; 13.19x lower heap; 6x fewer allocations |
| Virtual buckets, healthy | 1,386.5 ns; 1,740 B; 20 allocs | 282.45 ns; 132 B; 4 allocs | 4.91x faster; 13.18x lower heap; 5x fewer allocations |
| Virtual buckets, primary offline | 1,433.5 ns; 1,740 B; 20 allocs | 298.1 ns; 132 B; 4 allocs | 4.81x faster; 13.18x lower heap; 5x fewer allocations |

CPU values are alternating same-binary medians; heap and allocations are the
separate unchanged fixture before and after implementation. The topology store
replaces validated generations instead of mutating their backing arrays, so
the internal node view remains immutable after its short read lock. Election
records retain their own read lock, and no topology lock is held while waiting
for it. Candidate order, timestamps, timeout boundaries, maintenance, response
ownership, configuration, wire, persistence, and public behavior are unchanged.

<a id="allocation-free-election-node-updates"></a>
#### Allocation-Free Election Node Updates

`ElectionStore.Heartbeat` and `MarkOffline` previously cloned the complete
topology and all shard replica slices before linearly checking whether one node
ID was registered. Installed topology nodes are already validated and sorted.
The update path now performs one read-locked binary lookup in that normalized
node list, releases the topology lock, and writes the same election record
under the same election mutex.

The test-first generation fixture proves a node is accepted before a topology
update, rejected after removal, and that its replacement is immediately valid.
Existing unknown-node, heartbeat timeout, failover, and concurrent topology
tests cover validation and record behavior. The alternating benchmark compares
the direct lookup with the exact former clone-and-linear-scan path in both
orders; the separate fixture records allocation behavior.

```sh
make run CMD='go test . -run="TestElectionStore(NodeUpdatesFollowTopologyGeneration|RejectsUnknownNode|LeaderForKeyMatchesSnapshotControl|LeaderForKeyDuringTopologyAndHeartbeatUpdates|KeepsHealthyPrimaryAndPromotesReplica|TimesOutHeartbeats)" -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkElectionStoreNodeUpdate -benchmem -benchtime=100000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkElectionStoreNodeUpdateAlternating -benchtime=100000x -count=10 -cpu=1'
```

| Ten-run alternating median | Full topology clone | Normalized node lookup | Improvement |
| --- | ---: | ---: | ---: |
| Full replica heartbeat | 204.65 ns; 208 B; 1 alloc | 61.535 ns; 0 B; 0 allocs | 3.33x faster; timed allocation eliminated |
| Full replica offline mark | 201.8 ns; 208 B; 1 alloc | 61.595 ns; 0 B; 0 allocs | 3.28x faster; timed allocation eliminated |
| One-shard heartbeat | 279.65 ns; 272 B; 3 allocs | 60.91 ns; 0 B; 0 allocs | 4.59x faster; timed allocations eliminated |
| One-shard offline mark | 276.45 ns; 272 B; 3 allocs | 58.805 ns; 0 B; 0 allocs | 4.70x faster; timed allocations eliminated |
| Four-shard heartbeat | 535.5 ns; 896 B; 6 allocs | 60.505 ns; 0 B; 0 allocs | 8.85x faster; timed allocations eliminated |
| Four-shard offline mark | 529.1 ns; 896 B; 6 allocs | 59.99 ns; 0 B; 0 allocs | 8.82x faster; timed allocations eliminated |
| Virtual-bucket heartbeat | 549.1 ns; 944 B; 7 allocs | 59.99 ns; 0 B; 0 allocs | 9.15x faster; timed allocations eliminated |
| Virtual-bucket offline mark | 556.85 ns; 944 B; 7 allocs | 61.245 ns; 0 B; 0 allocs | 9.09x faster; timed allocations eliminated |

The alternating CPU values include equal per-path timing overhead and are more
conservative than the standalone 33-36 ns final medians. Heap and allocations
come from the unchanged standalone fixture before and after implementation.
The lookup still observes the current topology under its read lock; as before,
a later topology generation can replace membership after validation but before
the independent election record write. Node-ID trimming, unknown-node errors,
timestamps, offline state, mutex scope, configuration, wire, persistence, and
public behavior are unchanged.

<a id="normalized-election-status-generation"></a>
#### Normalized Election Status Generation

`ElectionStore.Status` previously cloned every topology slice, cloned shard
replica slices again, and sorted nodes and shards even though `TopologyStore`
already stores a validated normalized generation. Status generation now borrows
that immutable generation after a short topology read lock, then builds the
same owned node, leader, candidate, and timestamp response under the existing
election read lock. Sharded status iterates the normalized shard order directly;
full-replica status derives its one owned shard from normalized nodes.

The test-first fixture compares healthy and primary-offline output with the
exact former snapshot builder for all topology modes, mutates returned node and
candidate slices, and proves later status is unchanged. A concurrent topology
replacement/status test proves each borrowed generation remains complete and
sorted, and passes the race detector.

```sh
make run CMD='go test . -run="TestElectionStore(StatusMatchesSnapshotControl|StatusDuringTopologyUpdates|KeepsHealthyPrimaryAndPromotesReplica|TimesOutHeartbeats|ReportsUnavailableWhenAllCandidatesOffline|FullReplicaLeaderUsesSelfThenReplica|ExcludesPersistedMaintenanceNode)" -count=10'
make run CMD='go test -race . -run=TestElectionStoreStatusDuringTopologyUpdates -count=3'
make run CMD='go test . -run=NONE -bench=BenchmarkElectionStoreStatus -benchmem -benchtime=100000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkElectionStoreStatusAlternating -benchtime=100000x -count=10 -cpu=1'
```

| Ten-run median | Cloned/sorted topology | Borrowed normalized topology | Improvement |
| --- | ---: | ---: | ---: |
| Full replica, healthy | 931.05 ns; 1,248 B; 15 allocs | 412.45 ns; 480 B; 6 allocs | 2.26x faster; 2.60x lower heap; 2.50x fewer allocations |
| Full replica, primary offline | 960.45 ns; 1,272 B; 16 allocs | 494.75 ns; 504 B; 7 allocs | 1.94x faster; 2.52x lower heap; 2.29x fewer allocations |
| One shard, healthy | 808.05 ns; 944 B; 14 allocs | 387.1 ns; 464 B; 5 allocs | 2.09x faster; 2.03x lower heap; 2.80x fewer allocations |
| One shard, primary offline | 881.35 ns; 968 B; 15 allocs | 435.8 ns; 488 B; 6 allocs | 2.02x faster; 1.98x lower heap; 2.50x fewer allocations |
| Four shards, healthy | 1,851 ns; 2,432 B; 25 allocs | 800.0 ns; 976 B; 8 allocs | 2.31x faster; 2.49x lower heap; 3.13x fewer allocations |
| Four shards, primary offline | 1,937.5 ns; 2,456 B; 26 allocs | 909.0 ns; 1,000 B; 9 allocs | 2.13x faster; 2.46x lower heap; 2.89x fewer allocations |
| Virtual buckets, healthy | 1,899 ns; 2,480 B; 26 allocs | 803.8 ns; 976 B; 8 allocs | 2.36x faster; 2.54x lower heap; 3.25x fewer allocations |
| Virtual buckets, primary offline | 1,950.5 ns; 2,504 B; 27 allocs | 931.2 ns; 1,000 B; 9 allocs | 2.09x faster; 2.50x lower heap; 3x fewer allocations |

CPU values are alternating same-binary medians; heap and allocations come from
the unchanged standalone fixture before and after implementation. At this
stage the active node map and every response-owned slice remained present; the
following pass removes that map from ordinary topology generations. Topology generations
are replaced rather than mutated, and the topology lock is released before
the election lock is acquired. Maintenance, heartbeat timeout, failover,
ordering, timestamps, response ownership, configuration, wire, persistence,
and public behavior are unchanged.

<a id="election-record-status-leader-lookup"></a>
#### Election-Record Status Leader Lookup

After normalized status generation, `ElectionStore.Status` still constructed a
temporary `map[string]bool` for every topology node and then looked candidates
up in that map. Ordinary topology generations now elect leaders from the
existing read-locked election-record map. An untracked candidate remains
assumed online; tracked candidates apply the same explicit-offline and exact
heartbeat-timeout rules. The already-required owned node-status response is
still built once with one shared timestamp.

Maintenance is the one case where an election record alone cannot describe
liveness. `TopologyStore` therefore caches a `hasMaintenance` bit beside its
existing fingerprint and updates both under the same topology-generation lock.
Maintenance generations retain the former active-map algorithm. On amd64 the
extra bit consumes existing struct padding; an explicit layout check reported
`old=184 new=184 actual=184`. No persistent per-node index or response field was
added.

The test-first control compares exact responses with both the normalized
active-map builder and the older full-snapshot builder. It covers healthy,
primary-offline, two-offline, quarter/half/three-quarter-offline, and complete
outage states at 1, 2, 4, 16, 32, and 64 nodes; a 64-node shared-primary fixture
forces the same failed candidates across every shard. A separate maintenance
generation toggles maintenance through `TopologyStore.Set`, and the concurrent
generation test remains race-clean.

```sh
make run CMD='go test . -run="TestElectionStore(StatusMatchesSnapshotControl|StatusDuringTopologyUpdates|ExcludesPersistedMaintenanceNode|KeepsHealthyPrimaryAndPromotesReplica|TimesOutHeartbeats|ReportsUnavailableWhenAllCandidatesOffline|FullReplicaLeaderUsesSelfThenReplica)" -count=10'
make run CMD='go test -race . -run=TestElectionStoreStatusDuringTopologyUpdates -count=3'
make run CMD='go test . -run=NONE -bench=^BenchmarkElectionStoreStatusActiveMapAlternating$$ -benchmem -benchtime=10000x -count=10 -cpu=1'
```

| Ten-run alternating median | Normalized active map | Election records | Improvement |
| --- | ---: | ---: | ---: |
| One shard, healthy | 459.15 ns; 464 B; 5 allocs | 289.9 ns; 208 B; 3 allocs | 1.58x faster; 256 fewer heap bytes; 2 fewer allocations |
| Four shards, healthy | 961.65 ns; 976 B; 8 allocs | 744.65 ns; 720 B; 6 allocs | 1.29x faster; 256 fewer heap bytes; 2 fewer allocations |
| 64 nodes/64 shards, healthy | 12,100 ns; 14,680 B; 70 allocs | 9,153 ns; 11,136 B; 66 allocs | 1.32x faster; 3,544 fewer heap bytes; 4 fewer allocations |
| 64 nodes/64 shards, all offline | 14,485 ns; 16,216 B; 134 allocs | 13,271.5 ns; 12,672 B; 130 allocs | 1.09x faster; 3,544 fewer heap bytes; 4 fewer allocations |
| 64-node shared-primary, two shared candidates offline | 13,357 ns; 14,728 B; 72 allocs | 10,982 ns; 11,184 B; 68 allocs | 1.22x faster; 3,544 fewer heap bytes; 4 fewer allocations |
| 64 nodes, one maintenance node | 13,878.5 ns; 14,680 B; 70 allocs | 12,933.5 ns; 14,680 B; 70 allocs | 1.07x faster; memory unchanged |

The complete-outage and shared-primary controls rejected an earlier pure
binary-search prototype, which was respectively 1.08x and 1.11x slower than
the active map. A failed-probe threshold also regressed concentrated failures
because it built the map after paying for repeated searches. Neither prototype
is retained. The final record-map path is faster in every measured ordinary,
degraded, adversarial, and maintenance fixture. Locks, timeout boundaries,
maintenance precedence, failover order, response ownership, topology updates,
configuration, wire, storage, and persistence are unchanged.

<a id="cached-replication-routing-fingerprint"></a>
#### Cached Replication Routing Fingerprint

Every anti-entropy routing snapshot clones the validated topology and builds
node, shard-owner, and target lookup maps. It previously normalized, cloned,
sorted, and hashed that topology clone again even though `TopologyStore`
already computes and caches the same normalized fingerprint when topology is
installed. The store now returns a cloned topology and its matching cached
fingerprint under one read lock. Public topology reads and fingerprint calls
retain their existing cloning and ownership behavior.

The test-first fixture proves the returned fingerprint matches both the cloned
topology and the public store fingerprint, mutations to the clone cannot alter
store backing, and a nil store remains safe. Existing routing-snapshot tests
cover shard ownership and election state. The alternating benchmark runs the
cached and explicit-rehash paths in both orders inside one binary; the separate
construction benchmark records pre-change and final heap behavior.

```sh
make run CMD='go test . -run="TestTopologyStoreReplicationSnapshotClonesMatchingFingerprint|TestReplicationRoutingSnapshot" -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationRoutingSnapshotConstruction -benchmem -benchtime=10000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationRoutingSnapshotFingerprintAlternating -benchtime=10000x -count=10 -cpu=1'
```

| Ten-run median | Rehash fingerprint | Cached fingerprint | Improvement |
| --- | ---: | ---: | ---: |
| One shard, two nodes | 3,238 ns; 3,920 B; 52 allocs | 1,621.5 ns; 3,032 B; 14 allocs | 2.00x faster; 1.29x lower heap; 3.71x fewer allocations |
| Four shards, five nodes | 7,028.5 ns; 7,832 B; 129 allocs | 3,447.5 ns; 5,600 B; 34 allocs | 2.04x faster; 1.40x lower heap; 3.79x fewer allocations |

CPU values are the alternating same-binary medians. Heap and allocation values
are the medians from the unchanged construction fixture before and after the
implementation. Topology installation still pays exactly one fingerprint
calculation. Snapshot construction retains the same topology clone, maps,
target ordering, election snapshot, configuration, wire bytes, persistence,
and public behavior; it only reuses the already-required immutable hash.

<a id="normalized-replication-target-precomputation"></a>
#### Normalized Replication Target Precomputation

Routing-snapshot construction precomputes the ordered replication targets for
every shard. The private target helper previously allocated and consulted a
`seen` map even though its owners always come from a validated normalized
topology. Normalization trims owner IDs, requires every owner to reference a
registered node, and rejects duplicate primary/replica owners before a snapshot
can be built. The helper is now named
`precomputedNormalizedReplicationTargets` and relies on that established
invariant instead of repeating duplicate suppression for every shard.

The test-first
`TestPrecomputedReplicationTargetsMatchDeduplicatingControl` compares every
64-shard result with the former deduplicating implementation for multiple self
nodes and online maps. Existing snapshot routing and immutable-target reuse
tests continue to cover target filtering, sorting, ownership, and reuse.

```sh
make run CMD='go test . -run="TestPrecomputedReplicationTargetsMatchDeduplicatingControl|TestReplicationRoutingSnapshot(ReusesPrecomputedTargets|MatchesDynamicRouting)" -count=10'
make run CMD='go test . -run=NONE -bench=^BenchmarkPrecomputedReplicationTargetsDedupAlternating$$ -benchmem -benchtime=10000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingSnapshotConstruction$$ -benchmem -benchtime=10000x -count=10 -cpu=1'
```

| Ten-run median | Deduplicating targets | Normalized targets | Improvement |
| --- | ---: | ---: | ---: |
| Focused two owners | 286.9 ns | 228.35 ns | 1.26x faster |
| Focused three owners | 461.95 ns | 445.9 ns | 1.04x faster |
| Focused 64 owners | 5,659.5 ns | 3,787.5 ns | 1.49x faster |
| Complete one-shard snapshot | 1,436.5 ns; 3,032 B; 14 allocs | 1,395 ns; 3,032 B; 14 allocs | 1.03x faster; memory unchanged |
| Complete four-shard snapshot | 3,591.5 ns; 5,600 B; 34 allocs | 3,121.5 ns; 5,600 B; 34 allocs | 1.15x faster; memory unchanged |
| Complete 64-shard snapshot | 47,579.5 ns; 84,759 B; 403 allocs | 43,078.5 ns; 84,709 B; 402 allocs | 1.10x faster; 50 fewer heap bytes; one fewer allocation |

The focused values are alternating same-binary medians; complete snapshot rows
are standalone before/after medians from the same host and command. Public or
arbitrary owner slices are not routed through this helper. Topology validation,
self exclusion, online filtering, missing-node rejection, target sorting,
snapshot ownership, configuration, wire, storage, and persistence are
unchanged.

A follow-up candidate also removed owner trimming, the empty-ID guard, and the
registered-node check. Although normalized topology proves all three
conditions, the complete same-binary path did not justify relying on them:

| Ten-run alternating median | Checked normalized owners | Unchecked candidate | Result |
| --- | ---: | ---: | ---: |
| Two shards | 1,830 ns | 1,973.5 ns | 1.08x slower |
| Four shards | 3,464 ns | 3,426.5 ns | 1.01x faster, within run noise |
| 64 shards | 45,341.5 ns | 44,230.5 ns | 1.03x faster |

Heap and allocations were identical in every paired fixture. The unchecked
production code was reverted; the test-only candidate and
`BenchmarkReplicationRoutingSnapshotUncheckedOwnersAlternating` retain the
reproducer. Owner cleanup and missing-node rejection therefore remain in the
private helper, while only the proven duplicate map is absent.

<a id="map-free-replication-routing-snapshots"></a>
#### Map-Free Replication Routing Snapshots

Routing snapshots previously cloned the normalized topology and then built and
retained a second `map[string]TopologyNode`. Snapshot construction used that
map only while precomputing each shard's immutable target slice. Topology
normalization already sorts unique node IDs, so construction now resolves each
owner with the existing allocation-free binary lookup and retains no node map.
All production target consumers use the precomputed slices; remote-source
membership is handled by the direct normalized-owner check below rather than by
rebuilding targets.

The test-first control builds complete map-backed and sorted-node snapshots at
2, 4, 8, 16, 32, and 64 shards, both without election state and with the last
node offline, then compares every field exactly. The target-helper matrix also
compares sorted lookup with the former map and deduplicating controls for
multiple self IDs and online maps. The benchmark retains the former node map in
a test-only wrapper so its allocation and lifetime match the removed field.

```sh
make run CMD='go test . -run="TestReplicationRoutingSnapshotSortedNodesMatchNodeMap|TestPrecomputedReplicationTargetsMatchDeduplicatingControl|TestReplicationRoutingSnapshot" -count=10'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingSnapshotSortedNodesAlternating$$ -benchtime=20000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingSnapshotNodeIndex$$ -benchmem -benchtime=10000x -count=10 -cpu=1'
```

| Ten-run median | Retained node map | Normalized sorted nodes | Improvement |
| --- | ---: | ---: | ---: |
| Two shards | 1,722.5 ns; 3,360 B; 18 allocs | 1,282.5 ns; 2,288 B; 16 allocs | 1.34x faster; 1.47x lower heap; two fewer allocations |
| Four shards | 3,304.5 ns; 5,440 B; 34 allocs | 2,819.5 ns; 4,368 B; 32 allocs | 1.17x faster; 1.25x lower heap; two fewer allocations |
| Eight shards | 5,426 ns; 8,448 B; 58 allocs | 4,850.5 ns; 7,376 B; 56 allocs | 1.12x faster; 1.15x lower heap; two fewer allocations |
| 16 shards | 11,547 ns; 21,472 B; 114 allocs | 10,673 ns; 17,288 B; 110 allocs | 1.08x faster; 1.24x lower heap; four fewer allocations |
| 32 shards | 22,151 ns; 42,464 B; 210 allocs | 21,132.5 ns; 34,184 B; 206 allocs | 1.05x faster; 1.24x lower heap; four fewer allocations |
| 64 shards | 45,159 ns; 84,704 B; 402 allocs | 42,968 ns; 68,232 B; 398 allocs | 1.05x faster; 1.24x lower heap; four fewer allocations |

CPU values are alternating same-binary medians; heap and allocation values are
exact across every isolated run. The 64-shard snapshot removes 16,472 heap
bytes. The private target accessor now exposes only
its immutable precomputed result, matching every production caller. Topology
cloning, self and online filtering, missing-node rejection, target sorting,
election generation, shard routing, wire, storage, and persistence are
unchanged.

<a id="aligned-replication-shard-state"></a>
#### Aligned Replication Shard State

After removing the retained node index, routing snapshots still built three
maps keyed by shard ID for leaders, owner IDs, and precomputed targets. The
topology store already normalizes shards into unique ascending ID order, and
all three values are produced in that same pass. The snapshot now stores them
in aligned slices and carries the selected slice index through complete
route-and-target operations. Digest setup loops also enumerate the aligned
state directly. Defensive callers that provide only a shard ID use a binary
search over the same sorted immutable shard generation.

The test-first map control preserves the former constructor and route logic.
The exact matrix compares headers and all per-shard state at 2, 4, 8, 16, 32,
and 64 shards, with and without an offline election node. A second 4,096-key
matrix compares normal routes, scan routes, leaders, owners, targets, buckets,
and success results for hash sharding, non-contiguous IDs with reversed bucket
ranges, and full replication. Focused tests pass 20 repeated runs.

```sh
make run CMD='go test . -run="TestReplicationRoutingSnapshotShard|TestReplicationRouteTargetsNodeMatchesMaterializedControl|TestReplicationScanRouteForKey" -count=20'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingSnapshotShardSlicesAlternating$$ -benchtime=20000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingSnapshotShardSlices$$ -benchmem -benchtime=10000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingShardSlicesAlternating$$ -benchtime=200000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingShardSlicesRouteOnlyAlternating$$ -benchtime=500000x -count=10 -cpu=1'
```

| Ten-run construction median | Three shard maps | Three aligned slices | Improvement |
| --- | ---: | ---: | ---: |
| Two shards | 1,360 ns; 2,288 B; 16 allocs | 923.4 ns; 1,104 B; 13 allocs | 1.47x faster; 2.07x lower heap; three fewer allocations |
| Four shards | 2,905 ns; 4,368 B; 32 allocs | 2,365 ns; 3,424 B; 29 allocs | 1.23x faster; 1.28x lower heap; three fewer allocations |
| Eight shards | 4,889 ns; 7,376 B; 56 allocs | 4,304.5 ns; 6,976 B; 53 allocs | 1.14x faster; 1.06x lower heap; three fewer allocations |
| 16 shards | 10,153 ns; 17,288 B; 110 allocs | 8,794.5 ns; 14,080 B; 101 allocs | 1.15x faster; 1.23x lower heap; nine fewer allocations |
| 32 shards | 20,498.5 ns; 34,184 B; 206 allocs | 17,853 ns; 28,416 B; 197 allocs | 1.15x faster; 1.20x lower heap; nine fewer allocations |
| 64 shards | 44,388 ns; 68,232 B; 398 allocs | 38,234.5 ns; 55,808 B; 389 allocs | 1.16x faster; 1.22x lower heap; nine fewer allocations |

CPU values are paired same-binary medians. Heap and allocation values are
exact across the isolated runs.

| Complete route plus targets | Shard maps | Aligned slices | Improvement |
| --- | ---: | ---: | ---: |
| Hash, two shards | 75.385 ns | 70.88 ns | 1.06x faster |
| Hash, 16 shards | 95.83 ns | 70.07 ns | 1.37x faster |
| Hash, 64 shards | 89.315 ns | 65.025 ns | 1.37x faster |
| Bucket ranges, two shards | 81.245 ns | 72.535 ns | 1.12x faster |
| Bucket ranges, 16 shards | 114.35 ns | 80.66 ns | 1.42x faster |
| Bucket ranges, 64 shards | 150.3 ns | 102.55 ns | 1.47x faster |

Hash routes remain zero-allocation in both layouts. Explicit bucket routes
retain the same existing 4-byte allocation for the returned bucket pointer.
Route-only lookup is 1.12x/1.43x/1.44x faster at 2/16/64 hash shards and
1.13x-1.45x faster across the measured bucket fixtures. An initial integrated
version routed through a non-inlined shared index helper and was 1.11x-1.20x
slower at 2-8 shards; it was replaced before shipping by direct measured hot
operations. Topology validation, election results, target order, source and
online filtering, public routes, configuration, wire, storage, and persistence
are unchanged.

<a id="canonical-replication-owner-slices"></a>
#### Canonical Replication Owner Slices

Aligned routing snapshots still retained a separate outer owner-slice table
even though every `ElectionLeader` already carried the identical candidate
slice. With election enabled, construction also called the general election
helper after building owners for target planning, allocating a second
candidate backing per shard. Leader candidates are now the single immutable
owner representation: target planning and election scan the same backing, and
routes, digest checks, and defensive owner lookup read it directly. Removing
the unused outer field also shrinks the snapshot value by one 24-byte slice header
on the measured amd64 host.

The test-first control keeps the exact former snapshot shape, constructor, and
route methods. Exact tests compare snapshot headers, leaders, candidates,
targets, normal routes, and scan routes for hash sharding, reversed
non-contiguous bucket ownership, and full replication, both with and without
an offline election node. Each matrix covers 4,096 keys and passes 20 runs.

```sh
make run CMD='go test . -run="TestReplicationRoutingSnapshotLeaderCandidatesMatchOwnerSlices|TestReplicationRoutingSnapshot(SortedNodesMatchNodeMap|ShardSlicesMatchMaps|ShardSliceRoutesMatchMaps)|TestReplicationRouteTargetsNodeMatchesMaterializedControl" -count=20'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingLeaderCandidatesConstructionAlternating$$ -benchtime=10000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingLeaderCandidatesConstructionAlternating$$/^Election$$ -benchtime=20000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingLeaderCandidatesConstruction$$ -benchmem -benchtime=20000x -count=3 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingLeaderCandidatesRouteAlternating$$ -benchtime=500000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingLeaderCandidatesRoute$$ -benchmem -benchtime=1000000x -count=5 -cpu=1'
```

| Election snapshot, paired median | Separate owner slices | Leader candidates | Improvement |
| --- | ---: | ---: | ---: |
| Two shards | 1,293.5 ns; 1,424 B; 17 allocs | 1,140.5 ns; 1,312 B; 14 allocs | 1.13x faster; 112 fewer B; three fewer allocations |
| Four shards | 2,620 ns; 3,872 B; 35 allocs | 2,376 ns; 3,584 B; 30 allocs | 1.10x faster; 288 fewer B; five fewer allocations |
| Eight shards | 5,236 ns; 7,616 B; 63 allocs | 4,802 ns; 7,040 B; 54 allocs | 1.09x faster; 576 fewer B; nine fewer allocations |
| 16 shards | 11,369 ns; 15,832 B; 121 allocs | 10,183 ns; 14,680 B; 104 allocs | 1.12x faster; 1,152 fewer B; 17 fewer allocations |
| 32 shards | 22,979.5 ns; 31,832 B; 233 allocs | 20,953.5 ns; 29,400 B; 200 allocs | 1.10x faster; 2,432 fewer B; 33 fewer allocations |
| 64 shards | 45,965.5 ns; 62,424 B; 457 allocs | 41,608 ns; 57,560 B; 392 allocs | 1.10x faster; 4,864 fewer B; 65 fewer allocations |

Without election, the final constructor is 1.01x-1.05x faster across 2-64
shards, removes one allocation, and saves 48-1,792 cumulative heap bytes. The
smaller gain is expected because the former no-election leader already reused
the per-shard owner backing; only the redundant outer table remained.

| Complete route plus targets, ten-run median | Separate owner slices | Leader candidates | Improvement |
| --- | ---: | ---: | ---: |
| Hash, two shards | 75.095 ns | 74.575 ns | 1.007x faster |
| Hash, 16 shards | 73.56 ns | 72.715 ns | 1.012x faster |
| Hash, 64 shards | 68.21 ns | 67.845 ns | 1.005x faster |
| Bucket ranges, two shards | 82.935 ns | 77.80 ns | 1.07x faster |
| Bucket ranges, 16 shards | 89.095 ns | 84.16 ns | 1.06x faster |
| Bucket ranges, 64 shards | 97.16 ns | 90.555 ns | 1.07x faster |

Hash routes remain zero-allocation. Explicit bucket routes retain their
existing 4-byte returned-bucket allocation. An initial test-only route copied
the complete leader into a local before constructing the response and made
hash routes 1.02x-1.03x slower; direct indexed reads removed that regression
before production was changed. Election selection, offline handling,
candidate and target order, aliasing already present in no-election routes,
topology generation, wire, storage, persistence, and public configuration are
unchanged.

<a id="sparse-replication-liveness-exceptions"></a>
#### Sparse Replication Liveness Exceptions

Election-enabled routing snapshots formerly allocated and populated an active
map for every topology node. Healthy nodes are the normal state, so the map
retained the largest representation when there was nothing exceptional to
record. Routing snapshots now lazily retain only offline, timed-out, or
maintenance nodes. A healthy snapshot keeps a nil map; target planning, leader
selection, and digest target membership reject the sparse exceptions directly.

The test-first control retains the exact former active-map scan, target
planning, leader selection, and membership check. The exact matrix covers
untracked healthy, heartbeat-tracked healthy, offline, timed-out, and
maintenance states at 2, 4, 16, and 64 shards. It compares topology, leaders,
candidates, targets, per-node liveness, 4,096 complete routes, and sampled
source/target membership, and passes 20 runs.

```sh
make run CMD='go test . -run=TestReplicationRoutingSnapshotSparseInactiveMatchesOnlineControl -count=20'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingInactiveNodesConstructionAlternating$$/^(UntrackedHealthy|TrackedHealthy|OneOffline|OneTimeout|OneMaintenance)$$/^(2Shards|16Shards|64Shards)$$" -benchtime=20000x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingInactiveNodesConstruction$$/^(UntrackedHealthy|TrackedHealthy|OneOffline|OneTimeout|OneMaintenance)$$/^(2Shards|16Shards|64Shards)$$/^(Baseline|Candidate)$$" -benchmem -benchtime=10000x -count=5 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingInactiveNodeMembershipAlternating$$ -benchtime=1000000x -count=15 -cpu=1'
```

| Healthy snapshot, paired median | Active map | Sparse inactive map | Improvement |
| --- | ---: | ---: | ---: |
| Two shards | 1,104 ns; 1,312 B; 14 allocs | 895.8 ns; 1,056 B; 12 allocs | 1.23x faster; 256 fewer B; two fewer allocations |
| 16 shards | 10,692 ns; 14,680 B; 104 allocs | 9,248 ns; 13,696 B; 100 allocs | 1.16x faster; 984 fewer B; four fewer allocations |
| 64 shards | 40,087 ns; 57,560 B; 392 allocs | 36,527 ns; 54,016 B; 388 allocs | 1.10x faster; 3,544 fewer B; four fewer allocations |

Tracked-healthy medians improve 1.20x/1.11x/1.09x at 2/16/64 shards with
the same exact memory savings. Degraded states do not pay for the sparse form:

| One inactive node, paired median | Two shards | 16 shards | 64 shards | Heap/allocation result |
| --- | ---: | ---: | ---: | --- |
| Explicitly offline | 1.03x faster | 1.03x faster | 1.01x faster | Equal at two shards; 728/3,288 fewer B and two fewer allocations at 16/64 |
| Heartbeat timeout | 1.03x faster | 1.03x faster | 1.01x faster | Equal at two shards; 728/3,288 fewer B and two fewer allocations at 16/64 |
| Maintenance | 1.03x faster | 1.03x faster | 1.02x faster | Equal at two shards; 728/3,288 fewer B and two fewer allocations at 16/64 |

The complete membership check improves from 43.15 to 31.11 ns when healthy,
or 1.39x, because a nil exception map replaces a successful active-map probe.
With one offline node it improves from 38.16 to 30.23 ns, or 1.26x. Both paths
remain zero-allocation. The liveness map is private and immutable after
construction; election timeout rules, lock scope, node validation, leader and
target order, source exclusion, topology generation, wire, storage,
persistence, and public configuration are unchanged.

<a id="adaptive-replication-target-sorting"></a>
#### Adaptive Replication Target Sorting

Each routing snapshot sorts its immutable per-shard target slice by node ID.
The former `sort.Slice` call uses reflection and allocates even for the normal
one-to-three-target replica set. Production now uses `slices.SortFunc` through
16 targets, eliminating that reflection path, and retains `sort.Slice` above
16 targets where the reflective implementation is measurably faster.

The test-first reflective control covers untracked healthy, tracked healthy,
offline, timeout, and maintenance state at 2, 4, 16, and 64 shards, plus full
replica topologies with 2, 4, 8, 16, 32, and 64 nodes. It compares topology,
liveness, leaders, candidates, target order, and 4,096 complete routes per
case and passes 20 runs.

```sh
make run CMD='go test . -run=TestReplicationRoutingSnapshotAdaptiveTargetSortMatchesReflectiveControl -count=20'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingAdaptiveTargetSortConstructionAlternating$$/^(UntrackedHealthy|OneOffline|FullReplica)$$/^(2Shards|16Shards|64Shards|2Nodes|8Nodes|16Nodes|32Nodes|64Nodes)$$" -benchtime=50000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingAdaptiveTargetSortConstruction$$/^(UntrackedHealthy|OneOffline|FullReplica)$$/^(2Shards|16Shards|64Shards|2Nodes|8Nodes|16Nodes|32Nodes|64Nodes)$$/^(Baseline|Candidate)$$" -benchmem -benchtime=20000x -count=1 -cpu=1'
```

| Healthy sharded snapshot, paired median | Reflective sort | Adaptive sort | Improvement |
| --- | ---: | ---: | ---: |
| Two shards | 1,054 ns; 1,056 B; 12 allocs | 932.4 ns; 1,008 B; 10 allocs | 1.13x faster; 48 fewer B; two fewer allocations |
| 16 shards | 8,880 ns; 13,696 B; 100 allocs | 6,856 ns; 10,752 B; 52 allocs | 1.30x faster; 2,944 fewer B; 48 fewer allocations |
| 64 shards | 36,839 ns; 54,016 B; 388 allocs | 29,815 ns; 42,240 B; 196 allocs | 1.24x faster; 11,776 fewer B; 192 fewer allocations |

One-offline 2/16/64-shard snapshots improve 1.08x/1.21x/1.21x, save
48/2,624/11,456 cumulative heap bytes, and remove 2/44/188 allocations.
Full-replica 2/8/16-node snapshots improve 1.06x/1.05x/1.04x while removing
one/three/three allocations and 24/184/184 bytes. Full-replica 32/64-node
snapshots take the original sorter and retain identical heap and allocation
counts; paired CPU is neutral within 0.6%.

An all-generic candidate was rejected: the complete paired 31-target median
rose from 5,657 to 5,823 ns, or 1.03x slower, and the 63-target median rose
from 10,682 to 10,952 ns, or 1.025x slower. The retained cutoff therefore
improves common target sets without imposing the large-set regression. Node
filtering, deterministic order, topology and election state, routing, wire,
storage, persistence, and public configuration are unchanged.

<a id="borrowed-replication-topology-generation"></a>
#### Borrowed Replication Topology Generation

Routing snapshot construction formerly called the private cloned topology
snapshot, duplicating the normalized node slice, shard slice, every replica
slice, and bucket ranges before constructing its own immutable leaders and
targets. `TopologyStore.Set` replaces the complete normalized generation under
the store lock instead of mutating existing backing. The private replication
routing snapshot now borrows that generation and its matching cached
fingerprint under one read lock.

The public `TopologyStore.Get`, `Route`, election route, and topology response
paths retain their prior owned copies. Only the package-private routing
snapshot borrows backing, and its callers do not mutate topology slices. The
test-first cloned-generation control covers all five liveness states at 2, 4,
16, and 64 shards and full-replica topologies at 2, 4, 8, 16, 32, and 64
nodes. It compares complete snapshot state and 4,096 routes per case across 20
runs in
`TestReplicationRoutingSnapshotBorrowedTopologyMatchesClonedGeneration`.
`TestReplicationRoutingSnapshotBorrowedTopologySurvivesGenerationReplacement`
proves the old snapshot remains stable after replacement, and
`TestReplicationRoutingSnapshotBorrowedTopologyConcurrentGenerationReplacement`
stresses four readers across 1,000 alternating `Set` operations; the focused
race control passes five runs.

```sh
make run CMD='go test . -run="TestReplicationRoutingSnapshotBorrowedTopology" -count=20'
make run CMD='go test -race . -run="TestReplicationRoutingSnapshotBorrowedTopology" -count=5'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingBorrowedTopologyConstructionAlternating$$/^(UntrackedHealthy|OneOffline|FullReplica)$$/^(2Shards|16Shards|64Shards|2Nodes|16Nodes|64Nodes)$$" -benchtime=50000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingBorrowedTopologyConstruction$$/^(UntrackedHealthy|OneOffline|FullReplica)$$/^(2Shards|16Shards|64Shards|2Nodes|16Nodes|64Nodes)$$/^(Baseline|Candidate)$$" -benchmem -benchtime=20000x -count=1 -cpu=1'
```

| Healthy sharded snapshot, paired median | Cloned generation | Borrowed generation | Improvement |
| --- | ---: | ---: | ---: |
| Two shards | 790.35 ns; 1,008 B; 10 allocs | 541.65 ns; 672 B; 6 allocs | 1.46x faster; 1.50x lower heap; four fewer allocations |
| 16 shards | 6,439.5 ns; 10,752 B; 52 allocs | 5,294 ns; 7,552 B; 34 allocs | 1.22x faster; 1.42x lower heap; 18 fewer allocations |
| 64 shards | 27,287.5 ns; 42,240 B; 196 allocs | 22,112 ns; 30,208 B; 130 allocs | 1.23x faster; 1.40x lower heap; 66 fewer allocations |

One-offline 2/16/64-shard snapshots improve 1.38x/1.23x/1.22x with the
same 336/3,200/12,032-byte and 4/18/66-allocation savings. Full-replica
2/16/64-node snapshots improve 1.13x/1.27x/1.22x, save
208/1,792/6,784 bytes, and remove one allocation each. No measured path
regressed. The borrowed generation is immutable and remains reachable only as
long as the routing snapshot that previously retained an equivalent cloned
generation. Election state, target and route ownership, deterministic order,
topology replacement, lock scope, wire, storage, persistence, and public
configuration are unchanged.

<a id="grouped-replication-owner-backing"></a>
#### Four-Shard Replication Owner Backing

Sharded routing snapshots formerly called `routeOwners` once per shard, so
every immutable leader candidate slice received a separate backing allocation.
The final constructor counts owners in groups of at most four shards, allocates
one exact backing per group, and carves capacity-capped candidate slices from
it. The capped capacity prevents an append to one candidate slice from
overwriting another shard. Full-replica topology retains the exact former
single-shard construction path, without a counting pass or grouped backing.

The first test-only candidate used one backing for every shard. It removed 63
owner allocations at 64 shards, but Go size-class rounding added 128/256/128
cumulative heap bytes at 16/32/64 shards. That version was rejected. Four-shard
groups land in the same total size classes as the former per-shard allocations
at every measured size while retaining most of the allocation reduction.

The exact per-shard control covers untracked healthy, tracked healthy, offline,
timeout, and maintenance states at 2, 4, 16, and 64 shards, plus full-replica
topologies with 2, 4, 8, 16, 32, and 64 nodes. It compares complete snapshot
state and 4,096 routes and checks every final candidate slice capacity across
20 runs.

```sh
make run CMD='go test . -run=TestReplicationRoutingSnapshotPackedOwnerBackingMatchesPerShardOwners -count=20'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingPackedOwnerBackingConstructionAlternating$$/^(UntrackedHealthy|OneOffline)$$/^(2Shards|8Shards|16Shards|64Shards)$$" -benchtime=100000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingPackedOwnerBackingConstruction$$/^(UntrackedHealthy|OneOffline|FullReplica)$$/^(2Shards|8Shards|16Shards|64Shards|2Nodes|16Nodes|64Nodes)$$/^(Baseline|Candidate)$$" -benchmem -benchtime=20000x -count=1 -cpu=1'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingPackedOwnerBackingConstructionAlternating$$/^FullReplica$$" -benchtime=100000x -count=15 -cpu=1'
```

| Healthy sharded snapshot, paired median | Per-shard owners | Four-shard backing | Improvement |
| --- | ---: | ---: | ---: |
| Two shards | 591.9 ns; 672 B; 6 allocs | 576.15 ns; 672 B; 5 allocs | 1.03x faster; one fewer allocation |
| Eight shards | 2,945 ns; 3,776 B; 18 allocs | 2,767.5 ns; 3,776 B; 12 allocs | 1.06x faster; six fewer allocations |
| 16 shards | 5,183 ns; 7,552 B; 34 allocs | 4,983 ns; 7,552 B; 22 allocs | 1.04x faster; 12 fewer allocations |
| 64 shards | 24,017.5 ns; 30,208 B; 130 allocs | 23,445 ns; 30,208 B; 82 allocs | 1.02x faster; 48 fewer allocations |

One-offline 2/8/16/64-shard construction improves
1.01x/1.04x/1.04x/1.02x, keeps the exact former 928/4,032/7,808/30,464
heap bytes, and removes 1/6/12/48 allocations. Full-replica 2/16/64-node
construction keeps identical 800/4,416/16,264 heap bytes and 10/10/13
allocations. The long paired full-replica CPU control is neutral within 1.1%
at every size except a 1.03x improvement at 16 nodes.

The topology store validates every owner and replaces normalized generations
instead of mutating them. Candidate and target order, election and liveness
behavior, route ownership, topology replacement, lock scope, wire, storage,
persistence, and public configuration are unchanged.

<a id="combined-replication-owner-target-backing"></a>
#### Combined Replication Owner/Target Backing

After owner slices were grouped, each shard still allocated a separate target
backing slice. The owner group already counts the exact maximum number of
owners for up to four shards. Production now uses that same count to allocate
one target backing for the group and carves capacity-capped target slices from
it while each shard is processed. No additional scan, retained index, route
branch, or per-shard work is added.

This is materially different from the rejected standalone target-group
candidate below. That experiment added a separate capacity pass before every
four shards and then allocated owners independently; its added scan caused
several CPU regressions. The final layout shares the capacity pass already
required by owner grouping and uses the retained adaptive target sorter.

The exact separate-target control preserves the immediately preceding
constructor. Untracked healthy, tracked healthy, offline, timeout, and
maintenance states are compared at 2, 4, 16, and 64 shards, plus full-replica
topologies at 2, 4, 8, 16, 32, and 64 nodes. The test compares complete
snapshot state, 4,096 routes, and capacity-capped target slices across 20 runs.

```sh
make run CMD='go test . -run=TestReplicationRoutingSnapshotCombinedBackingMatchesSeparateTargets -count=20'
make run CMD='go test -race . -run=TestReplicationRoutingSnapshotCombinedBackingMatchesSeparateTargets -count=5'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingCombinedBackingConstructionAlternating$$/^(UntrackedHealthy|OneOffline)$$" -benchtime=50000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingCombinedBackingConstruction$$" -benchmem -benchtime=20000x -count=1 -cpu=1'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingCombinedBackingConstructionAlternating$$/^FullReplica$$" -benchtime=50000x -count=10 -cpu=1'
```

| Healthy sharded snapshot, paired median | Separate target backing | Combined group backing | Improvement |
| --- | ---: | ---: | ---: |
| Two shards | 544.35 ns; 672 B; 5 allocs | 490.5 ns; 672 B; 4 allocs | 1.11x faster; one fewer allocation |
| Four shards | 1,388 ns; 1,856 B; 7 allocs | 1,131 ns; 1,856 B; 4 allocs | 1.23x faster; three fewer allocations |
| Eight shards | 2,419 ns; 3,776 B; 12 allocs | 2,245 ns; 3,776 B; 6 allocs | 1.08x faster; six fewer allocations |
| 16 shards | 4,994.5 ns; 7,552 B; 22 allocs | 4,735.5 ns; 7,552 B; 10 allocs | 1.05x faster; 12 fewer allocations |
| 32 shards | 10,412 ns; 15,360 B; 42 allocs | 10,004.5 ns; 15,360 B; 18 allocs | 1.04x faster; 24 fewer allocations |
| 64 shards | 22,196.5 ns; 30,208 B; 82 allocs | 20,770.5 ns; 30,208 B; 34 allocs | 1.07x faster; 48 fewer allocations |

One-offline 2/4/8/16/32/64-shard paired medians improve
1.14x/1.02x/1.06x/1.05x/1.02x/1.03x. Heap remains exactly
928/2,112/4,032/7,808/15,616/30,464 bytes while allocations fall from
7/9/14/24/44/84 to 6/6/8/12/20/36. Full-replica construction takes the
unchanged single-shard branch: every measured heap and allocation count is
identical, and the same-loop CPU control varies within 1.3%.

A refinement moved the multi-shard loop behind one helper call to make the
single-shard machine-code body smaller. The call erased the smaller sharded
gain: offline 32/64-shard paired medians changed from 11,881 to 12,006 ns and
25,061 to 25,226 ns, both 1.01x slower. It was reverted before commit; the
faster inline loop remains.

The final target slices preserve deterministic order and have capacity equal
to length, so an accidental append cannot overwrite the next shard. Topology
generation ownership, election and liveness behavior, routing, lock scope,
wire, storage, persistence, and public configuration are unchanged.

<a id="grouped-replication-target-backing-rollback"></a>
#### Grouped Replication Target Backing Rollback

Routing snapshots retain one immutable target slice per shard. A candidate
reserved the sum of the existing per-shard capacities once and carved capped
target slices from that common backing. The exact control compares headers,
liveness, leaders, candidates, targets, and 4,096 complete routes for
untracked healthy, tracked healthy, offline, timeout, and maintenance states
at 2, 4, 16, and 64 shards. It also proves every returned target slice has its
capacity capped at its length, so an accidental append cannot overwrite the
next shard. The matrix passes 20 runs.

```sh
make run CMD='go test . -run=TestReplicationRoutingSnapshotPackedTargetsCandidateMatchesPerShardBacking -count=20'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingPackedTargetsConstruction$$/^(UntrackedHealthy|OneOffline)$$/^(2Shards|4Shards|8Shards|16Shards|32Shards|64Shards)$$/^(Baseline|Candidate)$$" -benchmem -benchtime=10000x -count=5 -cpu=1'
make run CMD='go test . -run=NONE -bench="^BenchmarkReplicationRoutingPackedTargetsConstructionAlternating$$/^(UntrackedHealthy|OneOffline)$$/^(2Shards|8Shards|16Shards|32Shards|64Shards)$$" -benchtime=20000x -count=10 -cpu=1'
```

The first one-backing version reduced healthy 64-shard construction from 388
to 325 allocations and improved its isolated median from 35,045 to 32,685 ns,
or 1.07x. Go size-class rounding nevertheless raised cumulative heap from
6,784 to 6,912 B at eight shards and from 13,696 to 13,952 B at 16 shards.
That 128/256-byte regression disqualified the layout.

A refined version grouped at most four shards per backing allocation. It
matched baseline heap exactly at every measured size and still reduced healthy
64-shard construction from 388 to 340 allocations. The complete same-loop CPU
control was mixed:

| Four-shard backing groups, paired median | Per-shard backing | Grouped backing | Result |
| --- | ---: | ---: | ---: |
| Healthy, two shards | 882.1 ns | 902.35 ns | 1.02x slower |
| Healthy, eight shards | 4,728.5 ns | 4,333 ns | 1.09x faster |
| Healthy, 16 shards | 9,117 ns | 9,362 ns | 1.03x slower |
| Healthy, 64 shards | 36,192 ns | 35,809 ns | 1.01x faster |
| One offline, eight shards | 4,115.5 ns | 4,854.5 ns | 1.18x slower |
| One offline, 32 shards | 18,956 ns | 19,434 ns | 1.03x slower |
| One offline, 64 shards | 38,124 ns | 38,595.5 ns | 1.01x slower |

Changing the sorter to use a zero-based shard subslice did not remove the
regressions. Allocation count alone therefore did not justify extra capacity
planning and a separate grouped-backing pass. That candidate remains only as
test and benchmark evidence. The later
[combined owner/target layout](#combined-replication-owner-target-backing)
reuses an already-required owner count and therefore removes the allocation
without retaining this candidate's CPU regressions.

<a id="adaptive-replication-bucket-search"></a>
#### Adaptive Replication Bucket Search

Explicit-bucket routing scanned every normalized bucket range until it found
the range containing the key hash. Normalization already sorts ranges,
requires contiguous complete bucket coverage, and sorts unique shard IDs. The
private replication snapshot now keeps the linear scan for at most eight
ranges and uses binary search by inclusive range end above that threshold. It
then performs the existing binary shard-ID lookup. No index or retained memory
is added.

The test-first control keeps the former linear implementation. For 2, 4, 8,
16, 32, 64, 128, and 256 ranges, the exact test reverses range-to-shard ID
order and compares every valid bucket plus three defensive out-of-range
buckets over 20 runs. The alternating same-binary benchmark measures complete
key hash, range selection, route construction, and target retrieval rather
than the search helper alone.

```sh
make run CMD='go test . -run="TestReplicationRoutingBucketRangeSearchMatchesLinear|TestReplicationRoutingSnapshotShard" -count=20'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingBucketRangeSearchAlternating$$ -benchtime=200000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRoutingShardSlices$$ -benchmem -benchtime=1000000x -count=5 -cpu=1'
```

| Ten-run complete-route median | Linear range scan | Adaptive binary search | Improvement |
| --- | ---: | ---: | ---: |
| 16 ranges | 91.115 ns | 77.825 ns | 1.17x faster |
| 32 ranges | 95.24 ns | 85.435 ns | 1.11x faster |
| 64 ranges | 111.0 ns | 87.37 ns | 1.27x faster |
| 128 ranges | 138.45 ns | 93.22 ns | 1.49x faster |
| 256 ranges | 180.05 ns | 98.92 ns | 1.82x faster |

Two, four, and eight ranges execute the former linear branch unchanged and
are not assigned an artificial speedup. Hash routing is untouched. Explicit
bucket routes retain exactly 4 heap bytes and one allocation for the public
bucket pointer in both versions; range lookup itself remains allocation-free.
Modulo fallback for empty ranges or an out-of-range defensive call, missing
shard rejection, topology validation, route contents, target order, election
state, wire, storage, and persistence are unchanged.

<a id="topology-store-large-bucket-dispatch-rollback"></a>
#### Topology-Store Large Bucket Dispatch Rollback

The private replication snapshot benefits from adaptive bucket search because
its complete route method can choose the search within an already specialized
hot path. Applying the same idea to `TopologyStore.Route` required an extra
range-count dispatch before selecting the existing normalized route function.
The candidate kept the exact old function for at most 32 ranges and isolated
the binary lookup in a separate large-topology function, so the control did
not give the candidate an artificial function-boundary advantage.

The test-first fixture creates 2, 4, 8, 16, 32, 64, 128, and 256 contiguous
bucket ranges, reverses range-to-shard assignment for the exact comparison,
and checks 4,096 keys over 20 runs. The alternating same-binary benchmark
measures the store lock, key hash, range selection, route construction, shard
clone, and owner result together.

```sh
make run CMD='go test . -run=TestNormalizedTopologyBucketIndexCandidateMatchesRoute -count=20'
make run CMD='go test . -run=NONE -bench=^BenchmarkNormalizedTopologyBucketIndexAlternating$$ -benchtime=300000x -count=10 -cpu=1'
```

| Ten-run complete-route median | Existing linear route | Indexed candidate | Result |
| --- | ---: | ---: | ---: |
| 2 ranges | 164.05 ns | 166.45 ns | 1.015x slower |
| 64 ranges | 210.40 ns | 181.25 ns | 1.16x faster |
| 128 ranges | 246.05 ns | 190.85 ns | 1.29x faster |
| 256 ranges | 319.75 ns | 194.60 ns | 1.64x faster |

The common two-range regression disqualified the candidate despite the large
topology wins. All production changes were removed, so topology-store routing
retains its former CPU, memory, allocation, wire, and behavior. The exact test
and benchmark control remain as a reproducible crossover record. The adaptive
private replication search remains because its independently measured design
keeps two through eight ranges on the former branch without adding this store
dispatch.

<a id="direct-replication-route-membership"></a>
#### Direct Replication Route Membership

Incoming digest and Merkle scans validate that the requested target belongs to
each routed shard. The former boolean helper called `replicationTargets` with
the remote source ID, which allocated a target slice, filtered every owner,
sorted the result, and then scanned it only to answer one membership question.
The helper now checks source exclusion and online state once, scans the existing
owner IDs directly, and validates the matching node against the snapshot index.
No target representation or ordering is needed for this private boolean result.

The test-first control covers 2, 3, 16, and 64 owners with explicit and
snapshot-fallback owner slices, local and remote sources, every registered
target, missing and empty targets, and nil/degraded online maps. It compares the
direct result with the former materializing implementation exactly. Existing
digest tests cover full handler routing and repair behavior.

```sh
make run CMD='go test . -run="TestReplicationRouteTargetsNodeMatchesMaterializedControl|TestReplicationDigest" -count=10'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRouteTargetsNodeAlternating$$ -benchtime=100000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRouteTargetsNode$$ -benchmem -benchtime=100000x -count=10 -cpu=1'
```

| Ten-run median | Materialize, filter, sort | Direct membership | Improvement |
| --- | ---: | ---: | ---: |
| Two owners | 182.95 ns; 232 B; 2 allocs | 39.07 ns; 0 B; 0 allocs | 4.68x faster; all heap and allocations eliminated |
| Three owners | 330.6 ns; 504 B; 4 allocs | 42.775 ns; 0 B; 0 allocs | 7.73x faster; all heap and allocations eliminated |
| 16 owners | 1,167 ns; 1,976 B; 4 allocs | 82.63 ns; 0 B; 0 allocs | 14.12x faster; all heap and allocations eliminated |
| 64 owners | 4,184.5 ns; 6,968 B; 4 allocs | 215.45 ns; 0 B; 0 allocs | 19.42x faster; all heap and allocations eliminated |

CPU values are alternating same-binary medians. Heap and allocation values are
from the isolated old/new sub-benchmarks; the direct path is zero-allocation in
every run. Source exclusion, online semantics, registered-node validation,
owner fallback, shard routing, topology ownership, configuration, wire,
storage, and persistence are unchanged.

A follow-up removes the remaining node-index probe after an owner ID matches.
Every owner in this private snapshot comes from topology normalization, which
already rejects empty, duplicate, and unregistered primary or replica IDs. The
same test matrix compares the normalized-owner result with both the indexed
direct control and the original materializing control.

```sh
make run CMD='go test . -run=TestReplicationRouteTargetsNodeMatchesMaterializedControl -count=20'
make run CMD='go test . -run=NONE -bench=^BenchmarkReplicationRouteTargetsNodeValidationAlternating$$ -benchtime=1000000x -count=10 -cpu=1'
```

| Ten-run alternating median | Direct plus node-index probe | Normalized owner match | Improvement |
| --- | ---: | ---: | ---: |
| Two owners | 36.76 ns | 30.315 ns | 1.21x faster |
| Three owners | 37.475 ns | 29.865 ns | 1.25x faster |
| 16 owners | 74.07 ns | 64.74 ns | 1.14x faster |
| 64 owners | 212.3 ns | 200.75 ns | 1.06x faster |

Both direct variants remain zero-allocation. The removed lookup did not provide
additional validation for any constructible routing snapshot; public arbitrary
topologies still pass through topology normalization. Source and online checks,
explicit and fallback owner selection, shard routing, wire, storage,
configuration, and persistence are unchanged.

<a id="direct-single-target-digest-inventory"></a>
#### Direct Single-Target Digest Inventory

The compatible anti-entropy path computes a value-digest inventory for every
target. `syncAllPaged` already proves whether all locally led shards have one
common target for capability-cache lookup. It now reuses that proof to select a
dedicated inventory scan for exactly one target, avoiding a
`map[TopologyNode]*inventory`, one map lookup per scanned key, output-map
materialization, and sorting. Zero or multiple targets call the original map
function unchanged.

The test-first fixture builds one- and three-replica routing snapshots, checks
target order, entry counts, finalized roots, and proves the direct one-target
inventory is deeply equal to the map result. Digest sync and legacy-fallback
tests pass ten times.

```sh
make run CMD='go test . -run="TestReplicationDigestInventoriesPreserveSingleAndMultipleTargets|TestHTTPReplicatorSyncAllDigest" -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationDigestInventoryPlanning10K -benchmem -benchtime=10x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationDigestInventorySingleTargetAlternating -benchtime=10x -count=10 -cpu=1'
```

| 10k-key single-target planning, ten-run median | Target map | Direct inventory | Improvement |
| --- | ---: | ---: | ---: |
| Alternating same-binary CPU | 4,479,183 ns | 3,907,212 ns | 1.15x faster |
| Cumulative heap | 19,955 B | 19,643 B | 312 B lower |
| Allocations | 58 | 56 | two fewer |

An initial shared-loop prototype put a `singleInventory != nil` branch in the
callback for every key. Its immediate four-target control was 1.8% slower, so
that design was removed. The final caller-level split keeps the multi-target
implementation unchanged and duplicates only the bounded scan loop. Digest
bytes, roots, target ordering, lock pages, requests, wire bytes, storage,
configuration, and compatibility behavior are unchanged.

### Hierarchical Merkle Anti-Entropy

The unfiltered single-shard path now compares a 1,024-leaf incremental Merkle
root before requesting key digests. Equal replicas need one fixed-size request;
a sparse mismatch fetches only differing leaves. Prefix and multi-shard syncs
retain the compatible sorted-digest implementation.

```sh
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleIncremental -benchtime=1x -count=5 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleIndexBuild -benchtime=1x -count=5 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleEmptyIndexAllocation -benchtime=100000x -count=10 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleEmptyIndexActivation -benchtime=10000x -count=10 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleEmptySnapshot -benchtime=10000x -count=10 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleIndexInitializationPaired -benchtime=5000x -count=5 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleNonemptySizeCheckPaired -benchtime=100x -count=7 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleTableOperations -benchtime=200ms -count=10 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleWriteTracking -benchtime=100000x -count=5 -benchmem'
make bench-merkle-maintenance BENCHTIME=1x COUNT=7
```

Both replicas contain 10,000 deterministic incompressible 1 KiB values. Sparse
repair changes 100 target values. Values are five-run medians.

| State | Path | Time/op | Wire B/op | Heap B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| Equal | Sorted digest | 18,271,905 ns | 215 | 560,720 | 20,538 |
| Equal | Merkle root | 992,977 ns | 228 | 233,744 | 451 |
| 1% changed | Sorted digest | 55,401,391 ns | 240,086 | 9,983,288 | 98,797 |
| 1% changed | Differing Merkle leaves | 25,443,399 ns | 132,820 | 3,149,024 | 47,664 |

Equal-state CPU improves 18.40x, heap 2.40x, and allocations 45.54x; its fixed
wire request is 13 bytes larger. Sparse repair improves CPU 2.18x, wire 1.81x,
heap 3.17x, and allocations 2.07x. Initial index construction takes 5.920 ms,
730,496 heap bytes, and 10,059 allocations; the active index retains 29.60
B/key. Subsequent tracked writes rise from 272.1 to 511.5 ns (1.88x slower)
with the same 16 B and one allocation per write. The index therefore remains
dormant until the first eligible anti-entropy sync.

Active indexes now defer value encoding until a root is requested. Up to 32
unique key hashes stay in an inline slice; a lookup microbenchmark measured
8.13 ns at 32 entries versus 16.16 ns for a map, with the map reaching parity
at 64. The pending set therefore promotes after 32 entries. Repeated writes
coalesce by key, and more than 1,024 unique keys invalidate the old index so the
next root performs one bounded linear rebuild. The cap prevents dirty-key
memory from growing with an unbounded write interval.

The following seven-run medians use 10,000 keys. The cycle changes 100,000
values across the full keyspace, so it exercises the rebuild fallback. Heap is
cumulative allocation, not peak RSS.

| Merkle maintenance | Immediate update baseline | Deferred/coalesced | Improvement/tradeoff |
| --- | ---: | ---: | --- |
| Active write | 486.6 ns; 0 heap B/op | 238.3 ns; 1 heap B/op | 2.04x faster; 6.1% over inactive writes |
| 100k writes plus root | 45,522,676 ns; 323,840 heap B; 19,901 allocs | 25,806,681 ns; 1,006,632 heap B; 19,983 allocs | 1.76x faster; 3.11x heap; 82 more allocations |
| Root after 10k dirty keys | 537,914 ns; 323,840 heap B | 3,226,957 ns; 897,560 heap B | 6.00x slower sync; 2.77x heap |

This deliberately shifts work from every mutation to anti-entropy. Sparse
churn flushes only final unique values; broad churn rebuilds once. A tested
string-keyed map variant slowed the 16,384-key candidate cycle from 29.294 ms
to 33.070 ms without reducing measured heap, so the hash-keyed map was retained.
Pending updates survive memory compaction, and snapshots flush them while
holding the same trie lock used by mutations.

<a id="lazy-empty-merkle-table-backing"></a>
#### Lazy Empty Merkle Table Backing

An activated empty Merkle index needs its fixed 1,024 leaves for root
compatibility but has no hash-to-digest records. It previously allocated the
three 1,024-slot hash, digest, and occupancy arrays immediately. A test was
added first requiring an empty snapshot and subsequent memory compaction to
retain no table capacity; it failed at 33,792 retained bytes instead of the
16,384-byte leaf array.

The index now leaves the table zero-valued until the first `set`. That method
already initialized a zero-valued table, so the first-key path reuses existing
code and every nonempty probe, resize, lookup, and delete remains byte-for-byte
the same. Empty memory compaction preserves the zero-capacity table, while the
one-key correctness control allocates the same 1,024 slots and indexes the key.

| Empty Merkle index | Eager table | Lazy table | Improvement |
| --- | ---: | ---: | ---: |
| Index allocation median | 6,497 ns | 3,122 ns | 2.08x faster |
| Cumulative heap | 35,840 B | 18,432 B | 1.94x lower |
| Allocations | 4 | 1 | 4x fewer allocations |
| Retained backing | 33,792 B | 16,384 B | 2.06x lower |
| Complete empty-trie activation | 156.591 us | 156.788 us | CPU-neutral within 0.13% |
| Complete activation heap/allocations | 54,320 B / 37 | 36,912 B / 34 | 1.47x lower heap; 3 fewer allocations |

Heap-resident eager and lazy 10,000-key index controls both use 558,083
cumulative bytes, 16 allocations, and 29.49 retained B/key. A 5,000-iteration
alternating paired run measured 333.530 versus 337.890 us, within 1.3%; the
bounded GC-disabled control measured lazy initialization 1.02x faster. The
earlier end-to-end process drift therefore was not attributed as a regression.
There is no wire, persistence, retained nonempty memory, or lookup change.

<a id="stateless-empty-merkle-root"></a>
#### Stateless Empty Merkle Root

The lazy constructor still retained the inline 1,024-leaf array whenever an
empty trie requested its first Merkle root. For an absent or invalid index,
native trie size zero proves that every leaf and the entry count are zero, so
the root is the fixed Merkle seed. A test was changed first to require that
exact root, a nil retained index before and after memory compaction, and the
ordinary nonempty table after inserting the first key; it failed with 16,384
retained bytes.

The snapshot path now returns the fixed root before constructing a scan cursor
or index. This also releases an invalid empty index. Existing valid indexes do
not take the new branch, and a nonempty first activation performs one native
size read before the unchanged rebuild.

| Empty Merkle snapshot path | Lazy index | Stateless root | Improvement |
| --- | ---: | ---: | ---: |
| Isolated activation median | 17,186 ns | 878.9 ns | 19.56x faster |
| Cumulative heap | 33,424 B | 0 B | Allocation eliminated |
| Allocations | 4 | 0 | Allocation eliminated |
| Retained Merkle backing | 16,384 B | 0 B | Retention eliminated |
| Complete create/snapshot/destroy | 177.917 us | 127.250 us | 1.40x faster |
| Complete lifecycle heap | 36,912 B | 3,488 B | 10.58x lower heap |
| Complete lifecycle allocations | 34 | 30 | 1.13x fewer allocations |

An alternating seven-run 10,000-key rebuild control measured 3.863 ms without
the native size check and 3.845 ms with it, neutral within 0.5%. Heap and
allocations were unchanged. Empty and nonempty roots, local-partition root
combination, snapshot invalidation, memory compaction, wire bytes, and storage
formats retain their existing semantics.

<a id="merkle-table-occupancy-sentinel-rollback"></a>
#### Merkle Table Occupancy Sentinel Rollback

The active Merkle index uses parallel hash, digest, and one-byte occupancy
arrays. A test was added first requiring 16 bytes of backing per table slot;
it failed against the retained 17-byte layout. The candidate used hash zero as
the empty-slot sentinel and kept a fixed side slot for the valid zero hash.
That removed one allocation per table size, reduced a 10,000-key direct build
from 522,242 to 491,522 cumulative heap bytes and from 12 to 8 allocations,
and lowered table backing from 27.85 to 26.21 B/key.

Same-command ten-run medians exposed the cache-locality cost:

| Merkle table operation | Byte occupancy | Zero sentinel | Candidate result |
| --- | ---: | ---: | ---: |
| Build 10k | 261.1 us | 263.4 us | 1.01x slower |
| Existing-key hit | 2.555 ns | 2.812 ns | 1.10x slower |
| Missing-key lookup | 7.172 ns | 10.035 ns | 1.40x slower |
| Existing-key update | 6.209 ns | 5.790 ns | 1.07x faster |
| Delete plus reinsert | 31.39 ns | 26.93 ns | 1.17x faster |

The complete index build also moved from 4.220 to 4.565 ms, or 1.08x slower,
while retained index memory improved only from 29.60 to 27.96 B/key and
cumulative heap from 574,867 to 543,126 bytes. Missing lookups benefit from
scanning the dense occupancy bytes before touching the wider hash array, so
the occupancy allocation is useful metadata rather than removable waste. The
runtime candidate and its failing compact-layout contract were removed; the
focused operation benchmark remains to prevent repeating the experiment.

### Indexed Expiration Heap

The expiration map now stores a compact `uint32` heap position instead of a
second timestamp. Each live TTL key owns exactly one heap node. Updating a
deadline repairs that node in place; persistence and deletion remove it with a
swap-and-sift operation. This replaces stale-node accumulation and periodic
whole-heap rebuilds.

```sh
make run CMD='go test . -run=none -bench=BenchmarkBigWins/ExpirationDeadlineUpdate -benchtime=100000x -count=5 -benchmem'
```

| 100k updates, one live TTL key | Median time/update | Heap B/op | Allocs/op | Final heap nodes |
| --- | ---: | ---: | ---: | ---: |
| Stale entries + periodic rebuild | 250.0 ns | 91 | 0 (rounded) | 19 |
| Indexed in-place update | 194.8 ns | 0 | 0 | 1 |

The indexed path is 1.28x faster, eliminates cumulative heap allocation in the
fixture, and leaves one node instead of 19. More importantly, the heap is now
strictly bounded by the number of live TTL keys, so repeated `EXPIREAT` calls
cannot create a temporary 64-entry stale backlog for one key. Multi-key tests
verify every map index after both upward/downward deadline changes and arbitrary
removals.

### Binary Grouped Replication Outbox

LevelDB outbox records now default to a compact binary envelope while reading
both binary and legacy JSON. Concurrent durable puts use a 1 ms group-commit
window by default; every caller waits for the shared sync. The legacy whole-file
JSON backend and JSON LevelDB codec remain configurable.

```sh
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationOutboxEncoding -benchtime=100000x -count=5 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationOutboxReplay10k -benchtime=1x -count=5 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationOutboxRestore100k -benchtime=1x -count=5 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationOutboxDurableEnqueue -benchtime=5x -count=5 -benchmem'
```

| Operation | JSON/sync-each | Binary/grouped default | Improvement |
| --- | ---: | ---: | ---: |
| Encode 4 KiB job | 8,949 ns; 6,935 heap B; 10 allocs; 5,948 stored B | 4,123 ns; 5,491 heap B; 4 allocs; 4,412 stored B | 2.17x CPU, 1.26x heap, 2.50x allocs, 1.35x storage |
| Replay 10k 1 KiB jobs | 217.479 ms; 54,882,672 heap B; 375,842 allocs; 1,858 B/job | 87.330 ms; 50,839,168 heap B; 279,883 allocs; 1,344 B/job | 2.49x CPU, 1.08x heap, 1.34x allocs, 1.38x storage |
| Restore 100k queued jobs | 466.884 ms; 100,000 resident jobs; 415,115,256 heap B; 2,767,664 allocs | 5.019 ms; 1,024 resident jobs; 3,518,536 heap B; 29,487 allocs | 93.03x CPU, 97.66x resident jobs, 118.0x heap, 93.86x allocs |
| Enqueue, 32 writers | 50.289 ms; 246,116 heap B; 968 allocs; 32 syncs | 3.542 ms; 366,531 heap B; 672 allocs; 1 sync | 14.20x CPU, 1.44x fewer allocs, 32x fewer syncs |

Grouped enqueue uses 1.49x cumulative heap for waiter/job coordination. A
measured 200 us window produced about two commits and a 4.79 ms median; 1 ms
consistently produced one commit at about 3.08 ms in the window-selection
fixture, so 1 ms is the default. `REPLICATION_OUTBOX_BATCH_WINDOW=0` restores
sync-each behavior, and `REPLICATION_OUTBOX_CODEC=json` restores JSON records.

LevelDB restart now reads ordered job-ID pages and refills the in-memory channel
when it reaches half capacity. `REPLICATION_QUEUE_SIZE` remains a hard resident
bound even when the durable backlog is larger; concurrent durable enqueues stay
behind the restore cursor and preserve FIFO order. Queue status reports
`durable_backlog=true` until every disk page has entered the bounded channel.
The legacy whole-file JSON backend also uses a bounded channel, but opening its
JSON snapshot still materializes the complete file for compatibility.

### Journal-Backed Replication Outbox

With a binary command journal and LevelDB outbox configured together, each
journal record owns the exact internal replication envelope and LevelDB stores
only its job ID and journal sequence. The journal is synced before success; the
reference does not need its own fsync because startup scans journal records newer
than the durable completion watermark and recreates missing references. Journal
segments containing unacknowledged envelopes are pinned. Existing full JSON and
binary jobs remain readable.

```sh
make run CMD='go test . -run=none -bench=BenchmarkJournalBackedReplicationOutbox -benchtime=100x -count=5 -benchmem'
```

Five-run medians on the benchmark host:

| Durable 4 KiB mutation path | Time/op | Seconds/10k | Heap B/op | Allocs/op | Encoded B/op | Disk B/op | Syncs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Separate journal + full LevelDB job | 13.685 ms | 136.854 | 20,993 | 18 | 8,550 | 8,591 | 2 |
| Journal envelope + LevelDB reference | 0.785 ms | 7.845 | 26,094 | 21 | 8,559 | 8,598 | 1 |

The measured latency improves 17.44x and sync work falls 2x. Cumulative heap is
1.24x higher and allocations are 1.17x higher because the exact envelope is
encoded as part of the journal transaction. Total storage is essentially flat:
the payload moves from the LSM job value into the WAL rather than being removed.
The operational gain is one durability boundary, compact outbox indexing, and
crash repair without sacrificing exact payload semantics. Fsync latency varies
substantially by filesystem, so use the included benchmark on deployment-class
storage before sizing write throughput.

## Latest Optimization Spot Check

After adding exact command fast paths for set, priority queue, Bloom filter,
Cuckoo filter, and Count-Min Sketch single-string workloads, this local
100,000-iteration spot check measured the optimized rows below:

```
make bench-hatrie-command-features HATRIE_COMMAND_BENCH='^BenchmarkCommandFeature/(SetAddHas|PriorityQueuePushPop|BloomAdd|BloomHas|CuckooDeleteAdd|CuckooHas|CountMinSketchIncrement|CountMinSketchEstimate)$' BENCHTIME=100000x
```

| Feature | Benchmark row | Time/op | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Set add+has | `BenchmarkCommandFeature/SetAddHas` | 666.1-859.6 ns | 0 B | 0 |
| Priority queue push+pop | `BenchmarkCommandFeature/PriorityQueuePushPop` | 702.4 ns | 56 B | 3 |
| Bloom filter add | `BenchmarkCommandFeature/BloomAdd` | 205.1 ns | 0 B | 0 |
| Bloom filter lookup | `BenchmarkCommandFeature/BloomHas` | 271.4 ns | 0 B | 0 |
| Cuckoo filter delete+add | `BenchmarkCommandFeature/CuckooDeleteAdd` | 591.5 ns | 0 B | 0 |
| Cuckoo filter lookup | `BenchmarkCommandFeature/CuckooHas` | 278.1 ns | 0 B | 0 |
| Count-Min Sketch increment | `BenchmarkCommandFeature/CountMinSketchIncrement` | 303.8 ns | 5 B | 0 |
| Count-Min Sketch estimate | `BenchmarkCommandFeature/CountMinSketchEstimate` | 267.6 ns | 0 B | 0 |

The set row uses a slice-first representation for one- and two-entry sets,
then promotes to a map. `BenchmarkSetRepresentationSmall*` measured slice
lookup faster through two entries and map lookup faster from three entries
upward; the command row became allocation-free but did not show a clear CPU
win in repeated local runs.

### Collection Allocation Follow-up

The following medians use five one-million-iteration runs on the same
AMD Ryzen 9 5950X host. Small-set reads now sort their two inline values
directly. A Typed priority-queue string slot avoids boxing a string into
`interface{}` on every push. Direct radix prefix JSON writes plain-string scan
results without allocating an intermediate `[]RadixTreeItem`.

```sh
make run CMD='go test -run=NONE -bench=BenchmarkSetRepresentationSmallValues -benchmem -benchtime=1000000x -count=5'
make run CMD='go test -run=NONE -bench=BenchmarkCommandFeature/PriorityQueuePushPop -benchmem -benchtime=1000000x -count=5'
make run CMD='go test -run=NONE -bench=BenchmarkCommandFeature/RadixPrefix -benchmem -benchtime=1000000x -count=5'
```

| Feature | Before | After | CPU improvement | Heap improvement | Allocation improvement |
| --- | ---: | ---: | ---: | ---: | ---: |
| Two-value small-set read (`BenchmarkSetRepresentationSmallValues`) | 155.5 ns, 48 B, 3 allocs | 54.46 ns, 32 B, 1 alloc | 2.86x | 1.50x | 3.00x |
| Priority queue push+pop | 875.9 ns, 56 B, 3 allocs | 769.1 ns, 40 B, 2 allocs | 1.14x | 1.40x | 1.50x |
| Radix prefix scan | 3,979 ns, 1,468 B, 20 allocs | 1,972 ns, 1,024 B, 1 alloc | 2.02x | 1.43x | 20.00x |

The radix command allocation count falls from 20 to 1; the remaining
allocation is the returned JSON string. Non-string or JSON-escaped radix values
use the generic clone-and-encode path to preserve behavior.

<a id="idempotent-plain-string-radix-updates"></a>
### Idempotent Plain-String Radix Updates

Exact plain-string `PUTRT` previously passed the request value through an
escaping interface and assigned a freshly boxed string even when the same key
already contained the same immutable string. The command now uses an internal
typed radix insertion path. At an exact populated node it compares string
values before assignment, avoiding the box for an idempotent update. New keys,
different strings, node splits, empty keys, and replacement of structured
values preserve the prior tree shape, item count, and return value. Generic
public writes still use `Put`, so constant-interface reuse and structured-value
cloning are unchanged.

Tests were added before the typed method and cover nil receivers, first insert,
duplicate, replacement, sibling and parent splits, empty keys, structured to
string replacement, item counts, reads, and the zero-allocation duplicate
contract. The alternating benchmark runs generic and typed operations in both
orders in the same binary. Dynamic construction uses precomputed request-like
keys and values; the lookup control compares identically shaped trees produced
by each method.

```sh
make run CMD='go test . -run=TestRadixTreePutPlainString -count=1 -v'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/RadixPut -benchtime=1000000x -count=9 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkRadixTreePlainStringPutAlternating -benchtime=20x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRadixTreePlainStringBuildAlternating -benchtime=20x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRadixTreePlainStringPutLookupControl -benchtime=2000000x -count=9 -cpu=1 -benchmem'
```

| Plain-string radix operation, median | Generic control | Typed command path | Result |
| --- | ---: | ---: | ---: |
| Complete duplicate `PUTRT` | 260.6 ns; 16 B; 1 alloc | 207.6 ns; 0 B; 0 allocs | 1.26x faster; allocation eliminated |
| Alternating focused duplicate | 43.60 ns; 16 B; 1 alloc | 16.63 ns; 0 B; 0 allocs | 2.62x faster; allocation eliminated |
| Alternating true replacement | 43.32 ns; 16 B; 1 alloc | 41.94 ns; 16 B; 1 alloc | 1.03x faster; memory unchanged |
| Dynamic 128-entry build | 20,141 ns; 31,168 B; 203 allocs | 20,091 ns; 31,168 B; 203 allocs | Neutral within 0.3%; memory unchanged |
| Lookup after dynamic build | 41.60 ns; 0 B; 0 allocs | 41.28 ns; 0 B; 0 allocs | Neutral within 0.8%; memory unchanged |

The exact command continues to record the write, update telemetry, cache the
same radix handle, and return the same added/not-added response for duplicates.
Node layout, retained memory, prefix ordering, snapshots, journals, persistent
records, replication, and wire formats are unchanged.

<a id="order-independent-radix-bulk-insertion"></a>
### Order-Independent Radix Bulk Insertion

`radixTreeData.PutEntries` previously copied every input-map key into a
temporary slice and sorted it before inserting the entries. That ordering was
redundant: each compressed radix node keeps children sorted by their first
byte, and node splitting produces the same canonical path-compressed tree for
every insertion order. The implementation now ranges over the input map
directly. Individual values still pass through `Put`, preserving value cloning,
replacement, item-count, nil-receiver, and error behavior.

The correctness test was added before the production change. It builds
adversarial parent, child, sibling, empty-key, and shared-prefix entries in four
different orders, then compares the exact private tree shape, item count,
snapshot, and info. It also proves order-independent replacement and bulk-map
insertion. The test passed 100 repeated runs before and after the change.

```sh
make run CMD='go test . -run=TestRadixTreeInsertionOrderProducesCanonicalShape -count=100'
make run CMD='go test . -run=NONE -bench=BenchmarkRadixTreePutEntriesOrderAlternating -benchtime=20x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRadixTreePutEntriesReplacementAlternating -benchtime=20x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRadixTreePutEntriesOrderAllocations/.*64 -benchtime=2000x -count=5 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkRadixTreePutEntriesOrderAllocations/.*4096 -benchtime=50x -count=5 -cpu=1 -benchmem'
```

| Bulk operation, median | Sorted control | Direct production path | Result |
| --- | ---: | ---: | ---: |
| Fresh 64-entry build | 12,681 ns; 15,296 B; 39 allocs | 10,566 ns; 14,144 B; 38 allocs | 1.20x faster; 1,152 B and one allocation removed |
| Fresh 4,096-entry build | 1,350,624 ns; 1,026,369 B; 2,280 allocs | 1,101,969 ns; 960,832 B; 2,279 allocs | 1.23x faster; 65,536 B and one allocation removed |
| Replace 64 existing entries | 6,840 ns; 1,152 B; 1 alloc | 3,058 ns; 0 B; 0 allocs | 2.24x faster; temporary heap eliminated |
| Replace 4,096 existing entries | 794,342 ns; 65,536 B; 1 alloc | 441,584 ns; 0 B; 0 allocs | 1.80x faster; temporary heap eliminated |

The CPU columns are medians from same-binary alternating controls; allocation
columns come from the dedicated allocation fixture. Final child order, prefix
scan order, and serialized snapshots remain deterministic even though the
temporary insertion order is not. No format, API, or retained-memory tradeoff
was introduced.

<a id="borrowed-command-pair-fields"></a>
### Borrowed Command Pair Fields

Pair-only `PUTMAP` and `PUTRT` requests previously validated every field, then
copied the full `Pairs` map into a temporary map before passing it to storage.
Both storage paths synchronously clone every retained value and never mutate
the field map, so that copy provided no ownership boundary. The command helpers
now return the already validated request map for pair-only calls. Requests that
also specify `Subkey` still receive an owned merge map so the subkey can
override a same-named pair without mutating caller input.

Tests were added before the production change. The zero-allocation extraction
contract failed at two allocations per call on the old implementation, then
passed 100 repeated runs after the change. A separate mixed-fields test mutates
the returned merge map and proves the request pairs remain unchanged. Existing
command tests continue to cover nested-value cloning, invalid-value rejection,
replacement of other cache types, and radix added counts.

```sh
make run CMD='go test . -run=TestCommand.*Fields -count=100'
make run CMD='go test . -run=TestExecuteCommand -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandPairFieldsAlternating -benchtime=20x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandPairBulkReplacement/.*64 -benchtime=2000x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandPairBulkReplacement/.*4096 -benchtime=50x -count=7 -cpu=1 -benchmem'
```

| Pair-only operation, median | Copied fields | Borrowed fields | Result |
| --- | ---: | ---: | ---: |
| Map helper, 64 fields | 7,439 ns/extraction | 1,082 ns/extraction | 6.88x faster; two map allocations eliminated |
| Radix helper, 64 fields | 7,444 ns/extraction | 974.7 ns/extraction | 7.64x faster; two map allocations eliminated |
| Complete `PUTMAP`, 64 fields | 26,107 ns; 11,513 B; 13 allocs | 14,976 ns; 2,144 B; 2 allocs | 1.74x faster; 5.37x lower heap; 6.50x fewer allocs |
| Complete `PUTRT`, 64 fields | 28,813 ns; 11,513 B; 13 allocs | 16,328 ns; 2,144 B; 2 allocs | 1.76x faster; 5.37x lower heap; 6.50x fewer allocs |
| Complete `PUTMAP`, 4,096 fields | 2,311,800 ns; 783,543 B; 50 allocs | 1,453,940 ns; 131,181 B; 2 allocs | 1.59x faster; 5.97x lower heap; 25x fewer allocs |
| Complete `PUTRT`, 4,096 fields | 2,544,023 ns; 783,543 B; 50 allocs | 1,742,736 ns; 131,181 B; 2 allocs | 1.46x faster; 5.97x lower heap; 25x fewer allocs |

The helper CPU figures use same-binary alternating controls. The complete
command rows compare repeated updates to already populated structures before
and after the change. Validation, clone depth, stored values, write accounting,
TTL handling, item counts, snapshots, journals, replication, and wire formats
are unchanged.

<a id="flat-scalar-structured-validation"></a>
### Flat Scalar Structured Validation

Checked map and radix writes previously called `json.Marshal` on the complete
value only to validate it, then discarded the returned bytes. Pair-only command
borrowing made this visible as the last two allocations and 2,144/131,181 bytes
for 64/4,096 scalar fields. Validation now recognizes built-in JSON-safe flat
scalars without encoding: `nil`, booleans, strings, byte slices, integer types,
and finite floats. Flat maps therefore require only one non-retaining type scan.

Any named or custom type, nested map/slice, pointer, invalid float,
`json.Number`, cycle, or marshaler-controlled value continues through
`goccy/go-json`'s compiled encoder. That fallback writes its pooled encoding to
`io.Discard` instead of allocating a returned byte copy. It therefore preserves
the serializer's exact acceptance and custom-marshaler behavior while also
offsetting the preliminary scalar scan for ordinary nested payloads.

Tests were added before production changes. The flat scalar contract initially
failed at two allocations for both map and radix validation. Acceptance tests
compare nested, invalid-number, unsupported function/channel, custom-marshaler,
and cyclic values with `json.Marshal`; a tracking marshaler proves both
validation paths still invoke it. All tests pass for 100 repetitions.

```sh
make run CMD='go test . -run=StructuredValidation -count=100'
make run CMD='go test . -run=TestExecuteCommand -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkFlatScalarStructuredValidationAlternating -benchtime=20x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkStructuredValidationFallbackAlternating -benchtime=10x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkStructuredValidationEncoder/.*64 -benchtime=2000x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkStructuredValidationEncoder/.*4096 -benchtime=50x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandPairBulkReplacement/.*64 -benchtime=2000x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandPairBulkReplacement/.*4096 -benchtime=50x -count=7 -cpu=1 -benchmem'
```

| Validation operation, median | Marshal control | Final validation | Result |
| --- | ---: | ---: | ---: |
| Flat 64-field map | 12,722 ns; 2,144 B; 2 allocs | 795.2 ns; 0 B; 0 allocs | 16.00x faster; heap eliminated |
| Flat 4,096-field map | 937,768 ns; 131,180 B; 2 allocs | 35,610 ns; 0 B; 0 allocs | 26.34x faster; heap eliminated |
| Nested 64-field fallback | 13,548 ns; 5,441 B; 4 allocs | 13,243 ns; 3,392 B; 3 allocs | 1.02x faster; 1.60x lower heap |
| Nested 4,096-field fallback | 1,229,763 ns; 336,101 B; 4 allocs | 1,087,097 ns; 205,029 B; 3 allocs | 1.13x faster; 1.64x lower heap |

| Complete scalar replacement, median | Before validation change | Final | Result |
| --- | ---: | ---: | ---: |
| `PUTMAP`, 64 fields | 14,976 ns; 2,144 B; 2 allocs | 3,251 ns; 0 B; 0 allocs | 4.61x faster; heap eliminated |
| `PUTRT`, 64 fields | 16,328 ns; 2,144 B; 2 allocs | 3,767 ns; 0 B; 0 allocs | 4.33x faster; heap eliminated |
| `PUTMAP`, 4,096 fields | 1,453,940 ns; 131,181 B; 2 allocs | 247,728 ns; 0 B; 0 allocs | 5.87x faster; heap eliminated |
| `PUTRT`, 4,096 fields | 1,742,736 ns; 131,181 B; 2 allocs | 525,596 ns; 0 B; 0 allocs | 3.32x faster; heap eliminated |

The CPU figures use same-binary alternating controls for isolated and fallback
validation; complete rows are seven-run medians before and after the production
change. Fallback allocation figures come from the equivalent discard-encoder
fixture; the scalar scan itself allocates nothing. Error behavior, caller
ownership, clone depth, write accounting, TTL handling, item counts, snapshots,
journals, replication, and wire and storage formats remain unchanged.

<a id="flat-scalar-sequence-validation"></a>
### Flat Scalar Sequence Validation

Checked slice and priority-queue writes had the same discarded-serialization
cost as maps and radix trees. Whole-value validation marshaled every item, while
variadic priority-queue pushes marshaled each item separately. Exact built-in
JSON scalars now take the shared non-retaining type scan. Whole-sequence nested
fallbacks write the encoded representation to `io.Discard`, and variadic slice
fallback builds the same temporary sequence as before, then discard-encodes it.

Tests were added before the production change. The allocation guard initially
reported two allocations for whole slice and priority-queue validation, three
for variadic slice validation, and one allocation per variadic priority-queue
item. Acceptance tests compare valid nested values, invalid floats and
`json.Number` values, unsupported functions, failing custom marshalers, and
cycles with `json.Marshal`. Focused sequence tests pass for 100 repetitions.

```sh
make run CMD='go test . -run=SequenceValidation -count=100'
make run CMD='go test . -run=Slice -count=20'
make run CMD='go test . -run=PriorityQueue -count=20'
make run CMD='go test . -run=NONE -bench=BenchmarkSequenceValidation/.*64 -benchtime=2000x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkSequenceValidation/.*4096 -benchtime=50x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkSequenceValidationFallbackAlternating -benchtime=10x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSequenceValidationFallbackAllocations/.*64 -benchtime=2000x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkSequenceValidationFallbackAllocations/.*4096 -benchtime=50x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkSequenceCheckedReplacement/.*64 -benchtime=2000x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkSequenceCheckedReplacement/.*4096 -benchtime=50x -count=7 -cpu=1 -benchmem'
```

| Flat validation, 64/4,096 items | Marshal baseline | Final scan | Result |
| --- | ---: | ---: | ---: |
| Whole slice | 2,084/145,050 ns; 1,048/65,565 B; 2 allocs | 140.5/7,838 ns; 0 B; 0 allocs | 14.83x/18.51x faster; heap eliminated |
| Variadic slice payload | 3,065/206,568 ns; 2,200/131,104 B; 3 allocs | 154.5/8,617 ns; 0 B; 0 allocs | 19.84x/23.97x faster; heap eliminated |
| Whole priority queue | 4,393/266,619 ns; 2,712/172,066 B; 2 allocs | 126.1/6,907 ns; 0 B; 0 allocs | 34.84x/38.60x faster; heap eliminated |
| Variadic priority payload | 4,244/286,868 ns; 1,024/65,541 B; 64/4,096 allocs | 217.3/12,248 ns; 0 B; 0 allocs | 19.53x/23.42x faster; heap eliminated |

The nested fallback places its only nested map last, forcing the candidate to
scan every preceding scalar before encoding. CPU uses nine same-binary
alternating runs; heap uses the matching allocation fixture.

| Worst-case nested fallback, 64/4,096 items | Marshal control | Final validation | Result |
| --- | ---: | ---: | ---: |
| Slice | 2,286/123,118 ns; 1,144/65,667 B; 3 allocs | 2,067/103,038 ns; 120/125 B; 2 allocs | 1.11x/1.20x faster; 9.53x/525.34x lower heap |
| Priority queue | 3,723/224,722 ns; 2,808/172,174 B; 3 allocs | 2,946/180,901 ns; 120/125 B; 2 allocs | 1.26x/1.24x faster; 23.40x/1,377x lower heap |

| Complete scalar replacement, 64/4,096 items | Before | Final | Result |
| --- | ---: | ---: | ---: |
| `UpsertSliceChecked` | 3,926/198,293 ns; 2,200/131,101 B; 3 allocs | 523.2/42,955 ns; 1,152/65,536 B; 1 alloc | 7.50x/4.62x faster; 1.91x/2.00x lower heap |
| `UpsertPriorityQueueChecked` | 8,049/443,711 ns; 5,912/368,678 B; 3 allocs | 2,791/182,809 ns; 3,200/196,608 B; 1 alloc | 2.88x/2.43x faster; 1.85x/1.88x lower heap |

The one remaining complete-operation allocation owns the replacement sequence;
validation itself is allocation-free. Invalid-value behavior, custom-marshaler
invocation, deep cloning, queue ordering, write accounting, snapshots,
journals, replication, and wire and persistent formats remain unchanged.

Variadic slice validation still materialized the entire payload after finding
one non-scalar, even when thousands of surrounding built-in scalars had already
passed the allocation-free scan. It now remembers the first non-scalar and
validates that value directly when it is the only fallback. Encountering a
second non-scalar selects the prior full-slice construction and single encoder
call, avoiding per-value encoder overhead for nested-heavy payloads.

The allocation test was added before production changes and failed at three
allocations for a 4,096-item payload with one final nested map. Acceptance tests
place valid nested values, invalid floats and numbers, unsupported values,
cycles, and custom marshalers between scalar neighbors. A separate two-marshaler
test proves the retained materialized path invokes each value exactly once.

```sh
make run CMD='go test . -run="SequenceValidation|SparseNestedSlice|MultiFallbackSlice" -count=100'
make run CMD='go test . -run=NONE -bench=BenchmarkSlicePayloadSparseFallback/Items64 -benchmem -benchtime=2000x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSlicePayloadSparseFallback/Items4096 -benchmem -benchtime=50x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSparseSlicePayloadFallbackAlternating -benchtime=10x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkMultipleSlicePayloadFallbackAlternating -benchtime=10x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSlicePayloadFallbackAllocations -benchtime=200x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkSliceCheckedSparsePush/Items64 -benchmem -benchtime=1000x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSliceCheckedSparsePush/Items4096 -benchmem -benchtime=20x -count=7 -cpu=1'
```

| Single nested payload, 64/4,096 items | Materialized control | Direct fallback | Improvement |
| --- | ---: | ---: | ---: |
| Validation | 2,654/149,037 ns; 1,273/65,663 B; 3 allocs | 476.3/10,453 ns; 97 B; 1 alloc | 5.57x/14.26x faster; 13.12x/676.94x lower heap; 3x fewer allocs |
| Complete `PushSliceChecked` plus delete | 6,432/284,075 ns; 2,764/131,753 B; 6/8 allocs | 2,969/38,259 ns; 1,588/66,179 B; 4/5 allocs | 2.17x/7.43x faster; 1.74x/1.99x lower heap; 1.50x/1.60x fewer allocs |

The isolated CPU row uses nine same-binary alternating runs; heap uses the
matching allocation fixture. Complete rows are seven-run medians before and
after the production change. The 64-item complete fixture used 1,000 operations
per run and the 4,096-item fixture used 20.

| Two nested values, 64/4,096 items | Prior materialized path | Final materialized path | Result |
| --- | ---: | ---: | ---: |
| Validation | 3,814/146,870 ns; 1,369/65,759 B; 4 allocs | 3,777/146,420 ns; 1,369/65,759 B; 4 allocs | 1.01x/1.003x faster; heap and allocations identical |

Direct fallback changes only validation work. Stored values are still deeply
cloned, and invalid-value behavior, custom-marshaler counts, mutation ordering,
write accounting, snapshots, journals, replication, wire bytes, and persistent
formats remain unchanged.

Whole slice and priority-queue replacement had a similar sparse case when the
only nested value was last. Because the existing scalar scan has already proven
every earlier item, that final position proves uniqueness without a second scan.
Validation now sends only that trailing value to the discard encoder. A
non-scalar at any earlier position immediately retains the prior whole-sequence
encoder call.

Tests were added before production changes. The 4,096-item allocation guard
initially reported two validation allocations and now reports one. Acceptance
tests place nested, invalid, cyclic, unsupported, and custom-marshaler values
between scalar neighbors, while a two-marshaler test proves the unchanged whole
fallback invokes each marshaler exactly once.

```sh
make run CMD='go test . -run="SequenceValidation|SparseNestedWholeSequence|MultiFallbackWholeSequence" -count=100'
make run CMD='go test . -run=TestSequenceValidationMatchesJSONMarshalAcceptance -race -count=10'
make run CMD='go test . -run=NONE -bench=WholeSequenceFallbackAlternating/Nested1 -benchtime=20x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=WholeSequenceFallbackAlternating/Nested2 -benchtime=30x -count=15 -cpu=1'
make run CMD='go test . -run=NONE -bench=WholeSequenceFallbackAllocations -benchmem -benchtime=200x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=WholeSequenceSparseCheckedReplacement/.*64 -benchmem -benchtime=2000x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=WholeSequenceSparseCheckedReplacement/.*4096 -benchmem -benchtime=50x -count=7 -cpu=1'
```

| Single trailing nested value, 64/4,096 items | Whole-sequence control | Direct fallback | Improvement |
| --- | ---: | ---: | ---: |
| Slice validation | 2,147/106,568 ns; 120/125 B; 2 allocs | 413.9/9,404 ns; 96/101 B; 1 alloc | 5.19x/11.33x faster; 1.25x/1.24x lower heap; 2x fewer allocs |
| Priority-queue validation | 3,158/181,245 ns; 120/125 B; 2 allocs | 436.1/10,255 ns; 96/101 B; 1 alloc | 7.24x/17.67x faster; 1.25x/1.24x lower heap; 2x fewer allocs |

| Complete checked replacement, 64/4,096 items | Before | Final | Improvement |
| --- | ---: | ---: | ---: |
| Slice | 2,589/128,450 ns; 1,608/65,992 B; 5 allocs | 1,835/63,606 ns; 1,584/65,973 B; 4 allocs | 1.41x/2.02x faster; one fewer allocation |
| Priority queue | 6,693/411,923 ns; 3,656/197,078 B; 5 allocs | 4,639/233,791 ns; 3,632/197,056 B; 4 allocs | 1.44x/1.76x faster; one fewer allocation |

The first prototype scanned the remainder after finding a non-scalar so it
could optimize a sole nested value at any position. A second nested value then
made the unchanged 4,096-item fallback about 1.01x slower. That prototype was
removed. In the retained trailing-only design, the two-nested paired medians
were neutral within 0.6% at 64 items; at 4,096 items slice validation was 1.01x
faster and priority-queue validation was unchanged. Candidate and control both
used exactly 217 B and three allocations at both sizes.

<a id="mutation-response-lock-release-rollback"></a>
### Mutation Response Lock Release Rollback

`POPSLICE` and `POPPQ` remove and retain their response value while holding the
exclusive cache lock. A candidate completed mutation, accounting, and cache
publication under that lock, then unlocked before serializing a non-string
response. Tests added before the candidate used a blocking caller-controlled
JSON marshaler: both legacy commands held an unrelated writer for at least 100
ms, while the candidate let the writer finish before marshaling was released
and still returned the removed point-in-time value. The race detector passed.

The first grouped microbenchmark was discarded because fixture rebuilding
dominated wall time and exposed different CPU-frequency phases. The final
same-binary control preloaded 32,768 independent one-item keys, timed legacy
and candidate command batches in alternating 256-key chunks, alternated first
order across five rounds, and repeated the process seven times on one logical
CPU:

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkMutationResponseLockScopePaired -benchtime=5x -count=7 -cpu=1'
```

| Pop response, median paired ratio | Candidate versus legacy | Heap / allocations |
| --- | ---: | ---: |
| Slice string | 0.9997x | unchanged at 0 B / 0 |
| Slice structured | 0.9752x, or 1.03x slower | unchanged at 152 B / 3 |
| Priority-queue string | 1.010x | unchanged at 32 B / 1 |
| Priority-queue structured | 0.9729x, or 1.03x slower | unchanged at 576 B / 8 |

The lock release therefore traded an unbounded writer stall reduction for a
repeatable complete-path CPU regression on both ordinary structured response
families. Production and experimental test code were fully reverted. Current
wire bytes, mutation semantics, lock scope, heap, and allocations are
unchanged.

<a id="compact-priority-queue-items"></a>
### Compact Priority-Queue Items

The typed priority-queue string path previously stored `Value`, `stringValue`,
and a `hasString` boolean in every heap item. On amd64 the trailing boolean
forced the item from 48 to 56 bytes. Nonempty strings now use the nonempty
`stringValue` field as their tag. Empty strings use one process-global
pre-boxed empty-string interface, while generic nil and other values retain
their prior `Value` representation. There is no per-cache or per-item marker
allocation, sequence range change, or representation promotion.

Tests were added before the layout change for nonempty and empty strings,
generic nil, uncomparable nested maps, FIFO ties, deep-copy ownership, snapshot
materialization, and the exact 48-byte item invariant. Existing snapshot,
LevelDB, sequence-exhaustion, and command tests cover the surrounding formats
and API behavior.

The retained-memory fixture builds one 100,000-item string queue from
preallocated inputs. Seven A/B runs alternate detached baseline `2436f19` and
candidate test binaries pinned to one CPU. Heap and allocation counts cover the
complete timed benchmark operation.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkPriorityQueueItemLayout100k -benchtime=1x -count=7 -benchmem -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkPriorityQueueTagOperations -benchtime=500ms -count=7 -benchmem -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/PriorityQueuePushPop -benchtime=1000000x -count=7 -benchmem -cpu=1'
```

| 100k-item layout | Baseline | Final | Improvement |
| --- | ---: | ---: | ---: |
| Item struct | 56 B | 48 B | 1.17x smaller |
| Retained heap/item | 56.06 B | 48.04 B | 1.17x lower |
| Timed cumulative heap | 8,003,616 B | 7,200,800 B | 1.11x lower |
| Timed allocations | 3 | 3 | Unchanged |
| Median build time/item | 135.2 ns | 119.2 ns | 1.13x faster |

| Operation, seven-run median | Baseline | Final | Improvement |
| --- | ---: | ---: | ---: |
| Nonempty string push/pop | 30.56 ns; 0 B; 0 allocs | 24.03 ns; 0 B; 0 allocs | 1.27x faster |
| Empty string push/pop | 30.74 ns; 0 B; 0 allocs | 24.07 ns; 0 B; 0 allocs | 1.28x faster |
| Generic value dispatch | 1.623 ns; 0 B; 0 allocs | 1.485 ns; 0 B; 0 allocs | 1.09x faster |
| Command push/pop | 641.1 ns; 40 B; 2 allocs | 612.1 ns; 40 B; 2 allocs | 1.05x faster; heap and allocs unchanged |

Exact plain-string `POPPQ` still extracted the compact item's value through
`interface{}` even though the typed accessor used by direct queue reads was
already available. The pop path now calls that accessor directly. Existing
semantic tests cover priority ordering, replacement, empty queues, generic
values, and exact output; before the production substitution, the allocation
budget was tightened from a loose upper bound to exactly the one returned JSON
string for a reused push/pop request.

```sh
make run CMD='go test . -run="TestExecuteCommandPriorityQueueExactPath|TestExecuteCommandPriorityQueueOperations" -count=100'
make run CMD='go test . -run=NONE -bench=BenchmarkPriorityQueuePopStringResponse -benchtime=1000000x -count=7 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/PriorityQueuePushPop -benchtime=1000000x -count=9 -cpu=1 -benchmem'
```

| Plain-string pop response, seven-run median | Interface extraction | Typed extraction | Improvement |
| --- | ---: | ---: | ---: |
| JSON response construction | 49.27 ns; 32 B; 1 alloc | 45.88 ns; 32 B; 1 alloc | 1.07x faster; heap and allocations unchanged |

The complete post-change push/pop median is 510.8 ns with the benchmark's
per-iteration priority pointer included, still 40 B and two allocations. The
separate-process baseline varied too much to attribute a complete-command ratio;
the same-binary response benchmark is the accepted CPU evidence. Empty or
escaped strings continue through generic JSON encoding, and structured values,
wire bytes, snapshots, journals, replication, and persistent formats are
unchanged.

The first candidate used a private marker in `Value` for every string. It
reached the same 48-byte item size, but generic dispatch regressed from 1.534
to 1.961 ns because it paid an interface-marker comparison. That variant was
discarded. The retained design dispatches on `stringValue` length and has no
generic-path marker check. Wire, snapshot, journal, and persistent storage
formats remain unchanged.

Exact `GETPQ` and `GETPRIORITY` command reads previously copied the internal
heap, popped and cloned every value into a second public slice, then encoded
that slice. The exact priority-queue commands now detect string-only queues for
exact output sizing, copy only a multi-item heap, and write the canonical JSON
array after unlocking. Empty and one-item queues need no heap copy. Mixed
queues encode strings without interface boxing and delegate only each
structured payload to the existing generic value encoder. Lazy storage
references retain the prior checked hydration path.

Tests were added before implementation and compare both aliases with the
generic encoder for empty, missing, wrong-type, escaped, HTML-sensitive,
Unicode, invalid UTF-8, signed-priority-boundary, and equal-priority ordering
cases. They also verify exact read telemetry and direct structured-value parity.
The rows are medians from seven runs for empty, one, and 100 items and nine runs
for 16 items, pinned to one CPU. The generic control and exact path execute in
the same binary.

```sh
make run CMD='go test . -run=none -bench=BenchmarkPriorityQueueGetCommand -benchmem -benchtime=500ms -count=7 -cpu=1'
```

| Command read | Public materialization | Direct string JSON | Improvement |
| --- | ---: | ---: | ---: |
| Empty | 214.6 ns; 64 B; 3 allocs | 54.02 ns; 0 B; 0 allocs | 3.97x faster; allocation-free |
| One item | 414.2 ns; 192 B; 6 allocs | 145.5 ns; 48 B; 1 alloc | 2.85x faster; 4.00x lower heap; 6x fewer allocs |
| 16 items | 2,894 ns; 2,168 B; 21 allocs | 2,098 ns; 1,472 B; 2 allocs | 1.38x faster; 1.47x lower heap; 10.5x fewer allocs |
| 100 items | 25,384 ns; 17,528 B; 108 allocs | 18,676 ns; 8,320 B; 2 allocs | 1.36x faster; 2.11x lower heap; 54x fewer allocs |

The response bytes, priority/sequence ordering, public `Items()` ownership,
snapshot, journal, replication, and persistent formats are unchanged. The path
adds no retained fields, configuration, background work, or fixed memory.

The generic exact-uppercase `GET` dispatcher now uses the same encoder for an
in-memory priority queue. It prepares the copied heap under the existing shared
read lock, preserving the scalar GET fallback for expired TTLs and lazy
storage. Non-priority-queue value types execute the byte-for-byte previous
switch branches.

```sh
make run CMD='go test . -run=none -bench=BenchmarkPriorityQueueGenericGetCommand -benchmem -benchtime=500ms -count=7 -cpu=1'
make run CMD='go test . -run=none -bench=BenchmarkPriorityQueueStructuredGetCommand/Items100 -benchmem -benchtime=750ms -count=9 -cpu=1'
```

| Generic `GET` | Public materialization | Shared-lock direct JSON | Improvement |
| --- | ---: | ---: | ---: |
| Empty | 207.1 ns; 64 B; 3 allocs | 159.3 ns; 0 B; 0 allocs | 1.30x faster; allocation-free |
| One item | 422.4 ns; 192 B; 6 allocs | 268.3 ns; 48 B; 1 alloc | 1.57x faster; 4.00x lower heap; 6x fewer allocs |
| 16 items | 2,863 ns; 2,168 B; 21 allocs | 2,386 ns; 1,472 B; 2 allocs | 1.20x faster; 1.47x lower heap; 10.5x fewer allocs |
| 100 items | 23,449 ns; 17,528 B; 108 allocs | 17,977 ns; 8,320 B; 2 allocs | 1.30x faster; 2.11x lower heap; 54x fewer allocs |
| 100 items, final heap slot structured | 26,295 ns; 17,985 B; 112 allocs | 19,154 ns; 11,513 B; 4 allocs | 1.37x faster; 1.56x lower heap; 28x fewer allocs |

The structured row exercises the longest string-only eligibility scan. An
intermediate implementation then returned to generic materialization and could
drift about 1% slower, so it was not retained. Direct mixed-value encoding
removes that second pass and its public-slice clones. Empty, one-, mixed-, and
string-only queues retain the same response bytes and ownership guarantees as
the dedicated commands.

<a id="radix-node-tag-compaction-rollback"></a>
### Radix-Node Tag Compaction Rollback

A test-first candidate removed the radix node's `hasValue` field and represented
a stored `nil` with one process-global private marker. It preserved split,
merge, lookup, deletion, enumeration, snapshot, and missing-key behavior and
reduced the amd64 node struct from 64 to 56 bytes. The 100,000-key fixture held
111,112 nodes; retained memory fell from 115.2 to 102.4 B/node and cumulative
heap fell from 23,466,496 to 20,799,856 B with the same 55,556 allocations.

Pinned, alternating seven-run A/B measurements against commit `6e008d1` found
that the marker check cost more on the dominant read path than the memory saved.

| Operation, seven-run median | Baseline | Compact candidate | Result |
| --- | ---: | ---: | ---: |
| Build 100k keys | 235.9 ns/key | 226.7 ns/key | 1.04x faster |
| Get string | 11.82 ns | 13.03 ns | 1.10x slower |
| Get stored `nil` | 9.953 ns | 11.12 ns | 1.12x slower |
| Get missing key | 7.092 ns | 8.225 ns | 1.16x slower |
| Retained heap/node | 115.2 B | 102.4 B | 1.125x lower |
| Cumulative heap | 23,466,496 B | 20,799,856 B | 1.128x lower |
| Allocations | 55,556 | 55,556 | Unchanged |

Both interface equality and private type-assertion marker variants failed the
read-throughput gate. The candidate and its marker were removed, restoring the
explicit presence bit and all baseline runtime characteristics. This rollback
changes no wire, snapshot, journal, or persistent storage format.

A later fast-path pass added exact numeric and plain-string command routes for
roaring/sparse adds, HyperLogLog add/count, Top-K add/get, quantile add/query,
and Fenwick add/range. The test `TestExecuteExactFastCommandCoversCompactNumericRows`
compares each new route against the generic command path before benchmarking.

```
make bench-hatrie-command-features HATRIE_COMMAND_BENCH='^BenchmarkCommandFeature/(RoaringAdd|SparseBitsetAdd|HyperLogLogAdd|HyperLogLogCount|TopKAdd|TopKGet|QuantileSketchAdd|QuantileSketchEstimate|FenwickTreeAdd|FenwickTreeRange)$' BENCHTIME=100000x
```

| Feature | Benchmark row | Time/op | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Roaring bitmap add | `BenchmarkCommandFeature/RoaringAdd` | 539.8 ns | 0 B | 0 |
| Sparse uint64 bitset add | `BenchmarkCommandFeature/SparseBitsetAdd` | 436.6 ns | 0 B | 0 |
| HyperLogLog add | `BenchmarkCommandFeature/HyperLogLogAdd` | 5,187 ns | 0 B | 0 |
| HyperLogLog count | `BenchmarkCommandFeature/HyperLogLogCount` | 4,399 ns | 0 B | 0 |
| Top-K add | `BenchmarkCommandFeature/TopKAdd` | 654.6 ns | 72 B | 3 |
| Top-K get | `BenchmarkCommandFeature/TopKGet` | 403.9 ns | 80 B | 1 |
| Quantile sketch add | `BenchmarkCommandFeature/QuantileSketchAdd` | 823.4 ns | 64 B | 1 |
| Quantile sketch estimate | `BenchmarkCommandFeature/QuantileSketchEstimate` | 638.3 ns | 64 B | 1 |
| Fenwick tree add | `BenchmarkCommandFeature/FenwickTreeAdd` | 783.2 ns | 95 B | 1 |
| Fenwick tree range | `BenchmarkCommandFeature/FenwickTreeRange` | 320.5 ns | 0 B | 0 |

<a id="incremental-hyperloglog-estimates"></a>
### Incremental HyperLogLog Estimates

HyperLogLog add responses, count commands, and info commands previously scanned
every register while holding the cache mutex. The command benchmark uses
precision 10 with 1,024 registers; the default precision 14 has 16,384
registers. The final implementation maintains the harmonic register sum and
zero-register count whenever a rank increases, so estimation is O(1).

The derived fields are rebuilt once after snapshot or persistent-storage load.
They are not serialized: snapshots, journals, replication payloads, encoded
sizes, and the exact register backing remain unchanged. A missing derived state
on a manually constructed internal value falls back to a full scan and is
rebuilt on mutation.

Tests were added before the production change and use the old full scan as an
independent oracle. They compare every public estimate after mutations at
precisions 4, 10, 14, and 20, continue through 200,000-value distinct streams,
exercise duplicates and adversarial ranks, and verify snapshot reconstruction
and continued mutation. Layout tests pin the amd64 header at 48 bytes and prove
that snapshots contain only register bytes.

Seven A/B runs alternate baseline `df0083e` and candidate test binaries pinned
to CPU 0. Command rows use 100,000 fixed iterations; focused rows use a 100 ms
minimum; layout and 4,096-value batch rows use one complete operation.

```sh
make run CMD='go test . -run=HyperLogLog -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkHyperLogLogEstimateOperations -benchmem -benchtime=100ms -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/HyperLogLog -benchmem -benchtime=100000x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkHyperLogLogLayout1000 -benchmem -benchtime=1x -count=7 -cpu=1'
```

| Operation, seven-run median | Baseline full scan | Incremental state | Improvement |
| --- | ---: | ---: | ---: |
| Precision-10 command add | 3,476 ns | 251.7 ns | 13.81x faster |
| Precision-10 command count | 3,393 ns | 231.6 ns | 14.65x faster |
| Precision-10 direct count | 3,190 ns | 31.88 ns | 100.06x faster |
| Precision-10 info | 3,667 ns | 35.19 ns | 104.20x faster |
| Precision-10 duplicate add and count | 3,229 ns | 36.32 ns | 88.90x faster |
| Precision-14 direct count | 53,283 ns | 31.73 ns | 1,679x faster |
| Precision-14 info | 58,395 ns | 34.99 ns | 1,669x faster |
| Precision-14 duplicate add and count | 50,760 ns | 36.91 ns | 1,375x faster |
| Precision-14 4,096-value add and count | 112,783 ns | 70,112 ns | 1.61x faster |

All rows remain at 0 B/op and 0 allocs/op. The mutation-only internal primitive,
which deliberately omits the required returned estimate, changes from 26.75 to
28.45 ns, or 1.06x slower. Complete single and 4,096-value add/count operations
remain faster because they eliminate the register scan.

| 1,000 materialized default filters | Baseline | Final | Change |
| --- | ---: | ---: | ---: |
| HLL header | 40 B/filter | 48 B/filter | 8 B higher |
| Register capacity | 16,384 B/filter | 16,384 B/filter | Unchanged |
| Timed cumulative heap | 16,424,960 B | 16,433,152 B | 0.050% higher |
| Timed allocations | 1,001 | 1,001 | Unchanged |

An initial candidate stored 16 summary bytes after each register slice. Although
the logical overhead was only 16 bytes, Go's size classes rounded each 16,400 B
allocation to 18,432 B; the fixture rose to 18,472,960 B, or 12.47% more heap.
That layout was discarded. The retained header layout leaves register
allocations exact and has no cost for serialized or transferred data.

<a id="xor-filter-scalar-fast-path"></a>
### XOR Filter Scalar Fast Path

Plain-string `ADDXF` and `HASXF` commands previously materialized canonical JSON
keys through the generic encoder. `ADDXF` also allocated a temporary one-item
slice and duplicate-detection map. `HASXF` performed a metadata lookup followed
by a second locked membership lookup. Exact scalar commands now hash canonical
JSON string bytes directly; staging constructs the same retained key without
the transient batch structures, and lookup performs both checks under one lock.

Tests were added before the implementation. They compare canonical keys and
hashes over multiple seeds, exact and generic command responses, aliases,
missing and unbuilt filters, generic fallbacks, cache read/write counters, and
pending and built snapshots. Escaped, Unicode, empty, and batch values continue
through the generic encoder. Snapshot, storage, journal, replication, and wire
formats are unchanged.

The `HASXF` rows are seven fixed one-million-operation runs before and after the
change. The lifecycle A/B runs both paths in the same binary for 10,000 fixed
iterations, repeated three times. Its generic control uses a leading-space
uppercase command, which bypasses exact dispatch and normalizes without the
extra allocation caused by lowercase conversion. Each lifecycle operation
creates a filter, stages 64 distinct scalar strings, builds it, and destroys the
trie.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/XorHas -benchmem -benchtime=1000000x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkXorCommandBuild64Path -benchmem -benchtime=10000x -count=3 -cpu=1'
```

| Operation, median | Generic baseline | Scalar fast path | Improvement |
| --- | ---: | ---: | ---: |
| Built-filter `HASXF` | 557.7 ns; 64 B; 4 allocs | 286.0 ns; 0 B; 0 allocs | 1.95x faster; all transient heap removed |
| Create + 64 `ADDXF` + build | 253.7 us; 22,601 B; 370 allocs | 232.9 us; 20,553 B; 242 allocs | 1.09x faster; 1.10x lower heap; 1.53x fewer allocations |

`ADDXF` necessarily retains one canonical key and value per distinct staged
item, exactly as before. The optimization removes 2,048 cumulative bytes and
128 allocations from the 64-item lifecycle without adding fields, background
work, configuration, or retained state. Plain-string filter contents and built
fingerprints remain byte-identical to the generic path.

<a id="adaptive-xor-batch-deduplication"></a>
#### Adaptive Generic Batch Deduplication

Generic `xorFilterData.AddOne` previously created a duplicate-detection map for
every request, including a scalar insertion and the small variadic batches used
by direct callers. The final path inserts one validated value directly, scans
the already-required pending slice for batches of two through eight, and keeps
the map for nine or more values where constant-time membership is expected to
win. The threshold is fixed from the measured crossover rather than exposed as
runtime configuration.

The test-first fixture covers an existing staged value, duplicates within one
request, mixed strings and `json.Number` values, and a late unsupported value.
It proves transactional validation: an error after valid leading values leaves
the complete staged snapshot unchanged. The implementation still validates and
deduplicates the full request before cloning or publishing any pending value.

The same-binary control retains the former per-request map algorithm and runs
16,384 additions per block, alternating which implementation goes first. Ten
fixed ten-pair runs on one logical CPU produced these medians; a separate
100,000-iteration benchmark confirmed the allocation profile:

```sh
make run CMD='go test . -run=TestXorFilterGenericBatchAddDeduplicatesTransactionally -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterGenericBatchDedupAlternating -benchtime=10x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterGenericBatchDedupStrategy -benchmem -benchtime=100000x -count=10 -cpu=1'
```

| Generic addition | Map control | Adaptive final | Improvement |
| --- | ---: | ---: | ---: |
| One unique value | 275.2 ns | 245.6 ns | 1.12x faster |
| Two unique values | 426.0 ns | 396.5 ns | 1.07x faster |
| Four unique values | 696.3 ns | 643.6 ns | 1.08x faster |
| Eight unique values | 1,219 ns | 1,173.5 ns | 1.04x faster |
| Four duplicate-heavy values | 628.9 ns | 595.9 ns | 1.06x faster |
| Eight duplicate-heavy values | 1,047.5 ns | 985.7 ns | 1.06x faster |

The accepted small workloads are 1.04x-1.12x faster, with heap and allocations
unchanged in every pair. Unique one/two/four/eight-value requests remain at
368/464/592/848 B and 4/7/11/19 allocations. The 16-value control confirms the
unchanged map path and memory profile; its separate CPU samples were noisy and
are not claimed as an improvement. Filter contents, item counts, build output,
snapshots, persistence, replication, and wire formats are unchanged.

<a id="compact-xor-filter-headers"></a>
### Compact XOR-Filter Headers

The XOR-filter header previously placed a one-byte built flag between aligned
integer fields and a four-byte block length before pointer-aligned fields. Two
independent padding gaps made the header 72 bytes. Grouping fields by alignment
reduces it to exactly 64 bytes without changing a field, branch, allocation, or
algorithm. Named-field construction means snapshots and restored filters are
unaffected, and the in-memory type is private.

A layout test and 100,000-filter fixture were added before the reorder. The
test pins both the 72-byte legacy control and final 64-byte header. Existing
tests cover staging, retries, byte-identical fingerprint construction, lookup,
info, snapshots, restore, and continued mutation. The focused lookup keeps the
old and new field orders in the same test binary and executes identical built-
filter logic on the same fingerprint backing.

```sh
make run CMD='go test . -run="TestXorFilter(HeaderLayoutIsBounded|BuildAndContains|SnapshotRoundTrip|BuildRetriesWithoutChangingFingerprintResult|PlainJSONStringFastPathMatchesGeneric)" -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterHeaderLayout100k -benchmem -benchtime=1x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterHeaderLookupLayout -benchmem -benchtime=500ms -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/XorHas -benchmem -benchtime=1000000x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterLifecyclePhases64/PlainStringStage -benchmem -benchtime=1s -count=9 -cpu=1'
```

| Metric, median | Legacy field order | Compact field order | Improvement |
| --- | ---: | ---: | ---: |
| Header size | 72 B/filter | 64 B/filter | 1.125x smaller |
| Retained 100k layout | 72.01 B/filter | 64.06 B/filter | 1.12x lower |
| Initialize 100k headers | 51.28 ns/filter | 34.19 ns/filter | 1.50x faster |
| Same-binary built lookup | 17.16 ns | 16.78 ns | 1.02x faster |
| Complete `HASXF` command | 208.4 ns | 209.8 ns | Neutral within 0.7%; 0 B and 0 allocations both |
| Stage 64 strings | 11,995 ns | 10,250 ns | 1.17x faster; 11,416 B and 139 allocations unchanged |

The retained-memory delta is about 776 KiB per 100,000 filters, before
allocator and cache-locality effects elsewhere in the process. The separate-
run command and staging rows are supporting regression checks; the primary CPU
evidence is the same-binary lookup. Fingerprint bytes, staged maps, public
results, snapshots, journals, replication, storage, and wire formats are
unchanged.

<a id="linked-xor-filter-build-queue"></a>
### Linked XOR-Filter Build Queue

Fingerprint construction previously allocated a `uint32` queue sized for all
three hash blocks in addition to the slot, peel-order, and final fingerprint
arrays. Each 16-byte build slot already contained four alignment-padding bytes.
Those bytes now hold the next queued slot index, removing the queue allocation
without increasing the slot, filter, or storage representation. The peel-order
slice remains contiguous for cache-local reverse assignment.

Tests were added before the implementation. A legacy slice-queue builder is
kept in test code as an independent oracle for the first successful seed,
failed-attempt handling, exact block length, and byte-identical fingerprints.
The slot-size assertion fixes the build slot at 16 bytes. The same-binary
benchmark ran each layout for 500 ms to one second, seven to ten times on one
CPU; rows below are medians.

```sh
make run CMD='go test . -run="TestXorFilterBuild(MatchesFirstSuccessfulFingerprintAttempt|RetriesWithoutChangingFingerprintResult|SlotFitsQueueLinkInExistingPadding)" -count=1 -v'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterQueueLayout -benchmem -benchtime=500ms -count=10 -cpu=1'
```

| Fingerprint build | Slice queue | Slot-linked queue | Improvement |
| --- | ---: | ---: | ---: |
| Three items, one failed seed | 873.4 ns; 1,744 B; 7 allocs | 733.4 ns; 1,424 B; 5 allocs | 1.19x faster; 1.22x lower heap; 1.40x fewer allocs |
| 64 items | 4,084 ns; 3,680 B; 4 allocs | 3,944 ns; 3,200 B; 3 allocs | 1.04x faster; 1.15x lower heap; 1.33x fewer allocs |
| 4,096 items | 0.339 ms; 173,312 B; 4 allocs | 0.324 ms; 152,832 B; 3 allocs | 1.05x faster; 1.13x lower heap; 1.33x fewer allocs |
| 65,536 items | 6.474 ms; 2,752,520 B; 4 allocs | 6.198 ms; 2,424,840 B; 3 allocs | 1.04x faster; 1.14x lower heap; 1.33x fewer allocs |

The complete 64-item scalar command lifecycle drops from the preceding 242
allocations and about 20,689 B to 241 allocations and about 20,073 B. Retained
fingerprints, false-positive behavior, seed selection, snapshots, journal,
storage, replication, and wire bytes are unchanged.

An initial variant also linked the reverse peel order through the slots. It
reduced a successful build to two allocations and roughly half the cumulative
heap, but random slot traversal made 4,096/65,536-item builds 1.03x/1.05x
slower. That variant was removed and is indexed as rejected; it adds no runtime
cost.

<a id="order-independent-xor-filter-build"></a>
#### Order-Independent XOR-Filter Build

`Build` previously copied staged map keys into a slice and sorted them before
fingerprint construction. Sorting is unnecessary: each key contributes count
and XOR updates to three slots, making slot aggregation commutative, and the
peel queue is initialized and traversed by slot index. Map order therefore
cannot alter the selected seed, block length, or fingerprint bytes.

A test added before the production change compares sorted and reversed 4,096-
key inputs across every attempted seed. The same-binary benchmark alternates
sorted and direct-map-order builds so host-frequency drift affects both sides.
Separate allocation controls verify that removing the in-place sort changes no
heap or allocation count.

```sh
make run CMD='go test . -run TestXorFilterFingerprintBuildIsOrderIndependent -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterBuildKeyOrderAlternating -benchtime=100x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterBuildKeyOrderAllocations -benchtime=100x -count=5 -cpu=1 -benchmem'
```

| Staged items, paired median | Sorted keys | Direct map order | Improvement |
| ---: | ---: | ---: | ---: |
| 64 | 7,520 ns | 4,513 ns | 1.67x faster |
| 4,096 | 895,411 ns | 430,851 ns | 2.08x faster |
| 65,536 | 18,136,097 ns | 10,070,846 ns | 1.80x faster |

The allocation controls measured 4,352 B/4 allocations at 64 items and
218,368 B/4 allocations at 4,096 items for both layouts. At 65,536 items both
were about 3.47 MB and four allocations. Staged values, pending snapshots,
fingerprints, false-positive behavior, storage, journals, replication, and wire
formats are unchanged.

<a id="compact-xor-filter-build-hash-index"></a>
#### Compact XOR-Filter Build Hash Index

After sorting was removed, `Build` still copied every staged key into a
temporary `[]string`. Each 16-byte string header remained live through all seed
attempts, and each retry rescanned every key byte to recompute the same FNV base
hash before mixing in a different seed. Build now computes each seed-independent
base hash once. Up to 64 hashes use a fixed 512-byte stack array; larger builds
use one 8-byte-per-key slice. Every seed still passes through the exact prior
mix, slot aggregation, peel order, and fingerprint assignment.

The allocation test was added before production changes and failed at four
allocations for a successful 64-item build, then passed at the three required
builder arrays. Existing first-success, forced-retry, reversed-order, snapshot,
lookup, and scalar-fast-path tests preserve the exact seed, block length, and
fingerprint bytes. Focused tests pass for 100 repetitions and under the race
detector.

```sh
make run CMD='go test . -run="TestXorFilterBuild(DoesNotCopyStagedKeys|MatchesFirstSuccessfulFingerprintAttempt|RetriesWithoutChangingFingerprintResult|AndContains)" -count=100'
make run CMD='go test . -run=TestXorFilterBuildAndContains -race -count=20'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterHashedStagedBuildAlternating -benchtime=5x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterHashedStagedBuildAllocations -benchmem -benchtime=20x -count=3 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/XorBuild64Items -benchmem -benchtime=1000x -count=7 -cpu=1'
```

| Staged build, paired median | String-header control | Base-hash index | Improvement |
| ---: | ---: | ---: | ---: |
| 64 items | 6,281 ns; 4,352 B; 4 allocs | 5,675 ns; 3,200 B; 3 allocs | 1.11x faster; 1.36x lower heap; 1.33x fewer allocs |
| 4,096 items | 371,676 ns; 218,368 B; 4 allocs | 367,143 ns; 185,600 B; 4 allocs | 1.01x faster; 1.18x lower heap |
| 65,536 items | 7,918,608 ns; 3,473,422 B; 4 allocs | 7,820,713 ns; 2,949,129 B; 4 allocs | 1.01x faster; 1.18x lower heap |
| Three items, forced failed first seed | 981.9 ns | 985.8 ns | Neutral within 0.4%; the hash index is reused by the retry |

The complete create, 64 scalar adds, and build lifecycle now uses 18,905 B and
240 allocations, down from the immediately preceding 20,057 B and 241
allocations. A direct-map prototype removed the index entirely and looked
faster in separate large runs, but paired controls found it 1.06x slower at 64
items and about 1.20x slower on the forced retry. It was removed and is listed
in the rejected index. The retained design changes no filter header, staged or
built state, selected seed, fingerprint bytes, false-positive behavior,
snapshot, journal, replication, storage, or wire format.

<a id="xor-staging-marker-rollback"></a>
#### XOR Staging Marker Rollback

Plain-string staging stores a canonical quoted JSON key and a boxed value used
by pending snapshots. A candidate replaced each value with one process-global
marker and reconstructed the plain string from the key during snapshot capture.
The existing fast-path equivalence test plus a test-first marker assertion
proved identical pending snapshots, builds, fingerprints, and lookups.

```sh
make run CMD='go test . -run XorFilter -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkXorFilterLifecyclePhases64 -benchtime=10000x -count=9 -cpu=1 -benchmem'
```

| 64 staged strings, median | Existing boxed value | Marker candidate | Result |
| --- | ---: | ---: | --- |
| Stage | 12,759 ns; 11,416 B; 139 allocs | 10,833 ns; 10,392 B; 75 allocs | Candidate 1.18x faster, 1.10x lower heap, 1.85x fewer allocations |
| Pending snapshot | 6,156 ns; 3,456 B; 2 allocs | 7,570 ns; 4,480 B; 66 allocs | Candidate 1.23x slower, 1.30x higher heap, and 33x more allocations |

The marker did not eliminate value boxing; it deferred 64 boxes from staging to
every pending snapshot. That is worse for repeated backups and monitoring, so
the candidate and its representation test were removed. The original staged
value and all baseline runtime behavior remain.

<a id="inline-sparse-bitset-containers"></a>
### Inline Sparse-Bitset Containers

Sparse bitsets divide `uint64` values into sorted 16-bit containers. A sparse
container with one value previously retained a separate tiny `[]uint16`
allocation. The 64-byte container had four alignment-padding bytes after its
cardinality, so it now stores its first two sorted values there. A third value
promotes to the existing slice representation; removal back to two values
releases the slice. Snapshot restore selects the same compact representation.
Dense bitmap conversion remains at 4,097 values.

Behavior tests were added before implementation for unsorted inserts,
duplicates, lookup, ordered output, encoded size, snapshots, promotion,
demotion, and final removal. Existing dense conversion, command, snapshot,
storage, journal, and replication suites remain applicable. At this pass, a
size assertion fixed the container at 64 bytes; the later
[compact-header pass](#compact-sparse-bitset-headers) reduces it to 48 bytes.
Same-binary controls retain the previous slice-value layout and alternate
execution order for the many-container CPU comparison.

```sh
make run CMD='go test . -run="TestSparseBitset|TestCheckedSparseBitset|TestExecuteCommandSparseBitset" -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSparseBitsetInlineLayout -benchmem -benchtime=500ms -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSparseBitsetDistinctSmallContainersAlternating -benchmem -benchtime=50x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSparseBitsetInlineRetained100k -benchmem -benchtime=1x -count=5 -cpu=1'
```

| Operation, median | Slice-backed control | Inline final | Improvement |
| --- | ---: | ---: | ---: |
| Build one value | 60.66 ns; 72 B; 2 allocs | 37.43 ns; 64 B; 1 alloc | 1.62x faster; one allocation removed |
| Build two values | 67.88 ns; 72 B; 2 allocs | 42.16 ns; 64 B; 1 alloc | 1.61x faster; one allocation removed |
| Promote at three values | 75.39 ns; 72 B; 2 allocs | 62.57 ns; 72 B; 2 allocs | 1.20x faster; heap and allocations unchanged |
| Build 16,384 singleton containers, alternating timer | 2.256 ms | 1.812 ms | 1.25x faster |
| Build 100,000 singleton containers | 26.63 ms; 35,311,496 B; 100,031 allocs | 23.49 ms; 34,511,480 B; 31 allocs | 1.13x faster; 1.02x lower cumulative heap; 3,227x fewer allocs |
| Retained singleton layout, 100,000 containers | 79.60 B and 0.500 objects/container | 71.60 B and 0.000030 objects/container | 1.11x lower heap; about 16,667x fewer retained objects |
| 4,096-value array lookup | 30.21 ns; 0 allocs | 29.93 ns; 0 allocs | 1.01x faster |
| 4,097-value bitmap build / lookup | 0.201/0.003440 us | 0.189/0.003441 us | Build 1.06x faster; lookup neutral within 0.03% |

An initial version retained the generic closure-based array search. Although
small containers improved, its extra representation branch made a 4,096-value
array lookup 30.86 ns versus 29.93 ns for the control, or 1.03x slower. It was
replaced by an inlineable typed binary search; the final promoted lookup is
1.01x faster. No failed implementation remains in production.

<a id="compact-sparse-bitset-headers"></a>
### Compact Sparse-Bitset Headers

Dense sparse-bitset containers always have exactly 1,024 `uint64` words. Their
stored `[]uint64` nevertheless occupied a 24-byte pointer/length/capacity
header in every container, including the common inline singleton containers.
The final layout stores a pointer to a fixed `[1024]uint64` backing and creates
a local slice view while operating on dense words. The container shrinks from
64 to 48 bytes; the two inline values still occupy otherwise unused tail
padding, and the dense allocation remains exactly 8 KiB.

Tests added before the change cover dense promotion, lookup, snapshot
round-trip, demotion, and the header invariant. A same-binary control retains
the prior slice header with identical inline, array, bitmap, and conversion
logic. Paired operations alternate execution order.

```sh
make run CMD='go test . -run SparseBitset -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSparseBitsetBackingLayoutAlternating -benchtime=500x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkSparseBitsetCompactHeaderRetained100k -benchtime=1x -count=5 -cpu=1 -benchmem'
```

| Operation, median | Slice-header control | Fixed-pointer final | Improvement |
| --- | ---: | ---: | ---: |
| Build 4,097-value bitmap | 211.179 us | 211.061 us | Neutral within 0.1% |
| Bitmap lookup | 3.068 ns | 2.954 ns | 1.04x faster |
| Bitmap remove/add | 5.675 ns | 5.594 ns | 1.01x faster |
| Build 100,000 singleton containers | 22.061 ms; 34,511,448 B; 30 allocs | 17.478 ms; 27,819,368 B; 30 allocs | 1.26x faster; 1.24x lower cumulative heap; allocations unchanged |
| Retained singleton layout | 71.60 B/container | 57.75 B/container | 1.24x lower retained heap |

Validation, bitmap thresholds, and persistent decoding already require the
fixed word count. Wire, snapshot, journal, database, ordering, and public API
behavior are unchanged. No operation regression was measured.

<a id="compact-roaring-container-headers"></a>
### Compact Roaring-Container Headers

Every dense Roaring container has exactly 1,024 `uint64` bitmap words. The
previous `[]uint64` field nevertheless retained a 24-byte pointer/length/capacity
header in every container, including sparse array containers. The final layout
stores a pointer to a fixed `[1024]uint64` backing instead. This reduces the
container from 64 to 48 bytes while leaving the 8 KiB dense allocation and all
array storage unchanged. Snapshot restore and array/bitmap conversion allocate
the same backing bytes as before.

A behavior test covers array-to-bitmap promotion, lookup, snapshot round-trip,
bitmap-to-array demotion, and exact header size. A same-binary control copies
the prior slice layout and identical add, remove, contains, and conversion
logic; each measured pair alternates execution order.

```sh
make run CMD='go test . -run RoaringBitmap -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRoaringBitmapBackingLayoutAlternating -benchtime=500x -count=9 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRoaringBitmapContainerRetained50k -benchtime=1x -count=5 -cpu=1 -benchmem'
```

| Operation, median | Slice-header control | Fixed-pointer final | Improvement |
| --- | ---: | ---: | ---: |
| Build 4,097-value bitmap | 200.967 us | 195.506 us | 1.03x faster |
| Bitmap lookup | 3.287 ns | 3.013 ns | 1.09x faster |
| Bitmap remove/add | 4.953 ns | 4.922 ns | 1.006x faster |
| Build 50,000 singleton containers | 14.510 ms; 17,470,672 B; 50,026 allocs | 10.762 ms; 14,153,680 B; 50,026 allocs | 1.35x faster; 1.23x lower cumulative heap; allocations unchanged |
| Retained singleton layout | 80.75 B/container | 66.66 B/container | 1.21x lower retained heap |

The bitmap size was already fixed by validation, conversion thresholds, and
the persistent format, so the pointer does not narrow accepted state. Wire,
snapshot, journal, database, ordering, and public API behavior are unchanged.
No operation regression was measured.

A follow-up converted the fixed pointer to a local slice view in dense
operations. Build and lookup remained faster, but the nine-run paired
remove/add median was 4.594 ns versus 4.486 ns for the legacy slice, or 1.024x
slower. It was reverted. A longer audit of the retained direct-pointer form
measured 4.922 versus 4.953 ns, so the shipped representation remains neutral
to slightly faster on mutation. No follow-up code remains.

<a id="roaring-field-order-compaction-rollback"></a>
#### Roaring Field-Order Compaction Rollback

A later alignment audit reordered the four existing container fields without
changing their types. Values-first order removed eight bytes of alignment
padding, reducing the header from 48 to 40 bytes. On 50,000 singleton
containers, the 40-byte layout reduced retained memory from 66.65 to 54.86
B/container, reduced cumulative heap from 14,153,680 to 11,368,736 bytes, and
reduced median construction time from 11.907 to 10.761 ms. That is 1.21x lower
retained heap, 1.24x lower cumulative heap, and a 1.11x faster sparse build,
with the same 50,026 allocations.

The layout was not operation-neutral. A same-binary alternating control copies
the 40-byte representation and identical add, conversion, lookup, and dense
mutation logic. Two nine-run confirmations made its 4,097-value dense build
1.018x and 1.044x slower than the 48-byte production order. The latest medians
were 181,579 versus 173,943 ns/build. Dense lookup and remove/add were neutral,
while a pointer-first 40-byte variant instead made lookup 1.039x slower.

```sh
make run CMD='go test . -run RoaringBitmap -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRoaringBitmapFieldOrderAlternating -benchtime=500x -count=9 -cpu=1'
```

The production field order was restored. The test-only 40-byte control remains
to prevent repeating the experiment; it has no runtime memory, CPU, wire,
persistence, or behavior cost.

<a id="inline-roaring-container-rollback"></a>
#### Inline Roaring-Container Rollback

Roaring containers also have four trailing padding bytes. An experiment used
them for the first two `uint16` values, promoted at the third value, and
demoted after removal. Behavior, snapshot round-trip, encoded-size, and
64-byte layout tests passed. Same-binary controls retained the slice-backed
implementation and alternated lookup timing to expose representation costs.

```sh
make run CMD='go test . -run RoaringBitmap -count=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRoaringBitmapInlineRetained50k -benchtime=1x -count=5 -cpu=1 -benchmem'
make run CMD='go test . -run=NONE -bench=BenchmarkRoaringBitmapPromotedContainsAlternating -benchtime=1000x -count=7 -cpu=1'
```

For 50,000 singleton containers, the five-run median improved from 11.749 ms
to 8.488 ms, or 1.38x. Retained memory fell from 80.75 to 72.75 B/container,
retained objects fell from about 0.5001 to 0.000040/container, and build
allocations fell from 50,027 to 27. The required representation branch was not
free, however: a paired 16-value lookup measured 5.139 versus 4.955 ns, or
1.04x slower. The 4,096-value array and bitmap controls were also roughly
1.01x-1.02x slower. Special-casing three values recovered that layout but
made the larger controls worse. The complete candidate and its tests were
removed; it adds no runtime cost.

The reservoir sample add path now has a plain-string fast path that hashes the
JSON string representation directly and only boxes retained values. The focused
1,000,000-iteration row was:

```
make bench-hatrie-command-features HATRIE_COMMAND_BENCH='^BenchmarkCommandFeature/ReservoirSampleAdd$' BENCHTIME=1000000x
```

| Feature | Benchmark row | Before | After | Improvement |
| --- | --- | ---: | ---: | ---: |
| Reservoir sample add | `BenchmarkCommandFeature/ReservoirSampleAdd` | 956.7 ns, 168 B, 6 allocs | 465.3 ns, 64 B, 1 alloc | 2.06x faster, 2.63x less memory, 6.00x fewer allocs |

<a id="reservoir-sample-read-materialization"></a>
### Reservoir Sample Read Materialization

`GETRS` previously copied the internal heap into a temporary private slice,
used reflected sorting, copied and cloned it again into the public item slice,
and then passed that slice through the generic JSON encoder. A 16-item read
spent 47% of sampled CPU below `Items()`, 41% below JSON encoding, and 26% below
reflected sorting; these cumulative profile percentages overlap. The allocation
profile attributed 49.1% of bytes to the required returned JSON, 22.8% to the
private sorted copy, 21.8% to the public copy, and 3.4% to reflected sorting.

The public `Items()` path now clones directly into its final slice and uses a
typed priority/sequence sorter. Exact `GETRS`, `RSGET`, and `SAMPLE` commands
retain one sorted copy and write the existing JSON shape directly with stack
numeric buffers. All-verbatim strings keep their original specialized writer;
escaped strings and structured values write into the same response buffer
without allocating and cloning a public item slice. Sorting and copying remain
under the cache mutex, as before, but JSON encoding occurs after unlock. Cold
LevelDB references retain the checked generic hydration path.

Tests were added before the implementation and cover exact byte parity, all
aliases, ignored request fields, missing and empty samples, priority/sequence
ties, escaped and structured values, cache counters, nested clone ownership,
encoded sizes, and existing snapshot behavior. No sample layout, retained
state, command schema, snapshot, journal, replication, storage, or wire format
changed.

The baseline and final command rows are seven fixed one-million-operation runs.
The phase and encoded-value controls run both routes in one binary with the
generic command forced by allocation-free whitespace normalization. The write
control alternates precompiled baseline `e92fb51` and candidate binaries on CPU
0 for seven fixed one-million-operation runs.

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/ReservoirSampleGet -benchmem -benchtime=1000000x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkReservoirSampleGetPath -benchmem -benchtime=1000000x -count=7 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkReservoirSampleGetEncodedPath -benchmem -benchtime=1000000x -count=7 -cpu=1'
```

| 16-item read, median | Time/op | Bytes/op | Allocs/op | Improvement from baseline |
| --- | ---: | ---: | ---: | ---: |
| Baseline generic materialization | 3,910 ns | 2,336 B | 8 | - |
| One-pass typed generic control | 2,754 ns | 1,744 B | 5 | 1.42x faster; 1.34x lower heap; 1.60x fewer allocations |
| Final exact plain-string command | 2,323 ns | 1,688 B | 3 | 1.68x faster; 1.38x lower heap; 2.67x fewer allocations |

The same-binary direct encoder median is 2,082 ns versus 2,754 ns for the
already optimized generic path, a further 1.32x CPU improvement. The
alternating write control measured 348.6 ns for the candidate versus 394.3 ns
for the baseline, while both remained at 64 B and one allocation; this is
treated as evidence of no write regression rather than a claimed write-path
gain because the write implementation is unchanged.

Exact uppercase generic `GET` now recognizes reservoir samples under the
existing shared read lock, sorts one private copy, unlocks, and uses the same
direct writer. Eleven 500 ms runs with `-cpu=1` produced these same-binary
medians:

```sh
make run CMD='go test . -run=NONE -bench BenchmarkReservoirSampleGenericGetCommand/.+16 -benchmem -benchtime=500ms -count=11 -cpu=1'
make run CMD='go test . -run=NONE -bench BenchmarkReservoirSampleGenericGetCommand/.+128 -benchmem -benchtime=500ms -count=11 -cpu=1'
```

| Exact generic `GET`, median | Generic control | Direct | Improvement |
| --- | ---: | ---: | ---: |
| 16 strings | 2,709 ns; 1,744 B; 5 allocs | 2,225 ns; 1,688 B; 3 allocs | 1.22x faster; 1.03x lower heap; 1.67x fewer allocs |
| 128 strings | 18,349 ns; 14,416 B; 5 allocs | 17,563 ns; 14,360 B; 3 allocs | 1.04x faster; 1.004x lower heap; 1.67x fewer allocs |
| 16 items, one structured | 2,941 ns; 2,216 B; 10 allocs | 2,701 ns; 1,808 B; 5 allocs | 1.09x faster; 1.23x lower heap; 2.00x fewer allocs |
| 128 items, one structured | 18,861 ns; 14,889 B; 10 allocs | 18,275 ns; 14,481 B; 5 allocs | 1.03x faster; 1.03x lower heap; 2.00x fewer allocs |

The escaped 16-item dedicated control was run in isolated exact-first and
generic-second processes after a combined run showed time-order drift. Exact
was 2,691 ns, 1,816 B, and 3 allocations versus 2,863 ns, 1,872 B, and 5
allocations: 1.06x faster, 1.03x lower heap, and 1.67x fewer allocations.

An intermediate exact-capacity candidate scanned every escaped string once to
count output bytes and again while writing. Its isolated exact median was 3,489
ns versus 2,707 ns generic, a 1.29x regression despite lower allocation. It was
removed. The retained layout checks all reservation arithmetic without the
second full escape scan; `strings.Builder` grows from that bounded reservation
only when encoded bytes exceed raw bytes. No retained state, configuration,
background work, format, or public ownership rule changed.

A later lock-scope candidate kept sizing and the required private copy under
the cache lock but moved the existing typed sort after unlock. It added no
memory, allocation, representation, or output change, but failed the serial
gate in seven 500 ms runs on one logical CPU. Default-capacity generic reads
regressed from 17,065 to 18,075 ns for strings and from 18,301 to 19,377 ns for
one structured value, both 1.06x slower with identical 14,360/14,481 B and
three/five allocations. The dedicated 16-item plain-string read regressed from
1,971 to 2,163 ns, or 1.10x, with the same 1,688 B and three allocations. The
candidate was fully reverted; reservoir copying and sorting therefore retain
their previous lock scope.

<a id="multi-item-top-k-read-materialization"></a>
### Multi-Item Top-K Read Materialization

Multi-item `GETTOPK` previously copied and reflection-sorted the internal heap,
allocated and cloned a second public item slice, then serialized that public
slice through the generic JSON encoder. The one-item special case was already
direct, so the former command benchmark did not exercise this multi-item cost.

Tests and realistic 16/default-100-item benchmarks were added before the
implementation. They require exact byte parity with generic JSON for plain,
escaped, Unicode, HTML-sensitive, and structured values and preserve
count/error/key tie ordering. The implementation sorts one private copy through
a typed standard-library sorter and writes canonical matching string keys plus
numeric fields into one exactly sized response buffer. Structured values are
encoded directly from their current stored value into that buffer instead of
allocating, cloning, and serializing a second public item slice. Public
`Items()` and snapshots retain their ownership-preserving clones.

The baseline and final rows are medians from seven fixed 10,000-operation runs
on the same Ryzen 9 5950X host. A leading-space command selects the generic
control without changing command semantics.

```sh
make run CMD='go test . -run=none -bench=BenchmarkTopKGet -benchmem -benchtime=10000x -count=7'
make run CMD='go test . -run=none -bench=BenchmarkCommandFeature/TopK -benchmem -benchtime=100000x -count=7'
```

| Read path, median | Baseline | Final | Improvement |
| --- | ---: | ---: | ---: |
| Exact command, 16 strings | 2,851 ns; 2,297 B; 8 allocs | 1,851 ns; 1,624 B; 3 allocs | 1.54x faster; 1.41x lower heap; 2.67x fewer allocs |
| Exact command, 100 strings | 10,558 ns; 13,516 B; 8 allocs | 6,898 ns; 9,752 B; 3 allocs | 1.53x faster; 1.39x lower heap; 2.67x fewer allocs |
| Generic control, 16 strings | 2,878 ns; 2,297 B; 8 allocs | 2,606 ns; 2,200 B; 6 allocs | 1.10x faster; 1.04x lower heap; 1.33x fewer allocs |
| Generic control, 100 strings | 10,859 ns; 13,512 B; 8 allocs | 10,317 ns; 13,394 B; 6 allocs | 1.05x faster; 1.01x lower heap; 1.33x fewer allocs |
| Structured fallback, 16 items | 3,482 ns; 2,512 B; 11 allocs | 3,133 ns; 2,414 B; 9 allocs | 1.11x faster; 1.04x lower heap; 1.22x fewer allocs |

An attempted one-item rewrite reduced heap but made its median CPU time 6%
slower, so it was discarded and the prior one-item code was restored. The final
one-item path remains at 80 B and one allocation. At that stage `ADDTOPK` was
72 B and three allocations; later scalar and inline-duplicate passes are
reported below. Their implementations, retained state, ordering,
storage, snapshots, journal, replication, and wire JSON shape are unchanged.

A later Go 1.26 candidate replaced the typed `sort.Interface` adapter with
`slices.SortFunc`. Ordering and parity tests passed ten times. Nine alternating
300 ms A/B pairs used otherwise identical precompiled test binaries pinned to
one CPU:

| Read path, nine-run median | `sort.Interface` | `slices.SortFunc` | Result |
| --- | ---: | ---: | ---: |
| Exact command, 16 strings | 2,240 ns; 1,624 B; 3 allocs | 2,407 ns; 1,600 B; 2 allocs | 1.07x slower |
| Exact command, 100 strings | 8,108 ns; 9,752 B; 3 allocs | 9,099 ns; 9,728 B; 2 allocs | 1.12x slower |
| Generic control, 16 strings | 3,138 ns; 2,192 B; 6 allocs | 3,290 ns; 2,168 B; 5 allocs | 1.05x slower |
| Generic control, 100 strings | 12,444 ns; 13,264 B; 6 allocs | 13,711 ns; 13,240 B; 5 allocs | 1.10x slower |

The generic sorter removed one interface allocation and 24 transient bytes,
but regressed every complete read path. It was removed, restoring the faster
adapter and leaving no runtime or compatibility tradeoff.

Exact uppercase generic `GET` now recognizes Top-K values under the existing
shared read lock and uses the same direct encoder as `GETTOPK`. The first
candidate retained a string-only helper and fell back to `Items()` after a
failed eligibility scan. Its 16-item structured median was 3,673 ns versus
3,468 ns for the unchanged generic control, a 1.06x regression with identical
2,664 B and 11 allocations, so that candidate was removed. The retained
one-pass mixed encoder avoids the scan/fallback pair and does not trust a stale
canonical key for caller-owned custom values: matching strings use their
already encoded key, while every other value is encoded from current stored
state.

Seven one-second paired runs produced these medians:

```sh
make run CMD='go test . -run=NONE -bench BenchmarkTopKGenericGetCommand -benchmem -benchtime=1s -count=7'
make run CMD='go test . -run=NONE -bench "BenchmarkTopKGet(Path|StructuredFallbackPath)" -benchmem -benchtime=1s -count=7'
```

| Exact generic `GET`, median | Generic control | Direct | Improvement |
| --- | ---: | ---: | ---: |
| 16 strings | 2,376 ns; 2,200 B; 6 allocs | 1,660 ns; 1,624 B; 3 allocs | 1.43x faster; 1.35x lower heap; 2.00x fewer allocs |
| 100 strings | 13,936 ns; 13,444 B; 6 allocs | 10,024 ns; 9,752 B; 3 allocs | 1.39x faster; 1.38x lower heap; 2.00x fewer allocs |
| 16 items, one structured | 3,019 ns; 2,680 B; 11 allocs | 2,148 ns; 1,754 B; 5 allocs | 1.41x faster; 1.53x lower heap; 2.20x fewer allocs |
| 100 items, one structured | 13,966 ns; 13,956 B; 11 allocs | 10,263 ns; 9,931 B; 5 allocs | 1.36x faster; 1.41x lower heap; 2.20x fewer allocs |

The exact generic `GET` originally kept its shared cache lock while sorting and
encoding the private Top-K copy. That made a caller-owned `MarshalJSON` method
part of the lock hold: a deterministic one- and multi-item test showed a writer
could not complete during the full 100 ms observation gate and would remain
blocked until the marshaler was released. The retained path now performs the
bounded key/count sizing pass and private item copy under the shared lock,
records the read, then unlocks before sorting and encoding. A one-item direct
encoder preserves the original one-allocation helper contract.

The serial comparison runs both orders to cancel host drift. The rows below are
pooled medians from five 500 ms samples per order on one logical CPU:

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkGenericTopKReadLockScope -benchmem -benchtime=500ms -count=5 -cpu=1'
```

| Exact generic `GET` lock scope | Lock-held encoding | Snapshot then encode | Result |
| --- | ---: | ---: | ---: |
| 16 strings | 2,023 ns; 1,624 B; 3 allocs | 1,988 ns; 1,624 B; 3 allocs | 1.02x faster; memory unchanged |
| 100 strings | 13,713 ns; 9,752 B; 3 allocs | 13,598 ns; 9,752 B; 3 allocs | 1.01x faster; memory unchanged |
| 16 items, one structured | 2,486 ns; 1,744 B; 5 allocs | 2,469 ns; 1,744 B; 5 allocs | 1.01x faster; memory unchanged |
| 100 items, one structured | 16,561 ns; 9,872 B; 5 allocs | 16,036 ns; 9,872 B; 5 allocs | 1.03x faster; memory unchanged |

Applying the same lock release to dedicated `GETTOPK` also let the writer
progress, but failed the serial gate. Its five-second 100-item structured run
was 14,457 ns versus 13,776 ns for the legacy lock-held path, 1.05x slower,
with the same 9,872 B and five allocations. CPU profiles showed no compensating
work or memory reduction, so the dedicated candidate was reverted and remains
listed in the rejected index. Only the neutral generic `GET` improvement is
retained.

The shared dedicated-command controls improved 1.49x for 16 strings, 1.43x
for 100 strings, and 1.46x for the 16-item structured fixture. The change adds
no retained state, configuration, background work, wire field, format version,
or public value alias. Cold references and nonexact command spellings retain
their checked generic paths.

<a id="lazy-small-top-k-indexes"></a>
### Lazy Small Top-K Indexes

Every nonempty Top-K sketch previously allocated a Go `map[string]int`, even
when only one or two values were tracked. Measured linear lookup is faster at
those cardinalities, while map lookup wins from the fourth item. Top-K now
searches its existing item slice directly through two values and constructs the
same map immediately before inserting a third distinct value. Capacity-one and
capacity-two sketches remain inline through replacement; there is no retained
side metadata or demotion work.

Tests were added before implementation and cover inline duplicate updates,
heap swaps, capacity-two eviction, zero-count estimates, batch promotion,
map-index consistency, and one/two/three-item snapshot restoration. The
plain-string update path also checks for a duplicate before constructing its
boxed retained item, removing one transient allocation at every cardinality.
Snapshot, storage, journal, replication, command, and wire formats are
unchanged.

The retained-memory rows use 100,000 default-capacity sketches and medians from
three runs of five complete builds. Operation rows are seven fixed one-million-
operation runs; complete commands use seven fixed 100,000-operation runs on the
same Ryzen 9 5950X host.

```sh
make run CMD='go test . -run=none -bench=BenchmarkTopKSmallIndexMemory -benchtime=5x -count=3'
make run CMD='go test . -run=none -bench=BenchmarkTopKSmallIndexLifecycle -benchmem -benchtime=1000000x -count=7'
make run CMD='go test . -run=none -bench=BenchmarkTopKSmallIndexDuplicate -benchmem -benchtime=1000000x -count=7'
make run CMD='go test . -run=none -bench=BenchmarkTopKSmallIndexEstimate -benchmem -benchtime=1000000x -count=7'
make run CMD='go test . -run=none -bench=BenchmarkTopKSmallIndexCommand -benchmem -benchtime=100000x -count=7'
```

| Retained representation | Eager map | Lazy index | Improvement |
| --- | ---: | ---: | ---: |
| One item | 384 B; 5 objects/sketch | 128 B; 3 objects/sketch | 3.00x lower heap; 1.67x fewer objects |
| Two items | 464 B; 7 objects/sketch | 208 B; 5 objects/sketch | 2.23x lower heap; 1.40x fewer objects |
| Three items after promotion | 592 B; 9 objects/sketch | 592 B; 9 objects/sketch | Unchanged |

| Operation, median | Eager map | Lazy index | Improvement |
| --- | ---: | ---: | ---: |
| Build one item | 282.5 ns; 336 B; 5 allocs | 108.0 ns; 80 B; 3 allocs | 2.62x faster; 4.20x lower heap; 1.67x fewer allocs |
| Build two items | 457.6 ns; 464 B; 8 allocs | 235.8 ns; 208 B; 6 allocs | 1.94x faster; 2.23x lower heap; 1.33x fewer allocs |
| Build three items and promote | 677.0 ns; 688 B; 11 allocs | 538.7 ns; 688 B; 11 allocs | 1.26x faster; heap and allocations unchanged |
| Duplicate update, one/two/three/16 items | 73.19/74.91/82.63/86.34 ns; 32 B; 2 allocs | 39.30/40.62/44.43/48.65 ns; 16 B; 1 alloc | 1.84x-1.86x faster; half the heap and allocations |
| Direct estimate, one/two/three/16 items | 11.11/12.32/12.31/14.62 ns | 5.616/8.429/11.58/12.86 ns | 1.98x/1.46x/1.06x/1.14x faster; zero allocation throughout |
| Complete duplicate `ADDTOPK`, one/two/three/16 items | 428.4/420.7/433.7/436.3 ns; 80 B; 3 allocs | 364.4/369.3/366.9/380.0 ns; 64 B; 2 allocs | 1.14x-1.18x faster; 1.25x lower heap; 1.50x fewer allocs |

A follow-up removes the remaining quoted-key allocation for duplicate
plain-string updates while the index is inline. It compares each stored
canonical key to virtual leading/trailing quotes around the command value;
only a miss constructs the canonical key that must be retained. Promoted
map-backed sketches keep their prior allocated lookup. Tests compare generic
and fast snapshots, estimates, zero-count reads, misses, and index state at
one, two, three, and 16 items.

```sh
make run CMD='go test . -run=TestTopKPlainJSONStringDuplicateMatchesGenericLayouts -count=1 -v'
make run CMD='go test . -run=NONE -bench=BenchmarkTopKPlainJSONStringDuplicateLookup -benchmem -benchtime=1000000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkCommandFeature/TopKAdd -benchmem -benchtime=1000000x -count=7 -cpu=1'
```

| Duplicate plain-string update, median | Allocated-key control | Virtual inline lookup | Improvement |
| --- | ---: | ---: | ---: |
| One tracked item | 37.06 ns; 16 B; 1 alloc | 12.48 ns; 0 B; 0 allocs | 2.97x faster; all transient heap removed |
| Two tracked items | 45.40 ns; 16 B; 1 alloc | 12.94 ns; 0 B; 0 allocs | 3.51x faster; all transient heap removed |
| Three tracked items, promoted | 41.25 ns; 16 B; 1 alloc | 40.85 ns; 16 B; 1 alloc | 1.01x faster; memory unchanged |
| 16 tracked items, promoted | 45.04 ns; 16 B; 1 alloc | 44.92 ns; 16 B; 1 alloc | Neutral within 0.3%; memory unchanged |
| Complete default one-item `ADDTOPK` | 316.7 ns; 56 B; 2 allocs | 262.5 ns; 48 B; 1 alloc | 1.21x faster; 1.17x lower heap; half the allocations |

No field, retained object, wire byte, persistent format, command response, or
configuration changed.

An initial helper-based lookup candidate made isolated map-backed estimates
1.62x-1.88x slower and was discarded. The retained cardinality branch makes
all direct estimate sizes faster. Complete `ESTTOPK` at two and three items is
neutral within 0.6%, while 16 items improves 0.8%; `GETTOPK` does not inspect
the index and retains identical code and allocation counts. No configuration,
fixed overhead, background work, or persistent-format tradeoff was added.

<a id="lazy-rate-limiter-shard-maps"></a>
### Lazy Rate-Limiter Shard Maps

Enabling the optional API rate limiter previously allocated an empty Go map in
each of its 64 lock shards. A limiter with no callers therefore performed 65
allocations and retained all map headers. Shards now leave the map nil until a
caller hashes to them; nil-map reads remain valid, and the cold new-client path
creates the map before its first write.

The test-first lifecycle fixture requires all maps to be absent after
construction, admits one client, and proves that exactly its selected shard is
initialized. Existing token refill, rejection, clock rollback, and bounded
high-cardinality tests retain the same behavior. The hot established-client
branch now returns directly; pruning runs only after inserting a new client,
the only operation that can increase shard cardinality.

```sh
make run CMD='go test . -run=TestRateLimiter -count=10'
make run CMD='go test . -run=NONE -bench=BenchmarkRateLimiterConstruction -benchmem -benchtime=100000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRateLimiterFirstClientLifecycle -benchmem -benchtime=100000x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRateLimiterAllowSameClientAlternating -benchtime=10x -count=10 -cpu=1'
make run CMD='go test . -run=NONE -bench=BenchmarkRateLimiterAllShardsLifecycle -benchmem -benchtime=10000x -count=10 -cpu=1'
```

| Rate-limiter lifecycle, ten-run median | Eager 64 maps | Lazy shards | Improvement |
| --- | ---: | ---: | ---: |
| Construction only | 2,540 ns; 4,224 B; 65 allocs | 269.5 ns; 1,152 B; 1 alloc | 9.42x faster; 3.67x lower heap; 65x fewer allocations |
| Construction plus first client | 2,683.5 ns; 4,640 B; 66 allocs | 414.9 ns; 1,616 B; 3 allocs | 6.47x faster; 2.87x lower heap; 22x fewer allocations |
| Established-client admission | 34.09 ns; 0 B; 0 allocs | 33.88 ns; 0 B; 0 allocs | 1.006x faster; memory unchanged |
| Construction plus one client in all 64 shards | 11,269.5 ns; 30,848 B; 129 allocs | 11,289.5 ns; 30,848 B; 129 allocs | CPU-neutral within 0.2%; memory unchanged |

The steady-state admission row is an alternating same-binary control; separate
old/final medians were also neutral at 34.78 and 34.64 ns. A shard pays its map
allocation once when its first caller arrives, but the complete first-client
lifecycle already includes that work and remains substantially cheaper. Once
every shard is active, cumulative heap and allocations exactly match the eager
control without a measurable CPU regression. Lock sharding, token
capacity/refill, client-state bounds, configuration, HTTP/gRPC behavior,
metrics, and responses are unchanged.

### On-Demand Runtime Profiling

Runtime profiling is an operator-only diagnostic feature and defaults off. The
disabled handler does not register or dispatch `/api/profile`; CPU, heap, and
goroutine work begins only after an authenticated request. Measurements were
taken on the same Ryzen 9 5950X host with Go's benchmark harness:

```sh
make run CMD="go test -run=NONE -bench=BenchmarkMonitoringProfilingDisabled -benchmem -benchtime=500000x -count=30 ."
make run CMD="go test -run=NONE -bench=BenchmarkCommandTransportFeature/InProcess/StringGet -benchmem -benchtime=100000x -count=10 ."
make run CMD="env GOMAXPROCS=1 go test -run=NONE -bench=BenchmarkMonitoringCPUProfileCommandOverhead -benchmem -benchtime=3s -count=5 ."
make run CMD="go test -run=NONE -bench=BenchmarkMonitoringProfileCapture -benchmem -benchtime=1x -count=5 ."
```

Disabled-mode medians stayed within benchmark noise and retained the exact
allocation counts:

| Guard | Before | After | Change | Before alloc | After alloc |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disabled profile route | 434.2 ns/op | 434.6 ns/op | 1.00x, 0.08% slower | 48 B, 3 allocs/op | 48 B, 3 allocs/op |
| In-process `GET` command | 386.3 ns/op | 385.7 ns/op | 1.00x, 0.17% faster | 0 B, 0 allocs/op | 0 B, 0 allocs/op |

During an active CPU profile, the single-core in-process `GET` median moved
from 203.6 ns/op to 206.2 ns/op, a 1.3% capture-window cost, while remaining at
0 B/op and 0 allocs/op. That cost ends when `StopCPUProfile` completes.

The following one-shot figures describe capture cost, not retained idle heap.
Profile bytes are already in Go's compressed pprof representation and vary with
live heap size, goroutine count, stack diversity, and sampled workload:

| Capture | Median elapsed | Cumulative allocation | Allocations | Output |
| --- | ---: | ---: | ---: | ---: |
| CPU, 1 second idle fixture | 1.001 s | 2.32 MiB | 151 | 438 B |
| Heap, small fixture | 0.642 ms | 1.30 MiB | 542 | 3,102 B |
| Goroutine, small fixture | 0.416 ms | 1.23 MiB | 381 | 1,518 B |

The operational bounds are one capture per node, 1-30 seconds for CPU, and a
256 MiB server/client stream cap. Support-bundle capture runs nodes concurrently
but profile types sequentially per node, avoiding both cluster-duration growth
and same-node profiler contention. Block and mutex profiling remain disabled
because their continuous instrumentation would impose standing cost.

<!-- BEGIN GENERATED COMMAND BENCHMARK COMPARISON -->
## Memory Summary

| System | Run | Memory metric | Value |
| --- | --- | --- | ---: |
| HAT-trie cache | comparable command benchmark, Go test binary | max resident set size | 30,140 KiB |
| HAT-trie cache | HyperLogLog command benchmark, Go test binary | max resident set size | 27,692 KiB |
| Tarantool 2.6.0 | 1,000,000 feature cycles, 10,000 keyspace | process RSS | 35,484 KiB |
| Tarantool 2.6.0 | 1,000,000 feature cycles, 10,000 keyspace | slab quota used | 32,768 KiB |
| Tarantool 2.6.0 | 1,000,000 feature cycles, 10,000 keyspace | slab items used | 1,519 KiB |
| Redis 7.0.4 | 10,000 requests, 10,000 keyspace | used_memory | 2,494,304 B |
| Redis 7.0.4 | 10,000 requests, 10,000 keyspace | used_memory_rss | 8,716,288 B |
| Redis 7.0.4 | 10,000 requests, 10,000 keyspace | used_memory_peak | 3,171,296 B |

HAT-trie memory is the benchmark test process RSS, so it includes the Go runtime
and test harness. Redis memory is server-reported memory from `INFO memory`.
Tarantool memory is `/proc/self/status` RSS plus `box.slab.info()` values.

## HAT-trie vs Tarantool

Tarantool was measured with `requests=1000000` and `keyspace=10000`.
HAT-trie was measured with the matching `BenchmarkCommandFeature/*` rows at
`BENCHTIME=1000000x`.

| Feature family | HAT-trie benchmark | HAT-trie seconds / 10k | HAT alloc/op | Tarantool measured operation | Tarantool seconds / 10k | Tarantool/HAT speedup |
| --- | --- | ---: | ---: | --- | ---: | ---: |
| String write | `BenchmarkCommandFeature/StringSet` | 0.004832 s | 8 B/op | `space:replace()` | 0.010344 s | 2.14x |
| String read | `BenchmarkCommandFeature/StringGet` | 0.002620 s | 0 B/op | `space.index.primary:get()` | 0.005154 s | 1.97x |
| Integer counter | `BenchmarkCommandFeature/CounterInc` | 0.005221 s | 7 B/op | `space:update({{"+", 2, 1}})` | 0.013184 s | 2.53x |
| TTL update | `BenchmarkCommandFeature/TTLExpire` | 0.007290 s | 99 B/op | `space:update({{"=", 3, expires_at}})` | 0.016871 s | 2.31x |
| Map/hash write | `BenchmarkCommandFeature/MapPut` | 0.003636 s | 16 B/op | `space:replace({key, field, value})` | 0.007924 s | 2.18x |
| Map/hash read | `BenchmarkCommandFeature/MapPeek` | 0.002713 s | 0 B/op | `space.index.primary:get({key, field})` | 0.025777 s | 9.50x |
| List/deque push+pop | `BenchmarkCommandFeature/SlicePushPop` | 0.006425 s | 16 B/op | `space:replace() + space:delete()` | 0.014026 s | 2.18x |
| Set add+has | `BenchmarkCommandFeature/SetAddHas` | 0.012380 s | 112 B/op | `space:replace() + space.index.primary:get()` | 0.021243 s | 1.72x |
| Priority queue push+pop | `BenchmarkCommandFeature/PriorityQueuePushPop` | 0.019410 s | 168 B/op | `tree index insert + index:min() + delete` | 0.038649 s | 1.99x |
| Roaring bitmap add | `BenchmarkCommandFeature/RoaringAdd` | 0.004360 s | 4 B/op | `space:replace() membership index` | 0.007246 s | 1.66x |
| Roaring bitmap lookup | `BenchmarkCommandFeature/RoaringHas` | 0.002793 s | 0 B/op | `space.index.primary:get() membership index` | 0.019767 s | 7.08x |
| Sparse uint64 bitset add | `BenchmarkCommandFeature/SparseBitsetAdd` | 0.004545 s | 8 B/op | `space:replace() membership index` | 0.008197 s | 1.80x |
| Sparse uint64 bitset lookup | `BenchmarkCommandFeature/SparseBitsetHas` | 0.002811 s | 0 B/op | `space.index.primary:get() membership index` | 0.010770 s | 3.83x |
| Radix-tree put | `BenchmarkCommandFeature/RadixPut` | 0.003131 s | 16 B/op | `space:replace() tree string key` | 0.010393 s | 3.32x |
| Radix-tree prefix scan | `BenchmarkCommandFeature/RadixPrefix` | 0.032430 s | 1,468 B/op | `index:pairs(prefix, {iterator = "GE"})` | 0.189574 s | 5.85x |
| Replication dump | `BenchmarkCommandFeature/ReplicationDump` | 0.004782 s | 64 B/op | `msgpack.encode(tuple)` | 0.040829 s | 8.54x |

In this run HAT-trie is faster on all 16 measured Tarantool-comparable rows.

## Replication Batching Benchmark

Run:

```sh
make bench-replication-optimizations \
  BENCHTIME=20x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-final.txt
```

The Make target runs the splitter, the
`BenchmarkHTTPReplicatorSyncAllBatching` 10,000-key end-to-end sync, digest
repair, and fallback iterator benchmarks. Raw output is written to
`build/benchmarks/replication-final.txt`. `Batched10k` uses one SyncAll page,
native protobuf replication, and one local HTTP target. The latest final row is
the median of ten runs with `-benchtime=20x` on the same AMD Ryzen 9 5950X host.
The three feature rows use the paired seven-run commands recorded during each
change. Older rows are retained from their original controlled runs and are not
directly comparable to the latest pass.

| Mode | Time/op | requests/op | wire_B/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before optimization (`b897b64`) | 162,195,812 ns | 1 | 144,227 | 57,035,706 | 1,040,310 |
| Start of this pass (`10cf4c8`) | 39,010,494 ns | 1 | 87,095 | 11,982,835 | 131,264 |
| Previous optimized (`c70d849`) | 31,830,774 ns | 1 | 55,792 | 5,668,513 | 50,753 |
| Start of latest pass (`84325af`) | 28,554,528 ns | 1 | 55,794 | 5,781,645 | 50,756 |
| Final optimized (`69a6018`) | 18,893,092 ns | 1 | 55,795 | 948,495 | 30,197 |
| Current optimized (`e5b127d`) | 15,698,676 ns | 1 | 55,794 | 847,763 | 10,241 |
| Before descriptor optimization (`0f4adc3`) | 24,269,975 ns | 3 | 56,041 | 16,386,600 | 14,277 |
| Zero-copy split (`20cdd2f`) | 21,743,638 ns | 3 | 56,038 | 4,888,410 | 10,743 |
| Compact default digest payloads (`8cb0e0d`) | 19,835,164 ns | 3 | 56,045 | 2,752,297 | 10,639 |
| Current combined (`375635e`) | 19,370,011 ns | 3 | 56,045 | 2,749,032 | 10,635 |
| Direct fallback repair collection (`3562273`) | 18,557,453 ns | 3 | 56,046 | 1,260,223 | 10,609 |
| Direct digest value arena (`fe06238`) | 18,562,257 ns | 3 | 56,046 | 1,019,337 | 610 |
| Capability cache, one-shard route, native radix (`5ef34af`) | 12,909,866 ns | 2.05 | 55,894 | 972,342 | 430 |
| Prevalidated scan scope (`c17afc9`) | 10,198,038 ns | 2.01 | 55,886 | 960,905 | 417 |
| Historical unbatched 10k | 51,455,645,995 ns | 10,000 | 2,135,564 | 1,794,046,848 | 202,050,916 |

Against `0f4adc3`, the current final result is 1.31x faster, uses 16.08x less
cumulative allocated heap, and performs 23.40x fewer allocations. Against
`375635e`, the two latest changes are 1.04x faster, use 2.70x less heap, and
perform 17.43x fewer allocations. Request count and request-body bytes are
unchanged. The historical batching request reduction is 10,000x for this
single-target sync. Header bytes are not included
in `wire_B/op`, and `B/op` measures cumulative bytes allocated during one
operation rather than peak process RSS.

The historical five-feature pass ending at `e5b127d` was 1.51x faster and used
6.10x less cumulative allocated heap than its recorded start.

The latest three-feature pass started from a fresh paired median of 26,378,608
ns, 3 requests, 56,046 wire B, 1,003,978 heap B, and 608 allocations. Its final
end-to-end median is 2.04x faster, uses 1.03x less cumulative heap, and performs
1.41x fewer allocations. The measured 2.05 requests/op includes the first
unsupported-digest probe across each 20-iteration sample; steady state is two
requests. Modern digest-capable targets remained at one request and 215 wire B,
with a neutral 14.916 ms before versus 14.909 ms after capability caching.

### CLI Redirect Credential Isolation

The authenticated CLI transport previously added its bearer token to every
HTTP round trip. A redirect therefore received the token even when its scheme,
host, or port differed from the original request. A test using two local
origins failed first with `Bearer operator-secret` observed at the redirected
destination. The retained implementation marks cross-origin redirect requests
as unauthenticated and removes operator and replication credential headers;
same-origin redirects keep the token.

The ordinary non-redirect path was measured for ten `100000x` runs before and
after the change. Median cost was 464.75 ns/op before and 464.45 ns/op after;
both retained 904 B/op and 6 allocs/op. The 0.06% timing difference is noise,
so the hardening has no measured steady-request CPU or memory cost. The only
compatibility change is intentional: an API redirected to another origin must
authenticate that origin independently.

Reproduce the focused measurement with:

```sh
make run CMD="go test ./cmd/hatrie-cli -run NoTests -bench BenchmarkAuthenticatedHTTPClientRequest -benchtime 100000x -count 10"
```

### Replication Descriptor Optimizations

Each row below reports the median from the raw paired runs used to accept the
feature. Improvements are ratios where larger is better.

| Feature benchmark | Before | After | Speed | Heap improvement | Allocation improvement | Tradeoff |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Carried-size splitter, 4,096 payloads | 1,069,489 ns; 2,945,587 B; 882 allocs | 38,888 ns; 30,720 B; 4 allocs | 27.50x | 95.89x | 220.50x | Immutable subslices retain the already-live source page until synchronous execution finishes |
| Default protobuf digest repair, 1,024 sets | 1,356,945 ns; 300,286 B; 1,197 allocs | 1,105,669 ns; 107,288 B; 1,157 allocs | 1.23x | 2.80x | 1.03x | None on all-set pages; wire bytes and compatibility fallback are unchanged |
| Mixed digest repair, 512 sets plus 512 deletes | 1,105,516 ns; 282,637 B; 684 allocs | 1,151,193 ns; 281,936 B; 685 allocs | 0.96x | 1.00x | 1.00x | 4.13% slower in the short run; a `100x`, ten-run confirmation narrowed this to 0.95%, so this is treated as neutral noise rather than a win |
| Fallback source scan, 10,000 1 KiB values | 6,309,581 ns; 210,240 B; 85 allocs | 4,480,362 ns; 209,088 B; 84 allocs | 1.41x | 1.01x | 1.01x | Key-only mode is restricted to full-push fallback; normal digest comparison still hashes values |
| Fallback repair collection, 10,000 keys | 4,939,194 ns; 1,854,982 B; 92 allocs | 4,544,286 ns; 421,376 B; 83 allocs | 1.09x | 4.40x | 1.11x | No wire or lock-scope change; collection is direct only after digest rejection |
| Direct default-wire digest serialization, 1,024 sets | 1,088,848 ns; 108,878 B; 1,158 allocs | 1,049,957 ns; 87,235 B; 136 allocs | 1.04x | 1.25x | 8.51x | Values share a bounded arena; keys remain direct immutable references for the synchronous group lifetime |
| Legacy-target full sync, 10,000 keys | 26,378,608 ns; 3 requests; 56,046 wire B; 1,003,978 B; 608 allocs | 13,147,784 ns; 2 steady requests; 55,893 wire B; 979,718 B; 432 allocs | 2.01x | 1.02x | 1.41x | Capability entries expire after five minutes, are capped at 1,024, and must match node address and topology fingerprint |
| Single-shard scan routing, 10,000 routes | 718,882 ns; 0 B; 0 allocs | 588,964 ns; 0 B; 0 allocs | 1.22x | 1.00x | 1.00x | Internal scan consumers omit unused bucket lookup; multi-shard routing delegates to the existing path |
| Prevalidated invariant fallback scan, 10,000 keys | 3,418,121 ns; 421,376 B; 83 allocs | 2,872,609 ns; 421,376 B; 83 allocs | 1.19x | 1.00x | 1.00x | Restricted to an unfiltered one-shard leader scan whose target is already in that shard's replication set |
| Known-legacy full-keyspace sync, 10,000 keys | 11,402,903 ns; 3 steady requests; 56,059 wire B; 1,002,881 B; 557 allocs | 11,070,085 ns; 2 steady requests; 55,886 wire B; 959,672 B; 417 allocs | 1.03x | 1.04x | 1.34x | Same-address in-place upgrades can wait at most the existing five-minute capability TTL; address and topology changes invalidate immediately |
| Packed fallback batch lookup, 10,000 keys | 10,688,124 ns; 2.004 requests; 55,877 wire B; 951,616 B; 413 allocs | 9,209,489 ns; 2.004 requests; 55,879 wire B; 652,898 B; 369 allocs | 1.16x | 1.46x | 1.12x | No measured regression: fixed-sequence protobuf is byte-identical and median maximum reader pause improved 1.33x; JSON and local partitions retain scalar lookup |
| Shared-lock fallback key scan under reader load, 10,000 keys | 10,074,975 ns; 4,120,711 ns reader pause; 1,064,766 B; 394 allocs | 8,965,740 ns; 214,766 ns reader pause; 1,049,238 B; 391 allocs | 1.12x; reader pause 19.19x shorter | 1.01x | 1.01x | One fixed mutex serializes fallback key scans; writers remain blocked, and any active/deferred TTL cleanup retains the exclusive path |
| Paired fallback scan lock scope, 10,000 keys | Exclusive: 2,925,823 ns; 3,511,665 ns reader pause; 20,112 B; 48 allocs | Shared: 2,927,976 ns; 178,865 ns reader pause; 20,118 B; 48 allocs | CPU neutral within 0.07%; reader pause 19.63x shorter | Neutral within 0.03% | 1.00x | Same-binary control isolates lock scope from HTTP and encoding noise; writer pause improved slightly from 1,345,226 ns to 1,337,389 ns |
| Native striped-counter fallback scan, 10,000 keys | Copy keys and mutable values: 2,612,095 ns; 19,712 B; 43 allocs | Copy keys only: 2,163,909 ns; 19,712 B; 43 allocs | 1.21x | 1.00x | 1.00x | Shared scans avoid reading value slots that striped counter writers may update concurrently; ordinary value scans keep the prior iterator |
| Epoch-validated fallback values, 10,000 keys | Second native lookup: 4,585,669 ns; 947,473 B; 51 allocs; 40 native batches | Reuse scan descriptors: 3,597,822 ns; 945,424 B; 50 allocs; 0 native batches | 1.27x | 1.002x | 1.02x | Stable values are validated under the existing shared lock in 256-entry chunks; mutation falls back for the remaining records and was neutral in an alternating-order control |
| Ordered native HAT-trie scan, 10,000 keys | 3,744,034 ns; 841,584 B; 100 allocs | 3,506,797 ns; 841,584 B; 100 allocs | 1.07x | 1.00x | 1.00x | Fixed 257-symbol stack histograms replace libc `qsort`; ordering and wire representation are unchanged |
| Ten-page known-legacy sync, 10,000 keys | Fresh arena/page: 12,234,348 ns; 10.01 requests; 57,488 wire B; 766,579 B; 1,408 allocs | Two-arena ring: 11,333,138 ns; 10.01 requests; 57,493 wire B; 287,987 B; 1,369 allocs | 1.08x | 2.66x | 1.03x | Arena reset waits only when its earlier streaming writer is still active; two bounded page arenas can overlap, while page size and lock duration stay fixed |
| Two-page known-legacy aggregation, 10,000 keys | Sender: 11,333,138 ns; 10.01 requests; 57,493 wire B; 287,987 B; 1,369 allocs. Receiver: 4,443,217 ns; 61,156 largest protobuf B | Sender: 9,698,527 ns; 5.01 requests; 56,551 wire B; 218,509 B; 745 allocs. Receiver: 3,663,435 ns; 122,156 largest protobuf B | Sender 1.17x; receiver 1.21x; combined 1.18x | Sender 1.32x; combined 1.02x | Sender 1.84x; combined 1.02x | Largest body is 2x higher but bounded to two unchanged 1,024-key scan pages and remains below the configured byte limit |
| Direct native packed scan, 10,000 keys (rejected) | Sender: 9,440,845 ns; 207,179 B; 744 allocs. Focused: 3,827,521 ns; 945,425 B; 50 allocs | Sender: 9,227,708 ns; 184,267 B; 720 allocs. Focused: 3,484,635 ns; 819,482 B; 29 allocs | Sender 1.02x; focused 1.10x | Sender 1.12x; focused 1.15x | Sender 1.03x; focused 1.72x | Rejected: the end-to-end speedup was below the 5% gate for adding and maintaining a specialized C ABI |
| Single-pass legacy repair, 10,000 keys (rejected) | 11,459,282 ns; 55,892 wire B; 977,706 B; 433 allocs | Unordered: 10,675,192 ns; 64,258 wire B; 948,316 B; 392 allocs | 1.07x | 1.03x | 1.11x | Rejected: unordered transfer was 1.15x larger; restoring deterministic order took 12,316,337 ns, 1.075x slower than baseline |
| Exact protobuf batch coalescing, 10,000 keys (rejected) | Sender: 10,422,384 ns; 2.004 requests; 949,539 B; 413 allocs. Receiver: 4,066,159 ns; 305,156 largest protobuf B | Sender: 10,214,713 ns; 1.004 requests; 928,371 B; 286 allocs. Receiver: 4,444,227 ns; 609,046 largest protobuf B | Sender 1.02x; combined 0.99x | Sender 1.02x; receiver 1.00x | Sender 1.44x; receiver 1.00x | Rejected: receiver decode was 1.09x slower, the largest request was 2.00x larger, and combined sender-plus-decode CPU was 1.012x slower |
| Carried compact payload estimates, 10,000 keys (rejected) | Estimate during split: 4,214,771 ns | Carry from serialization: 4,230,028 ns | 0.996x | 1.00x | 1.00x | Rejected: a same-binary alternating-order control showed that moving the work into serialization was 0.36% slower overall |
| Specialized compact payload estimator, 10,000 keys (rejected) | Generic: 8,117,717 ns; 650,825 B; 367 allocs | Specialized: 8,158,718 ns; 652,457 B; 369 allocs | 0.995x | 0.997x | 0.995x | Rejected: the focused splitter improved 1.92x, but equal-length end-to-end confirmation was 0.50% slower |

The mixed-page implementation carries the delete intent already discovered by
digest comparison. This selects generic compatibility storage before allocating
compact descriptors and removes the 17% transient-heap regression observed in
the first implementation. A concurrent state change can still force the old
dynamic conversion path, preserving the previous repair semantics.

The fallback collector now appends ordered, prefix-filtered repair changes
directly during the source scan instead of retaining an intermediate entry
page. It still releases the trie lock before splitting, network I/O, or flush.
The default protobuf/gRPC all-set repair path serializes each value directly
into one bounded byte arena and stores compact key/value-range records. Planned
mixed deletes and JSON compatibility continue to use the generic request path;
an unexpected concurrent deletion converts already-built direct records before
adding `INTERNALDEL`.

Older targets that accept a command envelope but do not implement digest
comparison previously paid for one rejected digest request and a separate
inventory scan on every synchronization. The replicator now remembers that
capability by node ID for five minutes and sends the full repair directly on
subsequent syncs. The cache is bounded, and entries are ignored after an
address or topology change. The first sync still probes, so upgrades are
detected without configuration. A digest-capable control benchmark kept the
same one request, 215 wire B, and allocation count.

Replication scans normally use one shard. Their internal route only needs the
shard, leader, owners, and replication targets, so the one-shard path now
constructs that scope directly instead of hashing every key and searching the
bucket table. The public route API still returns the exact bucket, and the
multi-shard path is byte-for-byte the previous generic decision path. The
10,000-key fallback collector improved 1.04x in the initial paired run and
1.05x for buffered collection in the final `100x` confirmation, with unchanged
heap and allocations.

That scope is also invariant for the lifetime of an eligible digest or fallback
iterator. The iterator now validates the one-shard, no-bucket-filter leader and
target relationship once, then avoids repeating it for every key. The final
mode is stored in an existing byte after iterator construction; an earlier
constructor-time flag candidate was discarded because it added one 704-byte
allocation. Direct fallback collection improved 1.19x, from 3.418 ms to 2.873
ms, with exactly the same heap and allocation counts. End-to-end fallback
improved 1.15x, from 11.703 ms to 10.198 ms, and maximum reader pause shortened
1.46x, from 4.903 ms to 3.357 ms. Wire bytes and request count were unchanged.

For a full-keyspace sync to a target already cached as digest-unsupported, the
replicator now checks that capability before attempting Merkle comparison. This
removes a redundant unsupported request while preserving the existing full
repair. The paired `100x` median improved from 11.403 ms to 11.070 ms, cumulative
heap fell from 1,002,881 B to 959,672 B, and allocations fell from 557 to 417.
Steady request count fell from three to two. A digest-capable control retained
one request, 215 wire B, and 414 allocations; opposing run order changed the
small timing difference, so no modern-target speedup is claimed.

The known-legacy compact fallback now copies sorted scan keys once into the
existing payload arena and resolves values through bounded 256-key native
lookups. A 10,000-key repair therefore uses 40 lookup cgo calls instead of
10,000 scalar calls. Key and value metadata remain 20 bytes per entry in two
compact slices, so batching adds no per-key descriptor memory. Protobuf order,
request splitting, expiration, and concurrent-delete conversion are unchanged;
tests compare fixed-sequence wire bytes directly. JSON compatibility and local
partitions retain the prior scalar implementation.

The paired `500x` median improved from 10.688 ms to 9.209 ms, cumulative heap
fell from 951,616 B to 652,898 B, and allocations fell from 413 to 369. Request
count stayed at 2.004 per operation. Compressed wire medians differed by two
bytes (55,877 versus 55,879 B) because benchmark sequence metadata varies;
fixed-sequence output is byte-identical. In a separate ten-run contention gate,
median maximum reader pause shortened from 4.583 ms to 3.438 ms, so the bounded
lock chunks introduced no measured reader cost.

The legacy fallback key collector now uses a shared trie lock while collecting
ordered keys when the trie has neither active expirations nor deferred expired
cursor entries. The callback only copies keys and evaluates an immutable
routing snapshot; value storage is still resolved later in bounded exclusive
chunks. A dedicated mutex preserves the former one-scan-at-a-time behavior, so
multiple repairs cannot create a new concurrent sort or scan load spike.
Writers therefore wait for the same consistent scan boundary as before.

Any TTL metadata, deferred expiration cleanup, local partitions, snapshots,
value-producing digest scans, and value materialization take the previous
exclusive path. Generation checks still restart a cursor after writes between
pages. Tests coordinate blocked callbacks to prove that ordinary reads proceed,
writers remain blocked, read-only scans remain serialized, and TTL cleanup is
not moved under a shared lock.

In the final reader-stress pair, median maximum pause fell from 4.121 ms to
0.215 ms, or 19.19x, while the full sync improved from 10.075 ms to 8.966 ms.
The same-binary lock-only control measured 2.926 ms exclusive versus 2.928 ms
shared, a neutral 0.07% difference, with 48 allocations in both modes. Its
reader pause improved 19.63x and writer pause was 0.6% shorter. A separate 10k
end-to-end run retained 2.040 requests per operation and effectively unchanged
compressed wire bytes; sequence metadata accounts for the measured 55,896 B
versus 55,893 B difference.

Opt-in striped counter writes share the trie read lock and update existing
native value slots under per-key stripes. The fallback scanner only needs keys,
but the former batched iterator copied every value and could therefore read a
slot concurrently with a striped update. A dedicated native key-only iterator
now avoids value access from construction through batch reads, including keys
stored as trie-node terminals after bucket bursting. It is selected only for
shared packed-key fallback scans when stripes are enabled. Mutation epochs are
loaded atomically on the shared path; TTL cleanup still selects the value-aware
exclusive iterator.

Native and Go tests cover buffer exhaustion, null and exhausted iterators,
sorted ordering, burst prefix terminals, empty returned values, and repeated
scans concurrent with striped counter writes. The paired 10k native benchmark
improved from 2.612 ms to 2.164 ms, or 1.21x, with the same 19,712 cumulative
heap bytes and 43 allocations.

The ordinary TTL-free fallback scan now carries each native value descriptor
in fields already present in its compact arena record. Before serialization,
the mutation epoch is validated under the trie read lock in bounded 256-entry
chunks. An unchanged page therefore avoids the later 40 native lookup calls
without adding per-key storage. A mutation between chunks switches all
remaining records to the previous native batch lookup. TTL state, local
partitions, and opt-in striped counter writes select that fallback immediately.

Tests compare fixed-sequence protobuf output against scalar lookup for every
in-memory value type and cover mutation before preparation and between chunks.
The 10k end-to-end median improved from 9.882 ms to 8.188 ms, or 1.21x, while
requests and compressed wire bytes were unchanged. Focused preparation
improved 1.27x and removed one allocation. The alternating-order mutation
control measured 4.810 ms for the old lookup and 4.799 ms for the candidate,
with 102 combined allocations in both modes; the 0.22% difference is treated
as neutral. Median maximum reader pause also shortened from the preceding
0.215 ms to 0.147 ms, or 1.46x.

The native sorted iterator now partitions its existing slot-pointer array with
an in-place MSD radix sort. Prefix skipping avoids repeatedly building byte
histograms for long shared prefixes; insertion sort handles groups of 24 or
fewer; and the largest partition is processed as a tail call so active
recursion remains logarithmically bounded. Tests cover embedded zero and high
bytes, prefix ordering, 127/128-byte length encoding boundaries, and a 1 KiB
shared prefix. The focused ordered scan improved 1.07x. In the final paired
replication collector, buffered collection improved 1.05x and direct
collection was neutral at 1.006x; cumulative heap and allocations were
identical. The end-to-end sync improved 1.008x over the immediately preceding
commit, so the sorter introduced no measured CPU, memory, or wire regression.

Paged legacy fallback now rotates between two request-local payload arenas
instead of allocating one arena for every page. A compressed HTTP body holds a
writer reference on its source arena and releases it only after serialization
finishes. Reset therefore cannot overwrite bytes still visible to `net/http`;
the second arena preserves serialization/transport overlap while the first is
still draining. gRPC remains synchronous and uses one arena. The ring does not
change the 1,000-key scan page, lock scope, request splitting, or wire format.

An immediate same-environment A/B measured the fresh-allocation control at
12.234 ms and the ring at 11.617 ms. A seven-run confirmation measured the ring
at 11.333 ms, 287,987 cumulative heap B, and 1,369 allocations. Against the A/B
control this is 1.08x faster, 2.66x lower heap, and 39 fewer allocations. The
10.01 requests/op and approximately 57.49 KiB compressed wire result are
unchanged. The bounded lifetime cost is at most two page arenas per active
fallback sync.

The next pass aggregates at most two unchanged scan pages into one fallback
request when the configured byte limit has room for a conservative 96 bytes per
entry. HTTP uses one two-page arena in this mode, so its retained staging
capacity does not exceed the preceding two one-page arena ring. A smaller or
disabled byte limit retains one-page behavior. The helper uses division before
multiplication and tests `math.MaxInt`, preventing configuration overflow from
turning into a huge allocation.

The paired sender median improved from 11.333 ms to 9.699 ms, requests fell from
10.01 to 5.01, wire fell 1.7%, cumulative heap fell from 287,987 B to 218,509 B,
and allocations fell from 1,369 to 745. The matching receiver decode median
improved from 4.443 ms for ten requests to 3.663 ms for five. Combined measured
CPU improves 1.18x, while combined heap and allocations each improve about
1.02x. Median maximum reader pause fell from 0.707 ms to 0.167 ms because trie
scan calls remain limited to 1,024 keys.

The accepted cost is a twofold largest protobuf body increase, from 61,156 B to
122,156 B, still 8.6x below the default 1 MiB transfer limit. A trial that
aggregated ten pages reached 305,156 B and reduced requests further, but it
could stage ten unusually large pages before splitting. That variant was
rejected and replaced by the two-page cap to preserve the prior bounded-memory
shape.

Complete packed fallback groups now avoid the exact per-key JSON size loop when
aggregate arena lengths prove that the group fits the configured batch limit.
The O(1) proof bounds each key byte at the worst-case six-byte JSON control
escape, adds the fixed compact-command cost per record, and adds the packed
binary arena length. All arithmetic saturates at `maxBytes + 1`; `math.MaxInt`
is handled without overflow. Indexed, direct, partial, and carried-estimate
arenas retain the previous exact splitter.

Tests were added before the implementation and initially failed on the missing
proof helper. They cover control characters, invalid UTF-8, exact-estimate
dominance, saturation, `math.MaxInt`, unsupported arena layouts, and unchanged
split backing. The detached-baseline `300x`, ten-run sender median improved from
9.738 ms to 9.020 ms, or 1.08x. Median cumulative heap fell from 215,929 B to
206,446 B, or 1.05x, while 744 allocations and 5.003 requests were unchanged.
Compressed wire was neutral at 56,554 B versus 56,559 B.

The five-request receiver path is untouched. Its control retained 50,428
allocations, 55,482 wire B, and a 122,156 B largest protobuf; timing varied from
4.500 ms to 4.609 ms across sequential runs and is treated as host noise. An
immediate reverse-order reader control measured 1.008 ms maximum pause before
and 1.008 ms after. The first shorter reader pair moved in the opposite
direction, confirming that the maximum is scheduler-sensitive; no reader-pause
gain is claimed.

A direct native packed-scan experiment added a lightweight iterator that wrote
full prefixed keys and raw values directly into the final replication arena in
up to 1,024-record calls. Tests were written first for packed value words,
offset overflow, undersized-buffer retry, binary and long keys, pagination,
mutation restart, TTL/striped fallback, and byte-identical all-type protobuf.
The candidate removed intermediate cursor arrays, prefix expansion, and most
256-record cgo crossings without changing sorted order or lock page size.

Focused preparation improved from 3.828 ms, 945,425 B, and 50 allocations to
3.485 ms, 819,482 B, and 29 allocations: 1.10x faster, 1.15x lower heap, and
1.72x fewer allocations. The detached-baseline `300x`, ten-run end-to-end gate
improved only from 9.441 ms to 9.228 ms, or 1.02x. Heap improved 1.12x and
allocations improved 1.03x; 5.003 requests and approximately 56,549 compressed
wire bytes were unchanged. Reader pause moved from 0.169 ms to 0.161 ms while
reader-load wall time was neutral within 0.5%. Because the 2.3% sender gain did
not clear the stated 5% threshold for maintaining a new C/Go ABI, all candidate
code and feature tests were rolled back. No runtime tradeoff remains.

The first arena candidate copied every key and reconstructed strings while
sizing and writing protobuf. Its paired 10k end-to-end median was about 9%
slower, so it was rejected before commit. Direct immutable key references
removed that loss. The final paired `100x` gate measured 18,030,151 ns,
1,248,561 B, and 10,606 allocations before versus 18,050,084 ns, 1,009,376 B,
and 604 allocations after: CPU was neutral within 0.11%, heap improved 1.24x,
and allocations improved 17.56x. A second profiled pair measured the final path
0.22% faster, confirming there is no repeatable CPU regression.

A later single-pass repair experiment wrote fallback values directly into the
outgoing arena and removed the intermediate change list plus second lookup.
The unordered variant improved CPU by 1.07x and allocations by 1.11x, but made
the compressed request 1.15x larger because key order affects compression. A
deterministically ordered variant restored the previous wire size but was
1.075x slower. Both implementations and their feature-specific tests were
removed; only the reusable reader-pause benchmark remains.

An exact-size splitter experiment replaced the compact protobuf path's
conservative JSON-oriented byte estimate. The 10,000-key receiver fixture
encoded to 609,046 B (about 595 KiB) and therefore fit beneath the existing 1
MiB limit as one request. The candidate included exact-boundary tests using the
worst-case 20-digit sequence
and an end-to-end batching test before implementation. On the sender it halved
steady request count, improved the paired `500x` median by 1.02x, lowered heap
1.02x, and reduced allocations 1.44x.

The receiver benchmark then decompressed and protobuf-decoded the same 10,000
commands as either one request or two. One request increased median decode time
from 4.066 ms to 4.444 ms and doubled the largest uncompressed protobuf body
from 305,156 B to 609,046 B. Receiver heap was also slightly higher. Adding the
paired sender and receiver medians made the candidate 1.012x slower overall,
before identical command-apply work. The exact splitter and its feature tests
were therefore rolled back. `BenchmarkReplicationCompactBatchReceiver` remains
as a gate for any future streaming-decoder or batch-coalescing proposal.

Two payload-estimation variants were also rejected. Carrying each compact
payload's conservative size in the existing arena record made the isolated
4,096-item split 4.37x faster, but an alternating-order benchmark that included
scan, value serialization, and splitting measured 4.215 ms for the previous
path and 4.230 ms for the candidate. Moving the same work earlier was therefore
0.36% slower despite unchanged allocations.

A second variant kept estimation in the splitter and replaced the generic
request estimator with an exact specialized formula. Differential tests first
proved identical estimates, escaped-key split boundaries, and protobuf bytes.
The focused split improved from 0.443 ms to 0.231 ms, or 1.92x. Equal-length
`500x` end-to-end confirmation nevertheless measured 8.118 ms before and 8.159
ms after, a 0.50% loss with no request or wire reduction. Both variants and
their feature-specific code were rolled back, leaving no runtime tradeoff.

Raw local output is retained in:

- `build/benchmarks/replication-fallback-collector.txt`
- `build/benchmarks/replication-fallback-direct-e2e.txt`
- `build/benchmarks/replication-arena-before.txt`
- `build/benchmarks/replication-direct-record-after.txt`
- `build/benchmarks/replication-direct-record-e2e.txt`
- `build/benchmarks/replication-direct-record-e2e-stable-baseline.txt`
- `build/benchmarks/replication-direct-record-e2e-stable.txt`
- `build/benchmarks/replication-capability-before.txt`
- `build/benchmarks/replication-capability-after.txt`
- `build/benchmarks/replication-capability-modern-before.txt`
- `build/benchmarks/replication-capability-modern-after.txt`
- `build/benchmarks/replication-single-route-paired.txt`
- `build/benchmarks/replication-single-route-before.txt`
- `build/benchmarks/replication-single-route-after.txt`
- `build/benchmarks/native-radix-before.txt`
- `build/benchmarks/native-radix-final-scan.txt`
- `build/benchmarks/native-radix-final-paired.txt`
- `build/benchmarks/native-radix-e2e-after.txt`
- `build/benchmarks/single-pass-fallback-before.txt`
- `build/benchmarks/single-pass-fallback-after.txt`
- `build/benchmarks/single-pass-fallback-ordered-after.txt`
- `build/benchmarks/single-pass-fallback-pause-before.txt`
- `build/benchmarks/single-pass-fallback-pause-after.txt`
- `build/benchmarks/prevalidated-scope-before.txt`
- `build/benchmarks/prevalidated-scope-precomputed-final.txt`
- `build/benchmarks/prevalidated-scope-e2e-before.txt`
- `build/benchmarks/prevalidated-scope-e2e-confirm.txt`
- `build/benchmarks/prevalidated-scope-pause-after.txt`
- `build/benchmarks/pre-merkle-cache-full-before.txt`
- `build/benchmarks/pre-merkle-cache-full-confirm.txt`
- `build/benchmarks/pre-merkle-cache-modern-before-final.txt`
- `build/benchmarks/pre-merkle-cache-modern-final.txt`
- `build/benchmarks/protobuf-size-before-final.txt`
- `build/benchmarks/protobuf-size-after-confirm.txt`
- `build/benchmarks/protobuf-receiver-two-requests.txt`
- `build/benchmarks/protobuf-receiver-one-request.txt`
- `build/benchmarks/protobuf-receiver-two-confirm.txt`
- `build/benchmarks/packed-lookup-before.txt`
- `build/benchmarks/packed-lookup-after.txt`
- `build/benchmarks/packed-lookup-pause-before.txt`
- `build/benchmarks/packed-lookup-pause-after.txt`
- `build/benchmarks/replication-shared-scan-paired.txt`
- `build/benchmarks/replication-shared-scan-e2e-before.txt`
- `build/benchmarks/replication-shared-scan-e2e-after.txt`
- `build/benchmarks/replication-shared-scan-reader-before.txt`
- `build/benchmarks/replication-shared-scan-reader-after.txt`
- `build/benchmarks/replication-key-only-before.txt`
- `build/benchmarks/replication-key-only-after.txt`
- `build/benchmarks/replication-key-only-paired.txt`
- `build/benchmarks/replication-scanned-values-before.txt`
- `build/benchmarks/replication-scanned-values-after.txt`
- `build/benchmarks/replication-scanned-values-mutation-paired.txt`
- `build/benchmarks/replication-scanned-values-reader-after.txt`
- `build/benchmarks/replication-packed-estimates-focused.txt`
- `build/benchmarks/replication-packed-estimates-after.txt`
- `build/benchmarks/replication-packed-estimates-paired.txt`
- `build/benchmarks/replication-compact-estimator-after.txt`
- `build/benchmarks/replication-compact-estimator-e2e-before-confirm.txt`
- `build/benchmarks/replication-compact-estimator-e2e-confirm.txt`
- `build/benchmarks/replication-page-arena-before.txt`
- `build/benchmarks/replication-page-arena-ring-after.txt`
- `build/benchmarks/replication-page-arena-ab-control.txt`
- `build/benchmarks/replication-page-arena-confirmation.txt`
- `build/benchmarks/replication-page-aggregation-reader-before.txt`
- `build/benchmarks/replication-page-aggregation-sender-after.txt`
- `build/benchmarks/replication-page-aggregation-receiver-two.txt`
- `build/benchmarks/replication-page-aggregation-receiver-ten.txt`
- `build/benchmarks/replication-page-aggregation-reader-after.txt`
- `build/benchmarks/replication-page-aggregation-two-page-sender.txt`
- `build/benchmarks/replication-page-aggregation-receiver-five.txt`
- `build/benchmarks/replication-page-aggregation-two-page-reader.txt`
- `build/benchmarks/replication-no-split-proof-confirm-before.txt`
- `build/benchmarks/replication-no-split-proof-confirm-after.txt`
- `build/benchmarks/replication-no-split-proof-receiver-before.txt`
- `build/benchmarks/replication-no-split-proof-receiver-after.txt`
- `build/benchmarks/replication-no-split-proof-reader-control-before.txt`
- `build/benchmarks/replication-no-split-proof-reader-confirm-after.txt`
- `build/benchmarks/replication-direct-native-scan-before.txt`
- `build/benchmarks/replication-direct-native-scan-after.txt`
- `build/benchmarks/replication-direct-native-scan-confirm-before.txt`
- `build/benchmarks/replication-direct-native-scan-confirm-after.txt`
- `build/benchmarks/replication-direct-native-scan-reader-before.txt`
- `build/benchmarks/replication-direct-native-scan-reader-after.txt`

Reproduce the stable end-to-end row with:

```sh
make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=NoSyncBenchmark \
  REPLICATION_DIGEST_BENCH=BenchmarkHTTPReplicatorSyncAllBatching/Batched10k \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=100x COUNT=1 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-direct-record-e2e-stable.txt
```

Reproduce the latest focused measurements through the same Makefile and
benchmark script:

```sh
make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkHTTPReplicatorSyncAllBatching/Batched10k \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=20x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-capability-after.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=NoSyncBenchmark \
  REPLICATION_DIGEST_BENCH='BenchmarkReplicationScanRouteModes|BenchmarkHatTrieScanOrderModes' \
  REPLICATION_ITERATOR_BENCH=BenchmarkReplicationDigestFallbackCollectionModes \
  BENCHTIME=20x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-route-radix.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=NoSyncBenchmark \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=BenchmarkReplicationDigestFallbackCollectionModes \
  BENCHTIME=100x COUNT=5 \
  REPLICATION_OPTIMIZATION_OUTPUT=prevalidated-scope-precomputed-final.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkHTTPReplicatorSyncAllBatching/FullKeyspace10k \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=100x COUNT=5 \
  REPLICATION_OPTIMIZATION_OUTPUT=pre-merkle-cache-full-confirm.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkHTTPReplicatorLegacyFallbackReaderPause \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=20x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=prevalidated-scope-pause-after.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkReplicationCompactBatchReceiver/TwoRequests \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=100x COUNT=5 \
  REPLICATION_OPTIMIZATION_OUTPUT=protobuf-receiver-two-requests.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkReplicationCompactBatchReceiver/OneRequest \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=100x COUNT=5 \
  REPLICATION_OPTIMIZATION_OUTPUT=protobuf-receiver-one-request.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkHTTPReplicatorSyncAllBatching/FullKeyspace10k \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=BenchmarkReplicationPackedFallbackPreparation \
  BENCHTIME=100x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-scanned-values-after.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=NoSyncBenchmark \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=BenchmarkReplicationPackedFallbackMutationPair \
  BENCHTIME=100x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-scanned-values-mutation-paired.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkHTTPReplicatorSyncAllBatching/Default1k \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=100x COUNT=7 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-page-arena-confirmation.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkHTTPReplicatorSyncAllBatching/Default1k \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=100x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-page-aggregation-two-page-sender.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkReplicationCompactBatchReceiver/FiveRequests \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=100x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-page-aggregation-receiver-five.txt

make bench-replication-optimizations \
  REPLICATION_SPLIT_BENCH=NoSplitBenchmark \
  REPLICATION_SYNC_BENCH=BenchmarkHTTPReplicatorSyncAllBatching/Default1k \
  REPLICATION_DIGEST_BENCH=NoDigestBenchmark \
  REPLICATION_ITERATOR_BENCH=NoIteratorBenchmark \
  BENCHTIME=300x COUNT=10 \
  REPLICATION_OPTIMIZATION_OUTPUT=replication-no-split-proof-confirm-after.txt
```

### Replication Page Traversal

The default 1,000-key page benchmark sends ten ordered pages for 10,000 keys.
Medians use `-benchtime=20x`; the current row is a seven-run median.

| Version | Time/op | requests/op | B/op | allocs/op | Cumulative speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before persistent cursor (`471c229`) | 61,122,327 ns | 10 | 1,877,005 | 123,996 | 1.00x |
| Previous (`e5b127d`) | 19,709,083 ns | 10 | 999,805 | 11,885 | 3.10x |
| Request-scoped arena ring | 11,333,138 ns | 10.01 | 287,987 | 1,369 | 5.39x |
| Bounded two-page aggregation | 9,698,527 ns | 5.01 | 218,509 | 745 | 6.30x |
| Current packed batch no-split proof | 9,019,787 ns | 5.003 | 206,446 | 744 | 6.78x |

The current default-page path is 6.78x faster, uses 9.09x less cumulative heap,
and performs 166.66x fewer allocations than the original traversal baseline.
The native iterator returns up to 256 records per cgo crossing, so a 10,000-key
scan needs about 40 batch calls instead of one crossing per key.

<a id="persistent-partition-replication-cursors"></a>
#### Persistent Partition Replication Cursors

The original local-partition branch ignored the reusable replication cursor.
Every page collected all child entries, globally sorted the full keyspace,
selected one page, and discarded the result. A 100-page traversal therefore
performed 100 complete scans and sorts. The current path retains one native
iterator and generation value per child plus a k-way key heap between pages.
Any child mutation closes all retained iterators, increments the restart count,
and rebuilds the heap; the caller's `after_key` prevents duplicates when the
scan resumes.

The benchmark preloads 100,000 deterministic strings into 16 local partitions
and traverses all keys in 1,000-key pages. The legacy control preserves the old
materialize/sort implementation. Both paths invoke the same visitor and return
the same 100 ordered pages. Results are seven-run medians on the Ryzen 9 5950X
host.

```sh
make bench-partition-cursor PARTITION_CURSOR_BENCH_KEYS=100000 PARTITION_CURSOR_BENCH_PAGE_SIZE=1000 BENCHTIME=1x COUNT=7 BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

| Metric | Full materialize per page | Persistent partition cursor | Improvement |
| --- | ---: | ---: | ---: |
| Complete traversal | 1,076.176 ms | 56.721 ms | 18.97x faster |
| Cumulative heap | 1,393,738,984 B | 9,779,040 B | 142.52x lower |
| Allocations | 10,069,862 | 300,538 | 33.51x fewer |
| Pages / keys | 100 / 100,000 | 100 / 100,000 | unchanged |

Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Full materialize per page | `1111.398, 1088.821, 1064.783, 1076.176, 1087.228, 1066.507, 1072.552` |
| Persistent partition cursor | `56.721, 52.879, 52.217, 56.881, 58.437, 54.422, 57.736` |

The complete output is generated at
`build/benchmarks/partition-replication-cursor.txt`. Cursor state is transient
and bounded by the configured local partition count. No wire, storage, routing,
or page-response format changes. `LOCAL_PARTITIONS=0` retains the existing
single-trie cursor path, while mutation tests verify ordered restart behavior
and end-to-end compact-binary replication from child tries.

<a id="packed-internal-scan-arenas"></a>
#### Packed Internal Scan Arenas

The next scan pass removes the remaining per-key Go allocation from synchronous
internal traversal. Native iterator batches expose key offsets into one reusable
arena, prefix scans expand a complete batch into one reusable buffer, and the
partition merge uses a typed in-place heap instead of `container/heap` interface
boxing. Digest inventories and roots, Merkle rebuilds, snapshot stream capture,
and compact replication consume each borrowed key before advancing the native
batch. Only the page boundary key is cloned so a later page or generation
restart has a stable resume token.

Public `Keys`/`Entries`, persistence captures, and other callers that retain
entries use one immutable arena per native batch instead. Their strings remain
valid after the cursor advances or closes. This durable mode can keep the rest
of a batch arena alive while one returned key remains referenced; each arena is
bounded to at most 256 iterator records. No wire, snapshot, or storage encoding
changed.

The same 100,000-key, 16-partition, 100-page fixture and command from the
previous section now includes `PersistentCursor` and `PackedCursor` rows. The
baseline is the committed persistent-cursor result above; medians use seven
single-traversal samples.

| Metric | Prior per-key cursor | Immutable batch arenas | Reusable internal arenas | Internal improvement |
| --- | ---: | ---: | ---: | ---: |
| Complete traversal | 56.721 ms | 48.183 ms | 49.752 ms | 1.14x faster |
| Cumulative heap | 9,779,040 B | 2,801,008 B | 356,816 B | 27.41x lower |
| Allocations | 300,538 | 937 | 669 | 449.23x fewer |
| Pages / keys | 100 / 100,000 | 100 / 100,000 | 100 / 100,000 | unchanged |

The immutable mode was 1.03x faster than borrowed mode in this noisy
single-traversal fixture, but borrowed mode used 7.85x less cumulative heap and
1.40x fewer allocations. Raw elapsed samples in milliseconds were:

| Path | Seven samples |
| --- | --- |
| Immutable batch arenas | `48.900, 47.874, 47.920, 49.112, 48.183, 49.235, 45.903` |
| Reusable internal arenas | `48.069, 49.752, 51.534, 43.828, 48.117, 56.524, 54.411` |

The raw benchmark output remains at
`build/benchmarks/partition-replication-cursor.txt`.

### Replication Transport

Run:

```sh
make run CMD='go test . -run=NONE -bench=BenchmarkReplicationSyncTransport -benchmem -benchtime=10x -count=7'
```

This measures the same 10,000-key, ten-page sender and receiver through local
HTTP/protobuf and one ordered gzip-compressed gRPC stream per target.

| Transport | Time/op | batches/op | wire_B/op | B/op | allocs/op | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTP/protobuf (default) | 44,957,163 ns | 10 | 57,479 | 19,652,940 | 123,772 | 1.00x |
| gRPC stream (opt-in) | 37,765,365 ns | 10 | 52,006 | 22,832,475 | 93,557 | 1.19x |

The gRPC stream sends 9.52% fewer bytes and performs 24.41% fewer allocations,
while allocating 16.18% more cumulative heap for gRPC framing and compression.
HTTP remains the default and the configurable fallback because it has the
smaller heap footprint and requires no native listener.

### Replication Compression Tradeoff

Run:

```sh
make run CMD='go test ./internal/jsonwire -run=NONE -bench=BenchmarkGzipCompressionLevels -benchtime=20x -count=3 -benchmem'
```

The benchmark compresses a 10,000-row replication-shaped payload. Rows are
three-run medians after writer initialization and buffer growth are excluded.

| Gzip level | Time/op | wire_B/op | B/op | allocs/op | CPU vs BestSpeed |
| --- | ---: | ---: | ---: | ---: | ---: |
| BestSpeed, level 1 (default) | 346,578 ns | 4,710 | 0 | 0 | 1.00x |
| Default, level 6 | 1,438,824 ns | 1,967 | 0 | 0 | 4.15x slower |
| BestCompression, level 9 | 1,443,409 ns | 1,967 | 0 | 0 | 4.16x slower |
| HuffmanOnly | 1,782,782 ns | 385,362 | 0 | 0 | 5.14x slower |

Default compression saves 2.39x body bytes versus BestSpeed on this highly
repetitive fixture but costs 4.15x CPU, so BestSpeed remains the latency-oriented
default. Replacing the GC-ephemeral gzip `sync.Pool` with a bounded four-writer
cache reduced sampled compressor allocation from 15.23 MB to 1.14 MB across 50
10k-key syncs, or 13.4x less compressor allocation, without changing wire bytes.

### Replication Target Fanout

Run:

```sh
make run CMD='go test -run=NONE -bench=BenchmarkHTTPReplicatorTargetFanout -benchmem -benchtime=20x -count=5'
```

Each operation sends to four local HTTP targets whose handlers each hold the
request for 2 ms. Medians from the five runs:

| Mode | Time/op | targets/op | B/op | allocs/op | Serial speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Serial (`REPLICATION_MAX_IN_FLIGHT_TARGETS=1`) | 9,544,371 ns | 4 | 48,172 | 420 | 1.00x |
| Bounded parallel (`REPLICATION_MAX_IN_FLIGHT_TARGETS=4`) | 2,617,552 ns | 4 | 55,269 | 432 | 3.65x |

The bounded path adds 12 allocations and about 1.15x cumulative heap for this
four-target operation. Single-target delivery does not start worker goroutines.

## Journal Delta-First Recovery Benchmark

Run:

```sh
make bench-journal-catchup BENCHTIME=5x COUNT=7
```

`BenchmarkJournalCatchUpDeltaVsFullSnapshot` measures end-to-end local HTTP
transfer, decode/validation, trie mutation, journal persistence, and cleanup.
The delta fixture applies 100 retained `SETSTR` records. The exact-rebuild
fixture replaces a follower from a 10,000-key fast-gzip binary snapshot,
including stale-key deletion. The control delta row keeps the old one-`fsync`
per command behavior. Values are seven-run medians on the same AMD Ryzen 9
5950X host; filesystem `fsync` latency is host/load sensitive.

| Recovery path | Work/op | Seconds/op | wire_B/op | B/op | allocs/op | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Retained delta, one batch `fsync` (default) | 100 deltas | 0.002170 s | 9,425 | 163,918 | 702 | 56.55x faster than per-command `fsync` |
| Retained delta, one `fsync` per command (control) | 100 deltas | 0.122684 s | 9,425 | 176,726 | 799 | 1.00x |
| Exact snapshot fallback | 10,000 keys | 0.092649 s | 56,267 | 25,709,960 | 231,154 | 42.70x slower than retained delta |

For this fixture, retaining 100 deltas instead of rebuilding 100x more entries
uses 5.97x less wire, 156.85x less cumulative allocated heap, and 329.28x fewer
allocations. Batching the durable delta append replaces 100 `fsync` calls with
one while preserving successful-prefix replay semantics on command failure.
The full snapshot remains the correctness fallback after compaction because it
replaces the complete key set at a source journal sequence; it is not the
normal catch-up path.

## HAT-trie vs Redis

Redis was measured with Redis 7.0.4 in a temporary Docker container, one
client, no pipeline, and 10,000 requests per command. Rows with two Redis
commands add the two Redis seconds-per-10k values before computing speedup.

| Feature family | HAT-trie benchmark | HAT-trie seconds / 10k | Redis measured command | Redis seconds / 10k | Redis/HAT speedup |
| --- | --- | ---: | --- | ---: | ---: |
| String write | `BenchmarkCommandFeature/StringSet` | 0.004832 s | `SET` | 1.203000 s | 248.97x |
| String read | `BenchmarkCommandFeature/StringGet` | 0.002620 s | `GET` | 0.998000 s | 380.92x |
| Integer counter | `BenchmarkCommandFeature/CounterInc` | 0.005221 s | `INCR` | 0.966000 s | 185.02x |
| TTL update | `BenchmarkCommandFeature/TTLExpire` | 0.007290 s | `EXPIRE` | 1.006000 s | 138.00x |
| Map/hash write | `BenchmarkCommandFeature/MapPut` | 0.003636 s | `HSET` | 1.296000 s | 356.44x |
| Map/hash read | `BenchmarkCommandFeature/MapPeek` | 0.002713 s | `HGET` | 1.396999 s | 514.93x |
| List/deque push+pop | `BenchmarkCommandFeature/SlicePushPop` | 0.006425 s | `LPUSH` + `RPOP` | 2.082000 s | 324.05x |
| Set add+has | `BenchmarkCommandFeature/SetAddHas` | 0.012380 s | `SADD` + `SISMEMBER` | 1.835000 s | 148.22x |
| Priority queue push+pop | `BenchmarkCommandFeature/PriorityQueuePushPop` | 0.019410 s | `ZADD` + `ZPOPMIN` | 2.216999 s | 114.22x |
| Roaring bitmap add approximation | `BenchmarkCommandFeature/RoaringAdd` | 0.004360 s | `SETBIT` bitmap, not roaring | 1.020000 s | 233.94x |
| Roaring bitmap lookup approximation | `BenchmarkCommandFeature/RoaringHas` | 0.002793 s | `GETBIT` bitmap, not roaring | 1.090000 s | 390.26x |
| Sparse uint64 bitset add approximation | `BenchmarkCommandFeature/SparseBitsetAdd` | 0.004545 s | `SETBIT` dense bitmap approximation | 1.020000 s | 224.42x |
| Sparse uint64 bitset lookup approximation | `BenchmarkCommandFeature/SparseBitsetHas` | 0.002811 s | `GETBIT` dense bitmap approximation | 1.090000 s | 387.76x |
| HyperLogLog add | `BenchmarkCommandFeature/HyperLogLogAdd` | 0.062230 s | `PFADD` | 1.043000 s | 16.76x |
| HyperLogLog count | `BenchmarkCommandFeature/HyperLogLogCount` | 0.054010 s | `PFCOUNT` | 1.186000 s | 21.96x |
| Replication dump | `BenchmarkCommandFeature/ReplicationDump` | 0.004782 s | `DUMP` | 1.088000 s | 227.52x |

<!-- END GENERATED COMMAND BENCHMARK COMPARISON -->
## HAT-trie Transport Costs

Run this section locally when you need apples-to-apples HAT-trie protocol
overhead before comparing against Redis TCP or a remote Tarantool service:

```
make bench-hatrie-transport-features HATRIE_TRANSPORT_BENCH='^BenchmarkCommandTransportFeature/(InProcess|HTTPJSON|HTTPProtobuf|GRPC)/(StringSet|StringGet|CounterInc|MapPut|MapPeek)$' BENCHTIME=100x
```

The benchmark rows are named as
`BenchmarkCommandTransportFeature/<transport>/<feature>`, for example
`BenchmarkCommandTransportFeature/HTTPProtobuf/StringSet` and
`BenchmarkCommandTransportFeature/GRPC/StringGet`. Use the transport rows to
measure CPU, heap, and per-operation latency added by the API layer before
making Redis/Tarantool conclusions from in-process HAT-trie rows.

Local 100-iteration spot check:

| Transport | Feature | Time/op | Bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| In-process | String write | 1,536 ns | 12 B | 1 |
| In-process | String read | 991.7 ns | 0 B | 0 |
| HTTP JSON | String write | 292,585 ns | 80,808 B | 124 |
| HTTP JSON | String read | 123,490 ns | 78,979 B | 121 |
| HTTP protobuf | String write | 214,397 ns | 119,920 B | 125 |
| HTTP protobuf | String read | 188,322 ns | 130,715 B | 123 |
| gRPC protobuf | String write | 132,951 ns | 22,825 B | 195 |
| gRPC protobuf | String read | 113,880 ns | 10,557 B | 191 |

On this small payload, native gRPC is the lowest-latency wire path. HTTP
protobuf reduces some response work but still pays HTTP request construction
and protobuf allocation costs, so it should be measured against the actual
payload shape before assuming it beats HTTP JSON.

## HAT-trie Command Families

HAT-trie cache currently has 92 canonical command groups in `ExecuteCommand`,
plus Redis-style aliases for several probabilistic and compact structures. The
command set is strongest where Redis is also strong as a data-structure server:
strings, counters, TTLs, lists/queues, sets, priority queues/sorted-set-like
workloads, HyperLogLog, Bloom filters, Cuckoo filters, Count-Min Sketch, Top-K,
and quantile estimation. It also includes HAT-trie-specific exact and compact
structures that Redis/Tarantool do not expose as a core command family, such as
XOR filters, roaring bitmaps, sparse uint64 bitsets, radix-tree prefix indexes,
reservoir samples, and Fenwick trees.

| Family | Canonical HAT-trie commands |
| --- | --- |
| Generic key/value, counters, TTL, batching, replication primitives | `BATCH`, `GET`, `DUMP`, `EXISTS`, `SET`, `SETX`, `SETINT`, `SETINTX`, `INC`, `DEL`, `INTERNALSET`, `INTERNALSETV2`, `INTERNALSETV3`, `INTERNALDEL`, `INTERNALBATCH`, `INTERNALBATCHV2`, `INTERNALDIGESTV1`, `TTL`, `EXPIRE`, `EXPIREAT` |
| Map/hash fields | `PUTMAP`, `PEEKMAP`, `TAKEMAP` |
| Slice/list/deque | `PUSHSLICE`, `POPSLICE`, `SHIFTSLICE`, `HEADSLICE`, `TAILSLICE` |
| Set | `ADDSET`, `REMSET`, `HASSET`, `GETSET` |
| Priority queue | `PUSHPQ`, `PEEKPQ`, `POPPQ`, `GETPQ` |
| Bloom filter | `CREATEBF`, `ADDBF`, `HASBF`, `INFOBF` |
| Cuckoo filter | `CREATECF`, `ADDCF`, `HASCF`, `DELCF`, `INFOCF` |
| XOR filter | `CREATEXF`, `ADDXF`, `BUILDXF`, `HASXF`, `INFOXF` |
| Roaring bitmap | `CREATERB`, `ADDRB`, `REMRB`, `HASRB`, `COUNTRB`, `GETRB`, `INFORB` |
| Sparse uint64 bitset | `CREATESB`, `ADDSB`, `REMSB`, `HASSB`, `COUNTSB`, `GETSB`, `INFOSB` |
| Radix-tree prefix index | `CREATERT`, `PUTRT`, `GETRT`, `DELRT`, `HASRT`, `PREFIXRT`, `INFORT` |
| Count-Min Sketch | `CREATECMS`, `INCRCMS`, `ESTCMS`, `INFOCMS` |
| HyperLogLog | `CREATEHLL`, `ADDHLL`, `COUNTHLL`, `INFOHLL` |
| Top-K heavy hitters | `CREATETOPK`, `ADDTOPK`, `ESTTOPK`, `GETTOPK`, `INFOTOPK` |
| Reservoir sample | `CREATERS`, `ADDRS`, `GETRS`, `INFORS` |
| Quantile sketch | `CREATEQ`, `ADDQ`, `ESTQ`, `INFOQ` |
| Fenwick tree | `CREATEFW`, `ADDFW`, `GETFW`, `SUMFW`, `RANGEFW`, `INFOFW` |

<!-- BEGIN GENERATED COMMAND BENCHMARK RAW RESULTS -->
## Raw Results

### Raw HAT-trie Comparable Result

```text
HAT-trie benchmark: bench=^BenchmarkCommandFeature/(StringSet|StringGet|CounterInc|TTLExpire|MapPut|MapPeek|SlicePushPop|SetAddHas|PriorityQueuePushPop|RoaringAdd|RoaringHas|SparseBitsetAdd|SparseBitsetHas|RadixPut|RadixPrefix|ReplicationDump)$ benchtime=1000000x count=1

goos: linux
goarch: amd64
pkg: hatrie_cache
cpu: AMD Ryzen 9 5950X 16-Core Processor
BenchmarkCommandFeature/StringSet-32                    1000000       483.2 ns/op        8 B/op       1 allocs/op
BenchmarkCommandFeature/StringGet-32                    1000000       262.0 ns/op        0 B/op       0 allocs/op
BenchmarkCommandFeature/CounterInc-32                   1000000       522.1 ns/op        7 B/op       0 allocs/op
BenchmarkCommandFeature/TTLExpire-32                    1000000       729.0 ns/op       99 B/op       1 allocs/op
BenchmarkCommandFeature/MapPut-32                       1000000       363.6 ns/op       16 B/op       1 allocs/op
BenchmarkCommandFeature/MapPeek-32                      1000000       271.3 ns/op        0 B/op       0 allocs/op
BenchmarkCommandFeature/SlicePushPop-32                 1000000       642.5 ns/op       16 B/op       1 allocs/op
BenchmarkCommandFeature/SetAddHas-32                    1000000      1238 ns/op        112 B/op       9 allocs/op
BenchmarkCommandFeature/PriorityQueuePushPop-32         1000000      1941 ns/op        168 B/op       8 allocs/op
BenchmarkCommandFeature/RoaringAdd-32                   1000000       436.0 ns/op        4 B/op       1 allocs/op
BenchmarkCommandFeature/RoaringHas-32                   1000000       279.3 ns/op        0 B/op       0 allocs/op
BenchmarkCommandFeature/SparseBitsetAdd-32              1000000       454.5 ns/op        8 B/op       1 allocs/op
BenchmarkCommandFeature/SparseBitsetHas-32              1000000       281.1 ns/op        0 B/op       0 allocs/op
BenchmarkCommandFeature/RadixPut-32                     1000000       313.1 ns/op       16 B/op       1 allocs/op
BenchmarkCommandFeature/RadixPrefix-32                  1000000      3243 ns/op       1468 B/op      20 allocs/op
BenchmarkCommandFeature/ReplicationDump-32              1000000       478.2 ns/op       64 B/op       1 allocs/op
PASS

Memory summary:

| Metric | Value |
| --- | ---: |
| Max resident set size | 30140 KiB |
| Benchmark process wall time | 0:11.98 |
```

### Raw HAT-trie HyperLogLog Result

```text
HAT-trie benchmark: bench=^BenchmarkCommandFeature/(HyperLogLogAdd|HyperLogLogCount)$ benchtime=1000000x count=1

BenchmarkCommandFeature/HyperLogLogAdd-32       1000000      6223 ns/op      64 B/op       4 allocs/op
BenchmarkCommandFeature/HyperLogLogCount-32     1000000      5401 ns/op       0 B/op       0 allocs/op
PASS

Memory summary:

| Metric | Value |
| --- | ---: |
| Max resident set size | 27692 KiB |
| Benchmark process wall time | 0:11.64 |
```

### Raw HAT-trie Reservoir Fast Path Result

```text
HAT-trie benchmark: bench=^BenchmarkCommandFeature/ReservoirSampleAdd$ benchtime=1000000x count=1

goos: linux
goarch: amd64
pkg: hatrie_cache
cpu: AMD Ryzen 9 5950X 16-Core Processor
BenchmarkCommandFeature/ReservoirSampleAdd-32          1000000       465.3 ns/op       64 B/op       1 allocs/op
PASS

Memory summary:

| Metric | Value |
| --- | ---: |
| Max resident set size | 28612 KiB |
| Benchmark process wall time | 0:00.47 |
```

### Raw Tarantool Result

```text
Tarantool benchmark: version=2.6.0-0-g47aa4e01e requests=1000000 keyspace=10000

| Feature family | Tarantool operation | Seconds / 10k feature cycles |
| --- | --- | ---: |
| String write | `space:replace()` | 0.010344 s |
| String read | `space.index.primary:get()` | 0.005154 s |
| Integer counter | `space:update({{"+", 2, 1}})` | 0.013184 s |
| TTL update | `space:update({{"=", 3, expires_at}})` | 0.016871 s |
| Map/hash write | `space:replace({key, field, value})` | 0.007924 s |
| Map/hash read | `space.index.primary:get({key, field})` | 0.025777 s |
| List/deque push+pop | `space:replace() + space:delete()` | 0.014026 s |
| Set add+has | `space:replace() + space.index.primary:get()` | 0.021243 s |
| Priority queue push+pop | `tree index insert + index:min() + delete` | 0.038649 s |
| Roaring bitmap add approximation | `space:replace() membership index` | 0.007246 s |
| Roaring bitmap lookup approximation | `space.index.primary:get() membership index` | 0.019767 s |
| Sparse bitset add approximation | `space:replace() membership index` | 0.008197 s |
| Sparse bitset lookup approximation | `space.index.primary:get() membership index` | 0.010770 s |
| Radix-tree put approximation | `space:replace() tree string key` | 0.010393 s |
| Radix-tree prefix scan approximation | `index:pairs(prefix, {iterator = "GE"})` | 0.189574 s |
| Replication dump | `msgpack.encode(tuple)` | 0.040829 s |

Memory summary:

| Metric | Value |
| --- | ---: |
| Process RSS | 35484 KiB |
| memtx_memory configured | 1048576 KiB |
| slab quota used | 32768 KiB |
| slab quota size | 1048576 KiB |
| slab arena used | 4463 KiB |
| slab arena size | 32768 KiB |
| slab items used | 1519 KiB |
| slab items size | 2115 KiB |
```

### Raw Redis Result

```text
Redis benchmark: host=127.0.0.1 port=6380 requests=10000 clients=1 keyspace=10000

| Feature family | Redis command | Throughput | Seconds / 10k ops |
| --- | --- | ---: | ---: |
| String write | `SET hatriebench:639144:string value` | 8312.55 req/s | 1.203000 s |
| String read | `GET hatriebench:639144:string:__rand_int__` | 10020.04 req/s | 0.998000 s |
| Integer counter | `INCR hatriebench:639144:counter` | 10351.97 req/s | 0.966000 s |
| TTL update | `EXPIRE hatriebench:639144:ttl 3600` | 9940.36 req/s | 1.006000 s |
| Hash/map write | `HSET hatriebench:639144:hash field value` | 7716.05 req/s | 1.296000 s |
| Hash/map read | `HGET hatriebench:639144:hash field` | 7158.20 req/s | 1.396999 s |
| List push | `LPUSH hatriebench:639144:list value` | 10183.30 req/s | 0.982000 s |
| List pop | `RPOP hatriebench:639144:list:pop` | 9090.91 req/s | 1.100000 s |
| Set add | `SADD hatriebench:639144:set value` | 11148.27 req/s | 0.897000 s |
| Set membership | `SISMEMBER hatriebench:639144:set value` | 10660.98 req/s | 0.938000 s |
| Sorted-set add | `ZADD hatriebench:639144:zset 10 value` | 9302.33 req/s | 1.074999 s |
| Sorted-set pop | `ZPOPMIN hatriebench:639144:zset:pop` | 8756.57 req/s | 1.142000 s |
| HyperLogLog add | `PFADD hatriebench:639144:hll value` | 9587.73 req/s | 1.043000 s |
| HyperLogLog count | `PFCOUNT hatriebench:639144:hll` | 8431.70 req/s | 1.186000 s |
| Bitmap add | `SETBIT hatriebench:639144:bitmap 65543 1` | 9803.92 req/s | 1.020000 s |
| Bitmap lookup | `GETBIT hatriebench:639144:bitmap 65543` | 9174.31 req/s | 1.090000 s |
| Replication dump | `DUMP hatriebench:639144:string` | 9191.18 req/s | 1.088000 s |

Memory summary:

| Metric | Value |
| --- | ---: |
| used_memory | 2494304 B |
| used_memory_rss | 8716288 B |
| used_memory_peak | 3171296 B |
```

<!-- END GENERATED COMMAND BENCHMARK RAW RESULTS -->
## Gaps Versus Redis

HAT-trie cache intentionally does not try to implement the entire Redis command
reference. Notable Redis-native families that are absent or only approximated:

- Pub/Sub, streams, consumer groups, and time-series commands.
- Transactions, Lua/functions, ACLs, modules API, server management, and cluster
  management commands.
- Geospatial indexes and vector sets.
- Full sorted-set algebra, blocking list/sorted-set commands, set algebra, and
  multi-key operations.
- Redis JSON path updates and search/query-engine commands.

## Gaps Versus Tarantool

Tarantool's strength is broader database/application-server programmability.
HAT-trie cache does not provide these Tarantool-style primitives:

- Arbitrary spaces, schemas, tuple formats, secondary index definitions, and SQL.
- Lua stored procedures as the primary extension model.
- General transactions across multiple tuple operations.
- Built-in database privilege management and role grants.

HAT-trie cache instead focuses on a fixed cache command API with many built-in
in-memory data structures and compact serialization/storage paths.
