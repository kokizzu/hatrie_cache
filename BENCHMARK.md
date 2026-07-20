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
make bench-hatrie-command-features HATRIE_COMMAND_BENCH='^BenchmarkCommandFeature/(StringSet|PipelineBatch16|StringGet|CounterInc|TTLExpire|MapPut|MapPeek|SlicePushPop|SetAddHas|PriorityQueuePushPop|RoaringAdd|RoaringHas|SparseBitsetAdd|SparseBitsetHas|RadixPut|RadixPrefix|ReplicationDump)$' BENCHTIME=1000000x
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
| Current pass | [Generation-based Pebble full save](#pebble-generation-full-save), 10k x 256 B | Legacy Pebble batch: 18.369 ms; 21.05 MB heap; 598.0 disk B/key | Generation SST: 24.651 ms; 9.61 MB heap; 299.6 disk B/key | 2.19x lower heap, 2.00x smaller disk, 10,680x less WAL | Full-save latency is 1.34x higher |
| Current pass | [Parallel cold-reference hydration](#parallel-cold-reference-hydration), 32 delayed reads | Serialized: 33.875 ms; 18,648 heap B | Parallel singleflight: 1.174 ms; 30,166 heap B | 28.85x faster | Cumulative heap is 1.62x higher and allocations are 1.80x higher |
| Current pass | [Compact lazy-reference slab](#compact-lazy-reference-slab), 100k references | Public-struct slab: 29.617 ms; 90.2 retained B/ref | Compact slab: 20.513 ms; 71.6 retained B/ref | 1.44x faster, 1.26x lower retained heap | Type IDs are internal; exported references are expanded on access |
| Current pass | [Persistent storage backend bakeoff](#persistent-storage-backend-bakeoff), 10k x 256 B plus 1k churn | LevelDB: 91.602 ms cycle; 41.52 MB heap; 265.3 disk B/key | Pebble: 98.273 ms cycle; 20.52 MB heap; 285.7 disk B/key | 2.02x lower cumulative heap; disk is within 1.08x | LevelDB completes the mixed cycle 1.07x faster |
| Earlier | [Replication request batching](#replication-batching-benchmark), 10k keys | Historical: 51,455,645,995 ns; 10,000 requests | First batched baseline: 162,195,812 ns; 1 request | About 317x faster, 10,000x fewer requests | Historical rows came from separate controlled runs |
| Earlier | [Replication routing and encoding](#replication-batching-benchmark), 10k keys | 162,195,812 ns; 144,227 wire B; 57,035,706 heap B | 18,893,092 ns; 55,795 wire B; 948,495 heap B | 8.58x faster, 2.59x smaller wire, 60.13x lower heap | Compact paths retain legacy materialization fallbacks |
| Earlier | [Replication page traversal](#replication-page-traversal), 10 pages | 61,122,327 ns; 1,877,005 heap B; 123,996 allocs | 19,709,083 ns; 999,805 heap B; 11,885 allocs | 3.10x faster, 1.88x lower heap, 10.43x fewer allocs | Mutation invalidates and safely restarts the cursor |
| Earlier | [gRPC replication transport](#replication-transport), 10k keys | HTTP: 44,957,163 ns; 57,479 wire B | gRPC: 37,765,365 ns; 52,006 wire B | 1.19x faster, 9.52% smaller wire, 24.41% fewer allocs | Cumulative heap is 16.18% higher; HTTP remains fallback |
| Earlier | [Bounded gzip writer cache](#replication-compression-tradeoff), 50 syncs | 15.23 MB compressor allocation | 1.14 MB | 13.4x less compressor allocation | Retains at most four initialized writers |
| Earlier | [Four-target replication fanout](#replication-target-fanout) | Serial: 9,544,371 ns | Bound 4: 2,617,552 ns | 3.65x faster | 1.15x cumulative heap and 12 more allocations |
| Earlier | [Journal delta durability](#journal-delta-first-recovery-benchmark), 100 records | Per-command fsync: 0.122684 s | One batch fsync: 0.002170 s | 56.55x faster | Filesystem fsync latency is host/load sensitive |
| Earlier | [Retained journal catch-up](#journal-delta-first-recovery-benchmark) | Exact 10k snapshot: 0.092649 s; 25,709,960 heap B | 100 deltas: 0.002170 s; 163,918 heap B | 42.70x faster, 156.85x lower heap, 5.97x smaller wire | Snapshot remains required after journal compaction gaps |
| Earlier | [Two-value small-set read](#collection-allocation-follow-up) | 155.5 ns; 48 B; 3 allocs | 54.46 ns; 32 B; 1 alloc | 2.86x faster, 1.50x lower heap, 3x fewer allocs | Promotes to a map at three entries |
| Earlier | [Priority queue push+pop](#collection-allocation-follow-up) | 875.9 ns; 56 B; 3 allocs | 769.1 ns; 40 B; 2 allocs | 1.14x faster, 1.40x lower heap | Typed string fast path retains generic fallback |
| Earlier | [Radix prefix scan](#collection-allocation-follow-up) | 3,979 ns; 1,468 B; 20 allocs | 1,972 ns; 1,024 B; 1 alloc | 2.02x faster, 1.43x lower heap, 20x fewer allocs | Escaped/non-string values use generic JSON encoding |
| Earlier | [Reservoir sample add](#collection-allocation-follow-up) | 956.7 ns; 168 B; 6 allocs | 465.3 ns; 64 B; 1 alloc | 2.06x faster, 2.63x lower heap, 6x fewer allocs | Fast path applies to plain strings |
| Final architecture | [Per-key telemetry](#per-key-telemetry-modes), 100k keys | 242.5 retained B/key, unbounded | 63.57 retained B/key, off by default | 73.8% lower memory, 3.81x efficiency | `StatsForKey` requires explicit bounded/full opt-in |
| Current pass | [Atomic cache-wide telemetry](#atomic-cache-wide-telemetry), 32 readers | 222.0 ns/read | 93.21 ns/read | 2.38x faster | Adds 64 fixed bytes/cache; detailed key telemetry retains its mutex |
| Final architecture | [Concurrent scalar reads](#concurrent-scalar-read-fast-path), 32 CPUs | 1,528 ns/read | 632.4 ns/read | 2.42x faster | Expiration cleanup and LevelDB hydration still take the exclusive path |
| Final architecture | [Striped existing-counter writes](#striped-existing-counter-writes), 2 writers | 362.8 ns/write | 209.7 ns/write | 1.73x faster | Opt-in; 64 stripes retain 1,536 B and semantic writes fall back |
| Final architecture | [Durable journal group commit](#durable-journal-group-commit), 16 callers | 878,909 ns/write | 73,286 ns/write | 11.99x faster | Sparse traffic can opt into a collection window; durability still precedes apply/ack |
| Current pass | [Durable public batches](#durable-public-batches), 10k writes | 9.821 s; 10,000 syncs | 29.051 ms; 3 syncs | 338x faster, 3,333x fewer syncs | Cumulative heap is 1.20x higher; ordinary item errors remain non-transactional |
| Current pass | [Native C command batching](#native-c-command-batching), 4,096 commands | Go loop: set 1.137 ms, get 1.123 ms | One C call: set 0.998 ms, get 0.979 ms | Set 1.14x faster, get 1.15x faster | Activates at 32 same-family commands; state-sensitive batches fall back |
| Current pass | [Segmented WAL compaction](#segmented-wal-compaction), 100k records | 31.462 ms; 20,810,464 heap B; 500,033 allocs | 1.845 ms; 22,256 heap B; 56 allocs | 17.06x faster, 935x lower heap, 8,929x fewer allocs | Retains bounded sidecar files; rotation adds directory metadata syncs |
| Final architecture | [Point-in-time snapshot capture](#point-in-time-snapshot-capture), 100k keys | 528,624,130 ns maximum read pause | 142,374,086 ns | 3.71x shorter pause | Total snapshot time is 5.5% higher and cumulative heap is 2.63x higher |
| Current pass | [Bounded-page snapshot capture](#bounded-page-snapshot-capture), 100k keys | 61.740 ms maximum read pause | 2.822 ms | 21.88x shorter pause | Total time and heap remain within 1% |
| Current pass | [Compact streaming snapshot capture](#compact-streaming-snapshot-capture), 100k keys | 182.221 ms; 47.61 MB heap; 97,152 KiB RSS | 151.348 ms; 24.57 MB heap; 63,104 KiB RSS | 1.20x faster, 1.94x lower heap, 1.54x lower RSS | Median maximum read pause is 7.9% higher at 3.24 ms |
| Current pass | [Delete-churn memory compaction](#delete-churn-memory-compaction), 100k insert/90k delete | 9,679,075 retained backing B; 9,850,096 retained heap B | 704,912 retained backing B; 884,600 retained heap B | 13.73x lower backing, 11.13x lower heap | One rebuild pauses access for 8.80 ms and adds 2.4% cumulative allocation to the full churn cycle |
| Current pass | [Indexed expiration heap](#indexed-expiration-heap), 100k deadline updates on one key | 250.0 ns/update; 91 B/op; 19 final heap nodes | 194.8 ns/update; 0 B/op; 1 heap node | 1.28x faster; cumulative allocation eliminated; 19x fewer final nodes | Heap index is `uint32`, limiting simultaneously scheduled TTL keys to practical in-memory sizes |
| Final architecture | [Equal-state anti-entropy](#incremental-anti-entropy), 10k x 1 KiB | 154,735,234 ns; 10,743,774 wire B | 22,129,470 ns; 215 wire B | 6.99x faster, 49,971x smaller wire | Equality still scans and hashes both replicas |
| Final architecture | [1%-changed anti-entropy](#incremental-anti-entropy), 10k x 1 KiB | Same full-transfer baseline | 72,812,784 ns; 240,086 wire B | 2.13x faster, 44.75x smaller wire | Digest pages add metadata before changed values |
| Current pass | [Merkle equal-state preflight](#hierarchical-merkle-anti-entropy), 10k x 1 KiB | Digest: 18.272 ms; 560,720 heap B | Merkle: 0.993 ms; 233,744 heap B | 18.40x faster, 2.40x lower heap | First activation builds a 29.60 B/key index |
| Current pass | [Merkle 1%-changed repair](#hierarchical-merkle-anti-entropy), 10k x 1 KiB | Digest: 55.401 ms; 240,086 wire B | Merkle: 25.443 ms; 132,820 wire B | 2.18x faster, 1.81x smaller wire | Active write tracking is 1.88x slower |
| Final architecture | [Sequential gRPC stream](#persistent-grpc-command-stream), 10k commands | Unary: 59,040 ns/command | 14,914 ns/command | 3.96x faster, 6.73x lower heap | Request/response remains sequential |
| Final architecture | [Pipelined gRPC stream](#persistent-grpc-command-stream), 10k commands | Unary: 59,040 ns/command | 3,118 ns/command | 18.94x faster, 7.67x lower heap, 6.57x fewer allocations | Requires concurrent sender/receiver with ordered response pairing |
| Current pass | [Native gRPC batch stream](#persistent-grpc-command-stream), 10k commands, batch 16 | Pipelined: 2,638 ns/command; 41.00 wire B/command | Native batch: 1,161 ns/command; 37.04 wire B/command | 2.27x faster, 1.62x lower heap, 2.77x fewer allocations, 1.11x smaller wire, 16x fewer messages | Batching can add queueing latency; client chooses envelope size |
| Current pass | [Pipelined live gRPC replication](#pipelined-live-grpc-replication), 10k writes | HTTP: 178.079 ms; 1,868,894 wire B | gRPC: 167.797 ms; 1,081,746 wire B | 1.06x faster, 1.73x smaller wire | Requires native gRPC listener; HTTP remains fallback |
| Current pass | [Live gRPC micro-batching](#pipelined-live-grpc-replication), 10k writes | 193.299 ms; 10,000 batches; 1,081,747 wire B | 149.682 ms; 2,910 batches; 368,252 wire B | 1.29x faster, 3.44x fewer batches, 2.94x smaller wire | One-caller throughput is 1.6% lower; set max commands to 1 for legacy behavior |
| Current pass | [Binary outbox encoding](#binary-grouped-replication-outbox), 4 KiB job | JSON: 8,949 ns; 5,948 B | Binary: 4,123 ns; 4,412 B | 2.17x faster, 25.8% smaller | Binary records require project tooling to inspect |
| Current pass | [Binary outbox replay](#binary-grouped-replication-outbox), 10k jobs | JSON: 217.479 ms | Binary: 87.330 ms | 2.49x faster, 1.34x fewer allocs | Existing JSON records remain readable |
| Current pass | [Bounded lazy outbox restore](#binary-grouped-replication-outbox), 100k jobs | 466.884 ms; 100,000 resident jobs; 415.1 MB heap | 5.019 ms; 1,024 resident jobs; 3.52 MB heap | 93.03x faster, 97.66x fewer resident jobs, 118.0x lower heap | LevelDB pages are lazy; legacy whole-file JSON still loads its file snapshot |
| Current pass | [Outbox group commit](#binary-grouped-replication-outbox), 32 writers | JSON sync-each: 50.289 ms; 32 syncs | Binary grouped: 3.542 ms; 1 sync | 14.20x faster, 32x fewer syncs | Cumulative heap is 1.49x higher |
| Current pass | [Journal-backed outbox](#journal-backed-replication-outbox), 10k durable 4 KiB mutations | Full LevelDB jobs: 136.854 s; 20,993 heap B/op; 2 syncs/op | Journal references: 7.845 s; 26,094 heap B/op; 1 sync/op | 17.44x faster, 2x fewer syncs | Total encoded/disk bytes are effectively unchanged; cumulative heap is 1.24x higher |

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

The direct scalar follow-up replaces repeated request/response messages with
packed operation, status, and value-kind columns plus one concatenated result
buffer. Both rows below use 16-command envelopes and are seven-run medians from
the same Ryzen 9 5950X checkout. The generic row is measured again alongside
the new path rather than copied from the older run above.

| 10,000 GET commands | Time/10k | ns/command | Heap B/10k | Allocs/10k | Wire B/command | Improvement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `CommandBatchStream` | 8.877 ms | 887.7 | 9,671,528 | 107,685 | 37.04 | baseline |
| `ScalarBatchStream` | 3.698 ms | 369.7 | 2,646,720 | 40,491 | 23.72 | 2.40x CPU, 3.65x heap, 2.66x allocations, 1.56x wire |

The direct path supports GET, EXISTS, string/counter set, counter increment,
and delete. It preserves ordered per-command statuses. Servers configured with
journaling, dirty persistence, replication, or leader enforcement route typed
columns through the existing transactional side-effect executor, retaining
correctness at the cost of some direct-path savings. The existing command batch
stream remains the fallback for structured commands and older clients.

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

### Hierarchical Merkle Anti-Entropy

The unfiltered single-shard path now compares a 1,024-leaf incremental Merkle
root before requesting key digests. Equal replicas need one fixed-size request;
a sparse mismatch fetches only differing leaves. Prefix and multi-shard syncs
retain the compatible sorted-digest implementation.

```sh
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleIncremental -benchtime=1x -count=5 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleIndexBuild -benchtime=1x -count=5 -benchmem'
make run CMD='go test . -run=NoSuchTest -bench=BenchmarkReplicationMerkleWriteTracking -benchtime=100000x -count=5 -benchmem'
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

The reservoir sample add path now has a plain-string fast path that hashes the
JSON string representation directly and only boxes retained values. The focused
1,000,000-iteration row was:

```
make bench-hatrie-command-features HATRIE_COMMAND_BENCH='^BenchmarkCommandFeature/ReservoirSampleAdd$' BENCHTIME=1000000x
```

| Feature | Benchmark row | Before | After | Improvement |
| --- | --- | ---: | ---: | ---: |
| Reservoir sample add | `BenchmarkCommandFeature/ReservoirSampleAdd` | 956.7 ns, 168 B, 6 allocs | 465.3 ns, 64 B, 1 alloc | 2.06x faster, 2.63x less memory, 6.00x fewer allocs |

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
make run CMD='go test -run=NONE -bench=BenchmarkHTTPReplicatorSyncAllBatching/Batched10k -benchmem -benchtime=20x -count=10'
```

`BenchmarkHTTPReplicatorSyncAllBatching` syncs 10,000 leader-owned keys to one
local HTTP target. `Batched10k` uses one SyncAll page and native protobuf
replication. The latest start and final rows are medians from identical ten-run
commands on the same AMD Ryzen 9 5950X host. Older rows are retained from their
original controlled runs.

| Mode | Time/op | requests/op | wire_B/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before optimization (`b897b64`) | 162,195,812 ns | 1 | 144,227 | 57,035,706 | 1,040,310 |
| Start of this pass (`10cf4c8`) | 39,010,494 ns | 1 | 87,095 | 11,982,835 | 131,264 |
| Previous optimized (`c70d849`) | 31,830,774 ns | 1 | 55,792 | 5,668,513 | 50,753 |
| Start of latest pass (`84325af`) | 28,554,528 ns | 1 | 55,794 | 5,781,645 | 50,756 |
| Final optimized (`69a6018`) | 18,893,092 ns | 1 | 55,795 | 948,495 | 30,197 |
| Current optimized (`e5b127d`) | 15,698,676 ns | 1 | 55,794 | 847,763 | 10,241 |
| Historical unbatched 10k | 51,455,645,995 ns | 10,000 | 2,135,564 | 1,794,046,848 | 202,050,916 |

The latest five-feature pass is 1.51x faster, uses 6.10x less cumulative allocated heap,
and performs 1.68x fewer allocations while keeping request-body
bytes effectively unchanged. From `10cf4c8`, the cumulative result is 2.06x
faster, 1.56x fewer body bytes, 12.63x less allocated heap, and 4.35x fewer
allocations. Against the earlier `b897b64` batched baseline, it is 8.58x faster,
sends 2.59x fewer body bytes, uses 60.13x less allocated heap, and performs
34.45x fewer allocations. The historical batching request reduction is 10,000x
for this single-target sync. Header bytes are not included in `wire_B/op`, so the real
network savings from batching are larger than the body-only metric. `B/op`
measures bytes allocated during one operation, not peak process RSS.

The current row adds persistent generation-checked page cursors, packed page
arenas, and 256-record native iterator batches. Against `69a6018`, it is 1.20x
faster, uses 1.12x less cumulative heap, and performs 2.95x fewer allocations.
Against `10cf4c8`, the cumulative result is 2.49x faster, 1.56x smaller on wire,
14.14x lower in allocated heap, and 12.82x lower in allocations.

### Replication Page Traversal

The default 1,000-key page benchmark sends ten ordered pages for 10,000 keys.
Medians use `-benchtime=20x`; the current row is a seven-run median.

| Version | Time/op | requests/op | B/op | allocs/op | Cumulative speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before persistent cursor (`471c229`) | 61,122,327 ns | 10 | 1,877,005 | 123,996 | 1.00x |
| Current (`e5b127d`) | 19,709,083 ns | 10 | 999,805 | 11,885 | 3.10x |

The current default-page path uses 1.88x less cumulative heap and 10.43x fewer
allocations. The native iterator returns up to 256 records per cgo crossing, so
a 10,000-key scan needs about 40 batch calls instead of one crossing per key.

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
