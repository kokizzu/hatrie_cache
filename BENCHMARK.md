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

Large HAT-trie comparable command rows:

```
make bench-hatrie-command-features HATRIE_COMMAND_BENCH='^BenchmarkCommandFeature/(StringSet|StringGet|CounterInc|TTLExpire|MapPut|MapPeek|SlicePushPop|SetAddHas|PriorityQueuePushPop|RoaringAdd|RoaringHas|SparseBitsetAdd|SparseBitsetHas|RadixPut|RadixPrefix|ReplicationDump)$' BENCHTIME=1000000x
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
make bench-redis-command-features REDIS_START_DOCKER=1 REDIS_PORT=6380 REDIS_REQUESTS=10000 REDIS_CLIENTS=1 REDIS_KEYSPACE=10000
```

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
make bench-redis-command-features BENCHMARK_ARTIFACT_DIR=build/benchmarks REDIS_START_DOCKER=1 REDIS_PORT=6380 REDIS_REQUESTS=10000
make bench-tarantool-command-features BENCHMARK_ARTIFACT_DIR=build/benchmarks TARANTOOL_REQUESTS=10000 TARANTOOL_KEYSPACE=10000
make bench-command-comparison BENCHMARK_ARTIFACT_DIR=build/benchmarks
```

The artifact directory receives `hatrie-command-features.tsv`,
`redis-command-features.tsv`, `tarantool-command-features.tsv`, matching memory
TSV files, raw Markdown output, and generated
`command-feature-comparison.md`.

HAT-trie end-to-end transport rows:

```
make bench-hatrie-transport-features HATRIE_TRANSPORT_BENCH='^BenchmarkCommandTransportFeature/(InProcess|HTTPJSON|HTTPProtobuf|GRPC)/(StringSet|StringGet|CounterInc|MapPut|MapPeek)$' BENCHTIME=100x
```

The transport benchmark uses the same command execution semantics as the
monitoring HTTP API and native gRPC API. HTTP protobuf uses
`application/x-protobuf` on `/api/commands`; gRPC uses the generated
`CacheService.Command` RPC over a local bufconn listener.

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
go test . -run NoTestsForBenchmark -bench BenchmarkHTTPReplicatorSyncAllBatching -benchmem -benchtime=1x
```

`BenchmarkHTTPReplicatorSyncAllBatching` syncs 10,000 leader-owned keys to one
local HTTP target. `Batched10k` uses one SyncAll page and native
`INTERNALBATCH`; `Unbatched10k` uses page size 1 to model the previous
one-request-per-key path.

| Mode | Time/op | requests/op | wire_B/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Batched 10k | 153,473,837 ns | 1 | 147,521 | 56,366,320 | 980,814 |
| Unbatched 10k | 51,455,645,995 ns | 10,000 | 2,135,564 | 1,794,046,848 | 202,050,916 |

The batching request reduction is 10,000x for this single-target sync. In this
local benchmark the batched path is about 335x faster, sends about 14.5x fewer
request-body bytes, uses about 31.8x less heap, and performs about 206x fewer
allocations. Header bytes are not included in `wire_B/op`, so the real network
savings are larger than the body-only metric.

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
| Generic key/value, counters, TTL, replication primitives | `GET`, `DUMP`, `EXISTS`, `SET`, `SETX`, `SETINT`, `SETINTX`, `INC`, `DEL`, `INTERNALSET`, `INTERNALDEL`, `INTERNALBATCH`, `TTL`, `EXPIRE`, `EXPIREAT` |
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
