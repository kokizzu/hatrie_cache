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

## Memory Summary

| System | Run | Memory metric | Value |
| --- | --- | --- | ---: |
| HAT-trie cache | comparable command benchmark, Go test binary | max resident set size | 35,808 KiB |
| HAT-trie cache | HyperLogLog command benchmark, Go test binary | max resident set size | 27,692 KiB |
| Tarantool 2.6.0 | 1,000,000 feature cycles, 10,000 keyspace | process RSS | 35,708 KiB |
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
| String write | `BenchmarkCommandFeature/StringSet` | 0.004865 s | 8 B/op | `space:replace()` | 0.008629 s | 1.77x |
| String read | `BenchmarkCommandFeature/StringGet` | 0.002133 s | 0 B/op | `space.index.primary:get()` | 0.005363 s | 2.51x |
| Integer counter | `BenchmarkCommandFeature/CounterInc` | 0.003922 s | 7 B/op | `space:update({{"+", 2, 1}})` | 0.013611 s | 3.47x |
| TTL update | `BenchmarkCommandFeature/TTLExpire` | 0.006242 s | 99 B/op | `space:update({{"=", 3, expires_at}})` | 0.018005 s | 2.88x |
| Map/hash write | `BenchmarkCommandFeature/MapPut` | 0.012240 s | 474 B/op | `space:replace({key, field, value})` | 0.010979 s | 0.90x |
| Map/hash read | `BenchmarkCommandFeature/MapPeek` | 0.003068 s | 0 B/op | `space.index.primary:get({key, field})` | 0.005224 s | 1.70x |
| List/deque push+pop | `BenchmarkCommandFeature/SlicePushPop` | 0.014660 s | 152 B/op | `space:replace() + space:delete()` | 0.018650 s | 1.27x |
| Set add+has | `BenchmarkCommandFeature/SetAddHas` | 0.011850 s | 112 B/op | `space:replace() + space.index.primary:get()` | 0.017228 s | 1.45x |
| Priority queue push+pop | `BenchmarkCommandFeature/PriorityQueuePushPop` | 0.016440 s | 168 B/op | `tree index insert + index:min() + delete` | 0.026943 s | 1.64x |
| Roaring bitmap add | `BenchmarkCommandFeature/RoaringAdd` | 0.004351 s | 4 B/op | `space:replace() membership index` | 0.009881 s | 2.27x |
| Roaring bitmap lookup | `BenchmarkCommandFeature/RoaringHas` | 0.003011 s | 0 B/op | `space.index.primary:get() membership index` | 0.014427 s | 4.79x |
| Sparse uint64 bitset add | `BenchmarkCommandFeature/SparseBitsetAdd` | 0.004773 s | 8 B/op | `space:replace() membership index` | 0.011004 s | 2.31x |
| Sparse uint64 bitset lookup | `BenchmarkCommandFeature/SparseBitsetHas` | 0.003227 s | 0 B/op | `space.index.primary:get() membership index` | 0.019058 s | 5.91x |
| Radix-tree put | `BenchmarkCommandFeature/RadixPut` | 0.015020 s | 482 B/op | `space:replace() tree string key` | 0.022540 s | 1.50x |
| Radix-tree prefix scan | `BenchmarkCommandFeature/RadixPrefix` | 0.032590 s | 1,468 B/op | `index:pairs(prefix, {iterator = "GE"})` | 0.262052 s | 8.04x |
| Replication dump | `BenchmarkCommandFeature/ReplicationDump` | 0.004086 s | 64 B/op | `msgpack.encode(tuple)` | 0.036781 s | 9.00x |

In this run HAT-trie is faster on 15 of the 16 measured Tarantool-comparable
rows. `MapPut` remains the current Tarantool-winning row.

## HAT-trie vs Redis

Redis was measured with Redis 7.0.4 in a temporary Docker container, one
client, no pipeline, and 10,000 requests per command. Rows with two Redis
commands add the two Redis seconds-per-10k values before computing speedup.

| Feature family | HAT-trie benchmark | HAT-trie seconds / 10k | Redis measured command | Redis seconds / 10k | Redis/HAT speedup |
| --- | --- | ---: | --- | ---: | ---: |
| String write | `BenchmarkCommandFeature/StringSet` | 0.004865 s | `SET` | 1.203000 s | 247.28x |
| String read | `BenchmarkCommandFeature/StringGet` | 0.002133 s | `GET` | 0.998000 s | 467.89x |
| Integer counter | `BenchmarkCommandFeature/CounterInc` | 0.003922 s | `INCR` | 0.966000 s | 246.30x |
| TTL update | `BenchmarkCommandFeature/TTLExpire` | 0.006242 s | `EXPIRE` | 1.006000 s | 161.17x |
| Map/hash write | `BenchmarkCommandFeature/MapPut` | 0.012240 s | `HSET` | 1.296000 s | 105.88x |
| Map/hash read | `BenchmarkCommandFeature/MapPeek` | 0.003068 s | `HGET` | 1.396999 s | 455.35x |
| List/deque push+pop | `BenchmarkCommandFeature/SlicePushPop` | 0.014660 s | `LPUSH` + `RPOP` | 2.082000 s | 142.02x |
| Set add+has | `BenchmarkCommandFeature/SetAddHas` | 0.011850 s | `SADD` + `SISMEMBER` | 1.835000 s | 154.85x |
| Priority queue push+pop | `BenchmarkCommandFeature/PriorityQueuePushPop` | 0.016440 s | `ZADD` + `ZPOPMIN` | 2.216999 s | 134.85x |
| Roaring bitmap add approximation | `BenchmarkCommandFeature/RoaringAdd` | 0.004351 s | `SETBIT` bitmap, not roaring | 1.020000 s | 234.43x |
| Roaring bitmap lookup approximation | `BenchmarkCommandFeature/RoaringHas` | 0.003011 s | `GETBIT` bitmap, not roaring | 1.090000 s | 362.01x |
| Sparse uint64 bitset add approximation | `BenchmarkCommandFeature/SparseBitsetAdd` | 0.004773 s | `SETBIT` dense bitmap approximation | 1.020000 s | 213.70x |
| Sparse uint64 bitset lookup approximation | `BenchmarkCommandFeature/SparseBitsetHas` | 0.003227 s | `GETBIT` dense bitmap approximation | 1.090000 s | 337.78x |
| HyperLogLog add | `BenchmarkCommandFeature/HyperLogLogAdd` | 0.062230 s | `PFADD` | 1.043000 s | 16.76x |
| HyperLogLog count | `BenchmarkCommandFeature/HyperLogLogCount` | 0.054010 s | `PFCOUNT` | 1.186000 s | 21.96x |
| Replication dump | `BenchmarkCommandFeature/ReplicationDump` | 0.004086 s | `DUMP` | 1.088000 s | 266.28x |

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
| Generic key/value, counters, TTL, replication primitives | `GET`, `DUMP`, `EXISTS`, `SET`, `SETX`, `SETINT`, `SETINTX`, `INC`, `DEL`, `INTERNALSET`, `INTERNALDEL`, `TTL`, `EXPIRE`, `EXPIREAT` |
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

## Raw Results

### Raw HAT-trie Comparable Result

```text
HAT-trie benchmark: bench=^BenchmarkCommandFeature/(StringSet|StringGet|CounterInc|TTLExpire|MapPut|MapPeek|SlicePushPop|SetAddHas|PriorityQueuePushPop|RoaringAdd|RoaringHas|SparseBitsetAdd|SparseBitsetHas|RadixPut|RadixPrefix|ReplicationDump)$ benchtime=1000000x count=1

goos: linux
goarch: amd64
pkg: hatrie_cache
cpu: AMD Ryzen 9 5950X 16-Core Processor
BenchmarkCommandFeature/StringSet-32                    1000000       486.5 ns/op        8 B/op       1 allocs/op
BenchmarkCommandFeature/StringGet-32                    1000000       213.3 ns/op        0 B/op       0 allocs/op
BenchmarkCommandFeature/CounterInc-32                   1000000       392.2 ns/op        7 B/op       0 allocs/op
BenchmarkCommandFeature/TTLExpire-32                    1000000       624.2 ns/op       99 B/op       1 allocs/op
BenchmarkCommandFeature/MapPut-32                       1000000      1224 ns/op        474 B/op       5 allocs/op
BenchmarkCommandFeature/MapPeek-32                      1000000       306.8 ns/op        0 B/op       0 allocs/op
BenchmarkCommandFeature/SlicePushPop-32                 1000000      1466 ns/op        152 B/op       6 allocs/op
BenchmarkCommandFeature/SetAddHas-32                    1000000      1185 ns/op        112 B/op       9 allocs/op
BenchmarkCommandFeature/PriorityQueuePushPop-32         1000000      1644 ns/op        168 B/op       8 allocs/op
BenchmarkCommandFeature/RoaringAdd-32                   1000000       435.1 ns/op        4 B/op       1 allocs/op
BenchmarkCommandFeature/RoaringHas-32                   1000000       301.1 ns/op        0 B/op       0 allocs/op
BenchmarkCommandFeature/SparseBitsetAdd-32              1000000       477.3 ns/op        8 B/op       1 allocs/op
BenchmarkCommandFeature/SparseBitsetHas-32              1000000       322.7 ns/op        0 B/op       0 allocs/op
BenchmarkCommandFeature/RadixPut-32                     1000000      1502 ns/op        482 B/op       5 allocs/op
BenchmarkCommandFeature/RadixPrefix-32                  1000000      3259 ns/op       1468 B/op      20 allocs/op
BenchmarkCommandFeature/ReplicationDump-32              1000000       408.6 ns/op       64 B/op       1 allocs/op
PASS

Memory summary:

| Metric | Value |
| --- | ---: |
| Max resident set size | 35808 KiB |
| Benchmark process wall time | 0:14.30 |
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

### Raw Tarantool Result

```text
Tarantool benchmark: version=2.6.0-0-g47aa4e01e requests=1000000 keyspace=10000

| Feature family | Tarantool operation | Seconds / 10k feature cycles |
| --- | --- | ---: |
| String write | `space:replace()` | 0.008629 s |
| String read | `space.index.primary:get()` | 0.005363 s |
| Integer counter | `space:update({{"+", 2, 1}})` | 0.013611 s |
| TTL update | `space:update({{"=", 3, expires_at}})` | 0.018005 s |
| Map/hash write | `space:replace({key, field, value})` | 0.010979 s |
| Map/hash read | `space.index.primary:get({key, field})` | 0.005224 s |
| List/deque push+pop | `space:replace() + space:delete()` | 0.018650 s |
| Set add+has | `space:replace() + space.index.primary:get()` | 0.017228 s |
| Priority queue push+pop | `tree index insert + index:min() + delete` | 0.026943 s |
| Roaring bitmap add approximation | `space:replace() membership index` | 0.009881 s |
| Roaring bitmap lookup approximation | `space.index.primary:get() membership index` | 0.014427 s |
| Sparse bitset add approximation | `space:replace() membership index` | 0.011004 s |
| Sparse bitset lookup approximation | `space.index.primary:get() membership index` | 0.019058 s |
| Radix-tree put approximation | `space:replace() tree string key` | 0.022540 s |
| Radix-tree prefix scan approximation | `index:pairs(prefix, {iterator = "GE"})` | 0.262052 s |
| Replication dump | `msgpack.encode(tuple)` | 0.036781 s |

Memory summary:

| Metric | Value |
| --- | ---: |
| Process RSS | 35708 KiB |
| memtx_memory configured | 1048576 KiB |
| slab quota used | 32768 KiB |
| slab quota size | 1048576 KiB |
| slab arena used | 4463 KiB |
| slab arena size | 32768 KiB |
| slab items used | 1519 KiB |
| slab items size | 2131 KiB |
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
