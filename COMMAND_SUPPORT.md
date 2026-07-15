# Command Support Comparison

This compares the cache command surface exposed by `POST /api/commands` and
`make cli ARGS='command ...'` with comparable Redis and Tarantool feature
families. It is a feature/command coverage matrix, not a wire-protocol
compatibility statement.

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

## Summary

HAT-trie cache currently has 92 canonical command groups in `ExecuteCommand`,
plus Redis-style aliases for several probabilistic and compact structures. The
benchmark table below is organized by feature family and lists the benchmark
that exercises that feature. The command set is strongest where Redis is also
strong as a data-structure server:
strings, counters, TTLs, lists/queues, sets, priority queues/sorted-set-like
workloads, HyperLogLog, Bloom filters, Cuckoo filters, Count-Min Sketch, Top-K,
and quantile estimation. It also includes HAT-trie-specific exact and compact
structures that Redis/Tarantool do not expose as a core command family, such as
XOR filters, roaring bitmaps, sparse uint64 bitsets, radix-tree prefix indexes,
reservoir samples, and Fenwick trees.

Tarantool is a database/application server rather than a data-structure command
server. Its closest built-in equivalents are spaces, tuples, primary and
secondary indexes, tuple update operations, Lua procedures, SQL, WAL/snapshot
persistence, and replication. Many HAT-trie or Redis data-structure commands
can be modeled in Tarantool with schema design plus Lua code, but they are not
single built-in data-type commands.

Local HAT-trie numbers were measured on an AMD Ryzen 9 5950X with:

```
make bench-command-features BENCHTIME=100x
```

Redis numbers were measured with Redis 7.0.4 in a temporary Docker container,
single client, no pipeline, and 10,000 requests per measured command:

```
make bench-redis-command-features REDIS_START_DOCKER=1 REDIS_PORT=6380 REDIS_REQUESTS=10000 REDIS_CLIENTS=1 REDIS_KEYSPACE=10000
```

Tarantool numbers were measured with the locally installed Tarantool 2.6.0
(`2.6.0-0-g47aa4e01e`), embedded in a temporary directory with WAL disabled,
and 10,000 feature cycles per measured operation:

```
make bench-tarantool-command-features TARANTOOL_REQUESTS=10000 TARANTOOL_KEYSPACE=10000
```

HAT-trie command benchmarks are in-process Go calls. Redis command benchmarks
include local loopback TCP, Redis protocol parsing, and server dispatch.
Tarantool command benchmarks are embedded Tarantool engine calls. The speedup
columns are `baseline seconds / HAT-trie seconds`, so values above `1.00x`
mean HAT-trie was faster and values below `1.00x` mean the baseline was faster.
The numbers are useful as practical local comparisons but not apples-to-apples
microbenchmarks.

## HAT-trie Command Families

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

Run the extracted alias table from the code with:

```
make command-support
```

## Benchmark Matrix

| Feature family | HAT-trie benchmark | HAT-trie seconds / 10k | Redis measured command | Redis seconds / 10k | Redis/HAT speedup | Tarantool measured operation | Tarantool seconds / 10k | Tarantool/HAT speedup |
| --- | --- | ---: | --- | ---: | ---: | --- | ---: | ---: |
| String write | `BenchmarkCommandFeature/StringSet` | 0.017410 s | `SET` | 0.930000 s | 53.42x | `space:replace()` | 0.030214 s | 1.74x |
| String read | `BenchmarkCommandFeature/StringGet` | 0.019810 s | `GET` | 0.963000 s | 48.61x | `space.index.primary:get()` | 0.004856 s | 0.25x |
| Integer counter | `BenchmarkCommandFeature/CounterInc` | 0.017600 s | `INCR` | 0.939000 s | 53.35x | `space:update({{"+", 2, 1}})` | 0.033353 s | 1.90x |
| TTL update | `BenchmarkCommandFeature/TTLExpire` | 0.020030 s | `EXPIRE` | 1.039000 s | 51.87x | `space:update({{"=", 3, expires_at}})` | 0.033555 s | 1.68x |
| Map/hash write | `BenchmarkCommandFeature/MapPut` | 0.046450 s | `HSET` | 1.068000 s | 22.99x | `space:replace({key, field, value})` | 0.009241 s | 0.20x |
| Map/hash read | `BenchmarkCommandFeature/MapPeek` | 0.014080 s | `HGET` | 1.194000 s | 84.80x | `space.index.primary:get({key, field})` | 0.003407 s | 0.24x |
| List/deque push+pop | `BenchmarkCommandFeature/SlicePushPop` | 0.034470 s | `LPUSH` + `RPOP` | 4.527003 s | 131.33x | `space:replace()` + `space:delete()` | 0.014234 s | 0.41x |
| Set add+has | `BenchmarkCommandFeature/SetAddHas` | 0.030300 s | `SADD` + `SISMEMBER` | 2.462999 s | 81.29x | `space:replace()` + `space.index.primary:get()` | 0.015085 s | 0.50x |
| Priority queue push+pop | `BenchmarkCommandFeature/PriorityQueuePushPop` | 0.042600 s | `ZADD` + `ZPOPMIN` | 2.398000 s | 56.29x | tree index insert + `index:min()` + delete | 0.053955 s | 1.27x |
| Bloom add | `BenchmarkCommandFeature/BloomAdd` | 0.030620 s | not measured: RedisBloom/Redis 8 module unavailable | - | - | no native built-in command | - | - |
| Bloom lookup | `BenchmarkCommandFeature/BloomHas` | 0.024230 s | not measured: RedisBloom/Redis 8 module unavailable | - | - | no native built-in command | - | - |
| Cuckoo delete+add | `BenchmarkCommandFeature/CuckooDeleteAdd` | 0.030880 s | not measured: RedisBloom/Redis 8 module unavailable | - | - | no native built-in command | - | - |
| Cuckoo lookup | `BenchmarkCommandFeature/CuckooHas` | 0.019840 s | not measured: RedisBloom/Redis 8 module unavailable | - | - | no native built-in command | - | - |
| XOR filter build | `BenchmarkCommandFeature/XorBuild64Items` | 2.492080 s | no native core command | - | - | no native built-in command | - | - |
| XOR filter lookup | `BenchmarkCommandFeature/XorHas` | 0.014240 s | no native core command | - | - | no native built-in command | - | - |
| Roaring bitmap add | `BenchmarkCommandFeature/RoaringAdd` | 0.016130 s | `SETBIT` bitmap, not roaring | 1.389001 s | 86.11x | `space:replace()` membership-index approximation | 0.013979 s | 0.87x |
| Roaring bitmap lookup | `BenchmarkCommandFeature/RoaringHas` | 0.016400 s | `GETBIT` bitmap, not roaring | 1.265000 s | 77.13x | `space.index.primary:get()` membership-index approximation | 0.002475 s | 0.15x |
| Sparse uint64 bitset add | `BenchmarkCommandFeature/SparseBitsetAdd` | 0.018220 s | `SETBIT` dense bitmap approximation | 1.389001 s | 76.23x | `space:replace()` membership-index approximation | 0.014560 s | 0.80x |
| Sparse uint64 bitset lookup | `BenchmarkCommandFeature/SparseBitsetHas` | 0.023190 s | `GETBIT` dense bitmap approximation | 1.265000 s | 54.55x | `space.index.primary:get()` membership-index approximation | 0.003436 s | 0.15x |
| Radix-tree put | `BenchmarkCommandFeature/RadixPut` | 0.038630 s | sorted-set lex/search approximation not measured | - | - | `space:replace()` tree string-key approximation | 0.011437 s | 0.30x |
| Radix-tree prefix scan | `BenchmarkCommandFeature/RadixPrefix` | 0.068760 s | sorted-set lex/search approximation not measured | - | - | `index:pairs(prefix, {iterator = "GE"})` approximation | 0.203635 s | 2.96x |
| Count-Min Sketch increment | `BenchmarkCommandFeature/CountMinSketchIncrement` | 0.023540 s | not measured: RedisBloom/Redis 8 module unavailable | - | - | no native built-in command | - | - |
| Count-Min Sketch estimate | `BenchmarkCommandFeature/CountMinSketchEstimate` | 0.017250 s | not measured: RedisBloom/Redis 8 module unavailable | - | - | no native built-in command | - | - |
| HyperLogLog add | `BenchmarkCommandFeature/HyperLogLogAdd` | 0.115600 s | `PFADD` | 1.047000 s | 9.06x | no native built-in command | - | - |
| HyperLogLog count | `BenchmarkCommandFeature/HyperLogLogCount` | 0.058980 s | `PFCOUNT` | 1.169000 s | 19.82x | no native built-in command | - | - |
| Top-K add | `BenchmarkCommandFeature/TopKAdd` | 0.036320 s | not measured: RedisBloom/Redis 8 module unavailable | - | - | no native built-in command | - | - |
| Top-K read | `BenchmarkCommandFeature/TopKGet` | 0.027220 s | not measured: RedisBloom/Redis 8 module unavailable | - | - | no native built-in command | - | - |
| Reservoir sample add | `BenchmarkCommandFeature/ReservoirSampleAdd` | 0.027010 s | no native core command | - | - | no native built-in command | - | - |
| Reservoir sample read | `BenchmarkCommandFeature/ReservoirSampleGet` | 0.080820 s | no native core command | - | - | no native built-in command | - | - |
| Quantile sketch add | `BenchmarkCommandFeature/QuantileSketchAdd` | 0.023790 s | not measured: `TDIGEST.*` unavailable in Redis 7.0.4 image | - | - | no native built-in command | - | - |
| Quantile sketch estimate | `BenchmarkCommandFeature/QuantileSketchEstimate` | 0.043960 s | not measured: `TDIGEST.*` unavailable in Redis 7.0.4 image | - | - | no native built-in command | - | - |
| Fenwick tree add | `BenchmarkCommandFeature/FenwickTreeAdd` | 0.022100 s | no native core command | - | - | no native built-in command | - | - |
| Fenwick tree range sum | `BenchmarkCommandFeature/FenwickTreeRange` | 0.012250 s | no native core command | - | - | no native built-in command | - | - |
| Replication dump | `BenchmarkCommandFeature/ReplicationDump` | 0.071580 s | `DUMP` | 1.182000 s | 16.51x | `msgpack.encode(tuple)` | 0.013092 s | 0.18x |

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
