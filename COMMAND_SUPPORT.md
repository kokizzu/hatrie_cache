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

HAT-trie command benchmarks are in-process Go calls. Redis command benchmarks
include local loopback TCP, Redis protocol parsing, and server dispatch, so the
numbers are useful as a practical local comparison but not an apples-to-apples
microbenchmark.

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

| Feature family | HAT-trie benchmark | HAT-trie seconds / 10k | Redis measured command | Redis seconds / 10k | Tarantool benchmark target |
| --- | --- | ---: | --- | ---: | --- |
| String write | `BenchmarkCommandFeature/StringSet` | 0.017410 s | `SET` | 0.930000 s | `space:replace()` / `space:put()` |
| String read | `BenchmarkCommandFeature/StringGet` | 0.019810 s | `GET` | 0.963000 s | `space.index.primary:get()` |
| Integer counter | `BenchmarkCommandFeature/CounterInc` | 0.017600 s | `INCR` | 0.939000 s | `space:update()` arithmetic |
| TTL update | `BenchmarkCommandFeature/TTLExpire` | 0.020030 s | `EXPIRE` | 1.039000 s | Lua/custom expiration index |
| Map/hash write | `BenchmarkCommandFeature/MapPut` | 0.046450 s | `HSET` | 1.068000 s | tuple update / Lua map field |
| Map/hash read | `BenchmarkCommandFeature/MapPeek` | 0.014080 s | `HGET` | 1.194000 s | tuple field read |
| List/deque push+pop | `BenchmarkCommandFeature/SlicePushPop` | 0.034470 s | `LPUSH` + `RPOP` | 4.527003 s | queue module or space procedure |
| Set add+has | `BenchmarkCommandFeature/SetAddHas` | 0.030300 s | `SADD` + `SISMEMBER` | 2.462999 s | membership space/index |
| Priority queue push+pop | `BenchmarkCommandFeature/PriorityQueuePushPop` | 0.042600 s | `ZADD` + `ZPOPMIN` | 2.398000 s | B-tree index + Lua pop |
| Bloom add | `BenchmarkCommandFeature/BloomAdd` | 0.030620 s | not measured: RedisBloom/Redis 8 module unavailable | - | Lua/module |
| Bloom lookup | `BenchmarkCommandFeature/BloomHas` | 0.024230 s | not measured: RedisBloom/Redis 8 module unavailable | - | Lua/module |
| Cuckoo delete+add | `BenchmarkCommandFeature/CuckooDeleteAdd` | 0.030880 s | not measured: RedisBloom/Redis 8 module unavailable | - | Lua/module |
| Cuckoo lookup | `BenchmarkCommandFeature/CuckooHas` | 0.019840 s | not measured: RedisBloom/Redis 8 module unavailable | - | Lua/module |
| XOR filter build | `BenchmarkCommandFeature/XorBuild64Items` | 2.492080 s | no native core command | - | Lua/module |
| XOR filter lookup | `BenchmarkCommandFeature/XorHas` | 0.014240 s | no native core command | - | Lua/module |
| Roaring bitmap add | `BenchmarkCommandFeature/RoaringAdd` | 0.016130 s | `SETBIT` bitmap, not roaring | 1.389001 s | bitmap index is not roaring value |
| Roaring bitmap lookup | `BenchmarkCommandFeature/RoaringHas` | 0.016400 s | `GETBIT` bitmap, not roaring | 1.265000 s | bitmap index is not roaring value |
| Sparse uint64 bitset add | `BenchmarkCommandFeature/SparseBitsetAdd` | 0.018220 s | `SETBIT` dense bitmap approximation | 1.389001 s | Lua/module |
| Sparse uint64 bitset lookup | `BenchmarkCommandFeature/SparseBitsetHas` | 0.023190 s | `GETBIT` dense bitmap approximation | 1.265000 s | Lua/module |
| Radix-tree put | `BenchmarkCommandFeature/RadixPut` | 0.038630 s | sorted-set lex/search approximation not measured | - | tree index over tuple key |
| Radix-tree prefix scan | `BenchmarkCommandFeature/RadixPrefix` | 0.068760 s | sorted-set lex/search approximation not measured | - | index range select |
| Count-Min Sketch increment | `BenchmarkCommandFeature/CountMinSketchIncrement` | 0.023540 s | not measured: RedisBloom/Redis 8 module unavailable | - | Lua/module |
| Count-Min Sketch estimate | `BenchmarkCommandFeature/CountMinSketchEstimate` | 0.017250 s | not measured: RedisBloom/Redis 8 module unavailable | - | Lua/module |
| HyperLogLog add | `BenchmarkCommandFeature/HyperLogLogAdd` | 0.115600 s | `PFADD` | 1.047000 s | Lua/module |
| HyperLogLog count | `BenchmarkCommandFeature/HyperLogLogCount` | 0.058980 s | `PFCOUNT` | 1.169000 s | Lua/module |
| Top-K add | `BenchmarkCommandFeature/TopKAdd` | 0.036320 s | not measured: RedisBloom/Redis 8 module unavailable | - | Lua/module |
| Top-K read | `BenchmarkCommandFeature/TopKGet` | 0.027220 s | not measured: RedisBloom/Redis 8 module unavailable | - | Lua/module |
| Reservoir sample add | `BenchmarkCommandFeature/ReservoirSampleAdd` | 0.027010 s | no native core command | - | Lua/module |
| Reservoir sample read | `BenchmarkCommandFeature/ReservoirSampleGet` | 0.080820 s | no native core command | - | Lua/module |
| Quantile sketch add | `BenchmarkCommandFeature/QuantileSketchAdd` | 0.023790 s | not measured: `TDIGEST.*` unavailable in Redis 7.0.4 image | - | Lua/module |
| Quantile sketch estimate | `BenchmarkCommandFeature/QuantileSketchEstimate` | 0.043960 s | not measured: `TDIGEST.*` unavailable in Redis 7.0.4 image | - | Lua/module |
| Fenwick tree add | `BenchmarkCommandFeature/FenwickTreeAdd` | 0.022100 s | no native core command | - | Lua/custom prefix-sum structure |
| Fenwick tree range sum | `BenchmarkCommandFeature/FenwickTreeRange` | 0.012250 s | no native core command | - | Lua/custom prefix-sum structure |
| Replication dump | `BenchmarkCommandFeature/ReplicationDump` | 0.071580 s | `DUMP` | 1.182000 s | snapshot/WAL tuple transfer |

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
