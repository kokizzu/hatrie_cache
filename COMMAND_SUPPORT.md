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

The Redis and Tarantool columns name the closest native operation family to
benchmark. They are not local throughput results in this table; this workspace
had Redis client tools but no Redis server listening on `127.0.0.1:6379`.

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

| Feature family | HAT-trie benchmark | Local HAT-trie result | Redis benchmark target | Tarantool benchmark target |
| --- | --- | ---: | --- | --- |
| String write | `BenchmarkCommandFeature/StringSet` | 1,741 ns/op, 20 B/op, 1 alloc | `redis-benchmark SET` | `space:replace()` / `space:put()` |
| String read | `BenchmarkCommandFeature/StringGet` | 1,981 ns/op, 6 B/op, 1 alloc | `redis-benchmark GET` | `space.index.primary:get()` |
| Integer counter | `BenchmarkCommandFeature/CounterInc` | 1,760 ns/op, 0 B/op, 0 alloc | `redis-benchmark INCR` / `INCRBY` | `space:update()` arithmetic |
| TTL update | `BenchmarkCommandFeature/TTLExpire` | 2,003 ns/op, 128 B/op, 1 alloc | `redis-benchmark EXPIRE` | Lua/custom expiration index |
| Map/hash write | `BenchmarkCommandFeature/MapPut` | 4,645 ns/op, 585 B/op, 5 alloc | `redis-benchmark HSET` | tuple update / Lua map field |
| Map/hash read | `BenchmarkCommandFeature/MapPeek` | 1,408 ns/op, 0 B/op, 0 alloc | `redis-benchmark HGET` | tuple field read |
| List/deque push+pop | `BenchmarkCommandFeature/SlicePushPop` | 3,447 ns/op, 237 B/op, 6 alloc | `redis-benchmark LPUSH` + `RPOP` | queue module or space procedure |
| Set add+has | `BenchmarkCommandFeature/SetAddHas` | 3,030 ns/op, 176 B/op, 9 alloc | `redis-benchmark SADD` + `SISMEMBER` | membership space/index |
| Priority queue push+pop | `BenchmarkCommandFeature/PriorityQueuePushPop` | 4,260 ns/op, 229 B/op, 8 alloc | `ZADD` + `ZPOPMIN`/`ZPOPMAX` | B-tree index + Lua pop |
| Bloom add | `BenchmarkCommandFeature/BloomAdd` | 3,062 ns/op, 792 B/op, 4 alloc | `BF.ADD` / `BF.MADD` | Lua/module |
| Bloom lookup | `BenchmarkCommandFeature/BloomHas` | 2,423 ns/op, 40 B/op, 3 alloc | `BF.EXISTS` | Lua/module |
| Cuckoo delete+add | `BenchmarkCommandFeature/CuckooDeleteAdd` | 3,088 ns/op, 128 B/op, 8 alloc | `CF.DEL` + `CF.ADD` | Lua/module |
| Cuckoo lookup | `BenchmarkCommandFeature/CuckooHas` | 1,984 ns/op, 40 B/op, 3 alloc | `CF.EXISTS` | Lua/module |
| XOR filter build | `BenchmarkCommandFeature/XorBuild64Items` | 249,208 ns/op, 22,761 B/op, 370 alloc | no native core command | Lua/module |
| XOR filter lookup | `BenchmarkCommandFeature/XorHas` | 1,424 ns/op, 64 B/op, 4 alloc | no native core command | Lua/module |
| Roaring bitmap add | `BenchmarkCommandFeature/RoaringAdd` | 1,613 ns/op, 5 B/op, 1 alloc | bitmap `SETBIT` is not roaring | bitmap index is not roaring value |
| Roaring bitmap lookup | `BenchmarkCommandFeature/RoaringHas` | 1,640 ns/op, 4 B/op, 1 alloc | bitmap `GETBIT` is not roaring | bitmap index is not roaring value |
| Sparse uint64 bitset add | `BenchmarkCommandFeature/SparseBitsetAdd` | 1,822 ns/op, 9 B/op, 1 alloc | bitmap/bitfield with encoded offsets | Lua/module |
| Sparse uint64 bitset lookup | `BenchmarkCommandFeature/SparseBitsetHas` | 2,319 ns/op, 8 B/op, 1 alloc | bitmap/bitfield with encoded offsets | Lua/module |
| Radix-tree put | `BenchmarkCommandFeature/RadixPut` | 3,863 ns/op, 605 B/op, 5 alloc | sorted-set lex or search index approximation | tree index over tuple key |
| Radix-tree prefix scan | `BenchmarkCommandFeature/RadixPrefix` | 6,876 ns/op, 1,468 B/op, 20 alloc | `ZRANGEBYLEX` / search prefix query | index range select |
| Count-Min Sketch increment | `BenchmarkCommandFeature/CountMinSketchIncrement` | 2,354 ns/op, 277 B/op, 4 alloc | `CMS.INCRBY` | Lua/module |
| Count-Min Sketch estimate | `BenchmarkCommandFeature/CountMinSketchEstimate` | 1,725 ns/op, 40 B/op, 3 alloc | `CMS.QUERY` | Lua/module |
| HyperLogLog add | `BenchmarkCommandFeature/HyperLogLogAdd` | 11,560 ns/op, 147 B/op, 4 alloc | `PFADD` | Lua/module |
| HyperLogLog count | `BenchmarkCommandFeature/HyperLogLogCount` | 5,898 ns/op, 0 B/op, 0 alloc | `PFCOUNT` | Lua/module |
| Top-K add | `BenchmarkCommandFeature/TopKAdd` | 3,632 ns/op, 276 B/op, 8 alloc | `TOPK.ADD` | Lua/module |
| Top-K read | `BenchmarkCommandFeature/TopKGet` | 2,722 ns/op, 208 B/op, 6 alloc | `TOPK.LIST` / `TOPK.COUNT` | Lua/module |
| Reservoir sample add | `BenchmarkCommandFeature/ReservoirSampleAdd` | 2,701 ns/op, 251 B/op, 6 alloc | no native core command | Lua/module |
| Reservoir sample read | `BenchmarkCommandFeature/ReservoirSampleGet` | 8,082 ns/op, 2,356 B/op, 8 alloc | no native core command | Lua/module |
| Quantile sketch add | `BenchmarkCommandFeature/QuantileSketchAdd` | 2,379 ns/op, 270 B/op, 4 alloc | `TDIGEST.ADD` | Lua/module |
| Quantile sketch estimate | `BenchmarkCommandFeature/QuantileSketchEstimate` | 4,396 ns/op, 128 B/op, 3 alloc | `TDIGEST.QUANTILE` | Lua/module |
| Fenwick tree add | `BenchmarkCommandFeature/FenwickTreeAdd` | 2,210 ns/op, 244 B/op, 3 alloc | Lua/custom prefix-sum structure | Lua/custom prefix-sum structure |
| Fenwick tree range sum | `BenchmarkCommandFeature/FenwickTreeRange` | 1,225 ns/op, 0 B/op, 0 alloc | Lua/custom prefix-sum structure | Lua/custom prefix-sum structure |
| Replication dump | `BenchmarkCommandFeature/ReplicationDump` | 7,158 ns/op, 1,061 B/op, 9 alloc | `DUMP` / `RESTORE` | snapshot/WAL tuple transfer |

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
