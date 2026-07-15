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
keys instead of scanning every TTL entry and compacts stale schedule entries
under churn.
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
```

The Svelte MPA management UI lives in `svelte-mpa/`. Install and run it with:

```
make frontend-install
make frontend-dev
```

Run the full local verification suite with `make verify`.
The C verifier automatically runs AddressSanitizer/UBSan leak and undefined
behavior checks when the local compiler supports them; use
`make verify-c SANITIZE_C=0` to skip that pass or `SANITIZE_C=1` to require it.
On hosts with `vm.overcommit_memory=2`, auto mode skips that sanitizer pass
because AddressSanitizer can reserve a large shadow-memory range that strict
commit accounting rejects. On this toolchain that reservation is expected around
`15392894357504` bytes, which can appear in repeated kernel
`__vm_enough_memory` messages. Auto mode also skips the pass when
`/proc/meminfo` shows available commit headroom below AddressSanitizer's
expected reservation. To force that pass anyway, use
`make verify-c SANITIZE_C=1 SANITIZE_C_ALLOW_STRICT_OVERCOMMIT=1` and, when
needed, `SANITIZE_C_ALLOW_LOW_COMMIT_HEADROOM=1`.

Run one-off commands through the Makefile/script wrapper:

```
make run CMD='go env GOMOD'
```

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

Provide a TLS certificate and key to serve the same monitoring API over HTTPS
with HTTP/2 ALPN enabled:

```
make monitoring-server MONITORING_ADDR=127.0.0.1:8443 MONITORING_TLS_CERT=cert.pem MONITORING_TLS_KEY=key.pem
```

Set `GRPC_ADDR` to expose the native protobuf API from
`proto/hatriecache/v1/cache.proto`:

```
make monitoring-server GRPC_ADDR=127.0.0.1:9090
```

Set `DB_PATH` to load and save cache data through LevelDB with Snappy
compression. LevelDB records use the binary storage format by default
(`DB_FORMAT=binary`), which avoids JSON object-field overhead and stores byte
values as raw bytes instead of base64. Map, slice, set, priority queue, Top-K,
radix tree, and reservoir sample payloads use the smaller of the compact binary
codec and JSON when both are supported, with automatic JSON fallback for values
outside the recursive JSON-compatible codec. Bloom filter, Count-Min Sketch, and
HyperLogLog values use compact binary payload codecs. Cuckoo filter values also
store packed fingerprints directly in binary records; built XOR filter values do
the same when that is smaller than JSON, while staged XOR filters keep JSON
fallback for pending values. Roaring bitmap
and sparse-bitset containers store their array or bitset payloads directly too.
Fenwick tree snapshots store their numeric tree vector as binary varints.
Quantile sketches store summary samples as binary float/varint tuples, and
reservoir samples store retained stream items as binary priority/sequence/value
tuples. Existing JSON records still load automatically.
Leaving format variables unset in the Makefile wrapper uses the compiled Go
defaults; set them only when you want to override the default format.
Set `DB_FORMAT=json` to keep writing the previous JSON record layout.
`DB_SYNC_INTERVAL` periodically syncs changed LevelDB records while the server
is running:

```
make monitoring-server DB_PATH=data/cache.leveldb DB_SYNC_INTERVAL=30s
make monitoring-server DB_PATH=data/cache.leveldb DB_FORMAT=json
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

Set `DB_HOT_LOAD=true` to load all non-expired LevelDB keys as compact
references while only materializing hot values in memory. By default a hot value
must be 1024 bytes or smaller, have a last hit within 1 hour, and have more than
1000 recorded hits. Cold values are hydrated from LevelDB on first value access
and are still preserved by later LevelDB saves. Keep the `LevelDBStore` open
while cold references may be accessed, or call `HydrateLevelDBReferences` before
closing it to materialize all references into the trie:

```
make monitoring-server DB_PATH=data/cache.leveldb DB_HOT_LOAD=true
```

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

Set `JOURNAL_PATH` to replay an append-only command journal at startup and fsync
mutating cache commands before applying them. When `SNAPSHOT_PATH` is also set,
snapshots store the journal checkpoint and compact older journal entries after a
successful snapshot. Journal records write in the binary format by default
(`JOURNAL_FORMAT=binary`) and still read existing line-delimited JSON journals,
including files that contain both old JSON records and new binary records.
Binary journal records store structured `values` and `pairs` payloads with the
compact binary value codec when that is smaller than their JSON representation,
and otherwise keep the JSON inner payload. Set `JOURNAL_FORMAT=json` to keep
writing the previous JSON journal layout:

```
make monitoring-server SNAPSHOT_PATH=data/snapshot.json JOURNAL_PATH=data/commands.journal
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_FORMAT=json
```

When journaling is enabled, `GET /api/journal?after_sequence=N&limit=1000`
returns a bounded batch of ordered mutating commands after `N` plus the latest
journal sequence. Responses include `has_more` when another batch is available.
If `N` is older than the compacted snapshot checkpoint, the endpoint returns
`409` so a replica can fall back to a snapshot or explicit replication sync.
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
source. Add `JOURNAL_PULL_INTERVAL` to repeat catch-up periodically:

```
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080 JOURNAL_PULL_INTERVAL=5s
make monitoring-server JOURNAL_PATH=data/commands.journal JOURNAL_PULL_SOURCE=http://leader:8080 JOURNAL_PULL_TIMEOUT=5s
```

Set `NODE_ID` and `TOPOLOGY_PATH` to expose and persist cluster topology JSON.
The topology endpoint validates nodes, shard ownership, and replicas, and can
route a key to its shard. Topologies default to `mode: "sharded"`. Set
`bucket_count` and compact `bucket_ranges` to use vbucket-style routing, or set
`mode: "full_replica"` to route every key to every node without partitions:

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

Set `REPLICATION=true` to let an elected leader broadcast successful local
mutations to the current key's topology owners over HTTP. Replication uses the
internal `DUMP`/`INTERNALSET` and `INTERNALDEL` commands, skips internal
replication commands to avoid loops, and records the last replication attempt at
`/api/replication`. HTTP replication command bodies use protobuf by default
(`REPLICATION_WIRE_FORMAT=protobuf`), then automatically use the previous JSON
wire format for structured `values` or `pairs` payloads that protobuf cannot
represent. Set `REPLICATION_WIRE_FORMAT=json` to always use JSON. Large HTTP
replication request bodies are gzip-compressed.
`POST /api/replication` runs an explicit anti-entropy sync
that pushes the local leader-owned keys, optionally filtered by prefix, to their
current topology replicas:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_WIRE_FORMAT=json
```

Set `REPLICATION_SYNC_INTERVAL` to run the same anti-entropy sync
periodically from the local leader. The first sync runs immediately at startup,
then repeats at the configured interval. Set `REPLICATION_SYNC_PREFIX` to limit
the scheduled sync to one key prefix:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_SYNC_INTERVAL=30s
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_SYNC_INTERVAL=30s REPLICATION_SYNC_PREFIX=session:
```

Set `REPLICATION_ASYNC=true` to enqueue replication in a bounded in-process
outbox instead of waiting for remote owners in the write request path. Queued
jobs store the already-materialized internal snapshot payload, so later local
mutations do not change what is delivered for the original write. Tune
`REPLICATION_QUEUE_SIZE`, `REPLICATION_RETRY_INTERVAL`, and
`REPLICATION_MAX_ATTEMPTS` to bound memory and retry failed HTTP deliveries.
Library users can pass `HTTPReplicatorOptions.Context` to tie the async worker
lifetime to a parent service context.
`GET /api/replication` includes the latest replication start/finish timestamps,
duration, async queue depth, capacity, enqueue/drop counts, delivery attempts,
successes, failures, retries, and closed state:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true REPLICATION_ASYNC=true
```

Set `ENFORCE_LEADER_WRITES=true` on clustered nodes to reject mutating client
commands unless the local node is the elected leader for the command key.
Internal replication commands are still accepted so followers can apply leader
updates:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true ENFORCE_LEADER_WRITES=true
```

The monitoring server exposes JSON APIs at `/api/health`, `/api/stats`,
`/api/entries`, `/api/topology`, `/api/election`, `/api/replication`,
`/api/journal`, and `/api/commands`.
Use `GET /api/entries?prefix=...&limit=N` to bound large key listings; limited
responses include `has_more` and `next_after_key` for cursor paging with
`after_key`. Empty keys are valid, so when `next_after_key` is empty and
`has_more` is true, send an explicit empty cursor as `after_key=`.
The Svelte MPA dashboard and key browser use bounded entry requests by default.
`/api/commands` accepts JSON and protobuf command request bodies based on
`Content-Type`; regular browser/API clients can continue to use JSON.
Responses are gzip-compressed when clients send `Accept-Encoding: gzip`.

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
use less heap; structured journal `values` and `pairs` also use the compact
binary value codec when it is smaller than JSON. In the structured journal
payload above that is 17% smaller and about 1.6x faster to decode, while encode
is about 1.6x slower because the writer compares JSON and binary payload sizes.
JSON remains easier to inspect manually. Snapshot and LevelDB
records omit unrelated null fields before compression, so scalar entries do not
carry empty collection fields. Binary snapshots reuse the compact LevelDB
record codec, avoid base64 for byte values, and use the same size-aware
collection, priority-queue, Top-K, radix-tree, built XOR filter, and
reservoir-sample payload choices. Bloom filter, Count-Min Sketch, and
HyperLogLog snapshots use direct compact binary codecs; staged XOR filters keep
JSON fallback for pending values. Cuckoo filter fingerprints, roaring bitmap
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
optional `prefix` and pushes matching local entries to their remote topology
owners.
`GET /api/journal?after_sequence=...&limit=...` returns the command journal tail
when journaling is configured. `POST /api/journal` pulls a remote journal tail
from `source` and applies it locally.
`POST /api/commands` accepts `command`, `key`, optional `value`, `values`,
`subkey`, `pairs`,
`priority`, `ttl_seconds`, and `unix_seconds`; it currently
supports `GET`, `GETSTR`, `EXISTS`, `SET`, `SETSTR`, `SETX`, `SETSTRX`,
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
and `INTERNALDEL`.
`DUMP`,
`INTERNALSET`, and `INTERNALDEL` are low-level replication primitives that move
one key as the same snapshot-entry JSON used by snapshot and LevelDB
persistence.

Use the HTTP client CLI against a running monitoring server:

```
make cli ARGS='stats'
make cli ARGS='-timeout 5s health'
make cli ARGS='-timeout 0 journal -pull-from http://leader:8080 -until-current'
make cli ARGS='entries -prefix session:'
make cli ARGS='entries -prefix session: -limit 1000'
make cli ARGS='entries -prefix session: -limit 1000 -after-key session:1000'
make cli ARGS='command -cmd SETSTR -key name -value ivi'
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
before their returned stop function.

Use `Keys`, `KeysWithPrefix`, `Entries`, and `EntriesWithPrefix` to iterate
over non-expired keys and value metadata. Prefix iteration returns full keys and
supports keys containing NUL bytes.

Use `MarshalMapJSON`, `UnmarshalMapJSON`, `UpsertMapJSON`, and `GetMapJSON`
for JSON serialization of Go map values. The JSON decoder preserves numbers as
`json.Number`.

Byte values larger than `DiskBytesThreshold` (64KB) are stored on disk and set
the `HatValue.OnDisk()` flag. `CreateHatTrie` uses an owned temporary spill
directory that is removed by `Destroy`; use `CreateHatTrieWithDiskDir` to supply
a specific directory.

Use `Stats` to read cache counters and hit-rate metadata. `StatsForKey` returns
per-key read/write counters and last access times without creating stats for
unknown-key misses. `SaveStats` writes the global statistics snapshot as JSON,
and `LoadStats` restores a saved snapshot.

Use `SaveSnapshot` and `LoadSnapshot` for portable data snapshots.
`SaveSnapshot` writes gzip-best binary by default; use
`SaveSnapshotWithFormat(path, SnapshotFormatGzipBestJSON)` for the previous
storage-optimized JSON layout or `SnapshotFormatJSON` for plain JSON. Snapshot
loads auto-detect gzip, binary, and JSON, replace the current in-memory key set,
skip expired entries, restore per-key access metadata when present, and re-apply
the normal disk spill threshold for large byte values.

Use `OpenLevelDBStore`, `SaveLevelDB`, and `LoadLevelDB` for LevelDB-backed
disk persistence. LevelDB loads replace the current in-memory key set. The
LevelDB writer uses Snappy compression, skips unchanged records, clears stale
keys on each save, and preserves per-key access metadata. LevelDB writes use
`DefaultStorageFormat` (`StorageFormatBinary`) by default; use
`SaveLevelDBWithFormat(path, StorageFormatJSON)` or
`OpenLevelDBStoreWithFormat(path, StorageFormatJSON)` to keep writing the
previous JSON record layout. Loads auto-detect both binary and JSON records.
`LevelDBStore.Close` is idempotent; operations after close return
`ErrLevelDBStoreClosed`.

Use `NewCacheGRPCServer` and `RegisterCacheGRPCServer` to mount the native gRPC
service in another Go process, or use the generated client in
`internal/gen/hatriecache/v1`. gRPC command handling can use the same journal,
leader-write enforcement, and HTTP replication options as the monitoring
command API. Clients may request gRPC transfer compression with the standard
`gzip` compressor; the server registers it at the fastest compression level.
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
- [x] create a web UI for management and monitoring (frontend: Svelte MPA)
- [x] create backend service using HTTP/2 JSON APIs so it can be accessed from another language
- [x] add native gRPC protobuf APIs for strongly typed client generation
- [x] create a client CLI for monitoring stats, key listing, and running commands
- [x] add client CLI support for cache command management:
```		
any type:
  SET/SETSTR/SETINT key value
  SETX/SETSTRX/SETINTX key ttl value
  EXISTS/GET/GETSTR/DUMP key
   check the value on the hat_map
  DEL key
  INTERNALSET/INTERNALDEL key
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
