# hatrie_cache
Experimental **TO BE** distributed memcache using HAT-Trie (a data structure designed by Dr Nikolas Askitis)

_**warning**: this project obviously not ready for production_

Slice/stack/queue values are stored behind compact HAT-trie indexes with a ring
deque backing store, so push/pop/shift stay O(1) and removed elements do not
retain old object references. Priority queue values use a flat binary heap with
stable insertion ordering for equal priorities, keeping push/pop O(log n), peek
O(1), and memory usage low without per-item node allocations.
Bloom filter values use packed bitsets plus double hashing for fast,
low-memory membership checks without storing inserted items.
Count-Min Sketch values use compact uint32 counter grids plus double hashing
for approximate frequency counts without storing observed items.
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

Run the raw byte backing-store benchmarks:

```
make bench
```

The Svelte MPA management UI lives in `svelte-mpa/`. Install and run it with:

```
make frontend-install
make frontend-dev
```

Run the full local verification suite with `make verify`.

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
`-monitoring-server` flag is set:

```
make monitoring-server MONITORING_ADDR=127.0.0.1:8080
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
compression. `DB_SYNC_INTERVAL` periodically rewrites the LevelDB snapshot while
the server is running:

```
make monitoring-server DB_PATH=data/cache.leveldb DB_SYNC_INTERVAL=30s
```

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

Set `SNAPSHOT_PATH` to load a JSON snapshot at startup and save it on shutdown.
Set `SNAPSHOT_INTERVAL` to periodically write the same snapshot while the server
runs:

```
make monitoring-server SNAPSHOT_PATH=data/snapshot.json SNAPSHOT_INTERVAL=30s
```

Set `JOURNAL_PATH` to replay an append-only command journal at startup and fsync
mutating cache commands before applying them. When `SNAPSHOT_PATH` is also set,
snapshots store the journal checkpoint and compact older journal entries after a
successful snapshot:

```
make monitoring-server SNAPSHOT_PATH=data/snapshot.json JOURNAL_PATH=data/commands.journal
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
or its heartbeat times out, the first healthy replica becomes leader:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json ELECTION_TIMEOUT=15s
```

Set `REPLICATION=true` to let an elected leader broadcast successful local
mutations to the current key's topology owners over HTTP. Replication uses the
internal `DUMP`/`INTERNALSET` and `INTERNALDEL` commands, skips internal
replication commands to avoid loops, and records the last replication attempt at
`/api/replication`:

```
make monitoring-server NODE_ID=node-a TOPOLOGY_PATH=data/topology.json REPLICATION=true
```

The monitoring server exposes JSON APIs at `/api/health`, `/api/stats`,
`/api/entries`, `/api/topology`, `/api/election`, `/api/replication`, and
`/api/commands`.
`GET /api/election` returns node liveness and elected shard leaders.
`GET /api/election?key=...` returns the topology route plus the elected leader
for that key. `POST /api/election` accepts `node` and `online` to record a
heartbeat or mark a node offline. `GET /api/replication` returns the most recent
replication result. `POST /api/commands` accepts `command`, `key`,
optional `value`, `values`, `subkey`, `pairs`,
`priority`, `ttl_seconds`, and `unix_seconds`; it currently
supports `GET`, `GETSTR`, `EXISTS`, `SET`, `SETSTR`, `SETX`, `SETSTRX`,
`SETINT`, `SETINTX`, `INC`, `DEL`, `TTL`, `EXPIRE`, `EXPIREAT`, `PUTMAP`,
`PEEKMAP`, `TAKEMAP`, `PUSHSLICE`, `POPSLICE`, `SHIFTSLICE`, `HEADSLICE`,
`TAILSLICE`, `ADDSET`, `REMSET`, `HASSET`, `GETSET`, `PUSHPQ`, `PEEKPQ`,
`POPPQ`, `GETPQ`, `CREATEBF`, `ADDBF`, `HASBF`, `INFOBF`, `CREATECMS`,
`INCRCMS`, `ESTCMS`, `INFOCMS`, `DUMP`, `INTERNALSET`, and `INTERNALDEL`. `DUMP`,
`INTERNALSET`, and `INTERNALDEL` are low-level replication primitives that move
one key as the same snapshot-entry JSON used by snapshot and LevelDB
persistence.

Use the HTTP client CLI against a running monitoring server:

```
make cli ARGS='stats'
make cli ARGS='entries -prefix session:'
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
make cli ARGS='command -cmd CREATECMS -key freq:paths -value 2048 -subkey 4'
make cli ARGS='command -cmd INCRCMS -key freq:paths -value /api/users -subkey 3'
make cli ARGS='command -cmd ESTCMS -key freq:paths -value /api/users'
make cli ARGS='command -cmd DUMP -key tags'
make cli ARGS='topology'
make cli ARGS='topology -key session:1'
make cli ARGS='topology -file data/topology.json'
make cli ARGS='election'
make cli ARGS='election -key session:1'
make cli ARGS='election -heartbeat node-a'
make cli ARGS='election -offline node-a'
make cli ARGS='replication'
make cli ARGS='snapshot'
```

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
background cleanup. Use `VacuumExpiredOnMemoryPressure` or
`StartMemoryPressureVacuum` to remove expired keys only when heap allocation is
above a configured threshold.

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

Use `SaveSnapshot` and `LoadSnapshot` for portable JSON data snapshots. Snapshot
loads skip expired entries, restore per-key access metadata when present, and
re-apply the normal disk spill threshold for large byte values.

Use `OpenLevelDBStore`, `SaveLevelDB`, and `LoadLevelDB` for LevelDB-backed
disk persistence. The LevelDB writer uses Snappy compression and clears stale
keys on each save while preserving per-key access metadata. `LevelDBStore.Close`
is idempotent; operations after close return `ErrLevelDBStoreClosed`.

Use `NewCacheGRPCServer` and `RegisterCacheGRPCServer` to mount the native gRPC
service in another Go process, or use the generated client in
`internal/gen/hatriecache/v1`.

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
count-min sketch type:
  CREATECMS key width [depth]
  INCRCMS key val [count]
  ESTCMS/INFOCMS key
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
- [ ] the distributed part using emitter.io, 
      or offloaded to another MQ, 
      or [dynomite](https://github.com/Netflix/dynomite) (eventually consistent), 
      or [bcache](https://github.com/iwanbk/bcache) 
      or [consul](https://medium.com/@didil/building-a-simple-distributed-system-with-go-consul-39b08ffc5d2c) 
      or learn from [rqlite](https://github.com/rqlite/rqlite) (master-slave), 
      or learn from [etcd](https://github.com/etcd-io/etcd/tree/master/raft) 
      or learn from [projects using Badger](https://github.com/dgraph-io/badger#other-projects-using-badger)
      or learn from [autocache](https://github.com/pomerium/autocache)
      or use [finn](https://github.com/tidwall/finn) and learn from [summitdb](https://github.com/tidwall/summitdb)
      or use [dragonboat](https://github.com/lni/dragonboat) (multi-master)

## Use cases:

- storing session keys
- counting url hits, likes
- caching 
