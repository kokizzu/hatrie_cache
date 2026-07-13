# hatrie_cache
Experimental **TO BE** distributed memcache using HAT-Trie (a data structure designed by Dr Nikolas Askitis)

_**warning**: this project obviously not ready for production_

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

The Go wrapper supports key expiration with `Expire`, `ExpireAt`, `Persist`,
and `TTL`. Expired entries are removed lazily when the key is read or mutated.
`TTL` returns `NoTTL` for missing, expired, or persistent keys. Use
`VacuumExpired` for immediate cleanup or `StartExpirationCleaner` for periodic
background cleanup.

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

Use `Stats` to read cache counters and hit-rate metadata. `SaveStats` writes the
statistics snapshot as JSON, and `LoadStats` restores a saved snapshot.

The bundled C HAT-trie tests can be compiled directly with GCC when autotools
build files have not been generated.

## TODO:

- [x] bind [HAT-Trie](https://github.com/luikore/hat-trie) to Go using CGO
- [x] `hat_map<string,int+byte>` stores index or special types (deque/set/etc) to `[][]byte` (aka raws); raws can be serialized using [FlatBuffers](http://github.com/google/flatbuffers) or [FastBinaryEncoding](http://github.com/chronoxor/FastBinaryEncoding)
- [x] add TTL map, check for expiration when read, delete if expired
- [x] need benchmark which how much faster: `[][]byte` compared to `map[int][]byte` (~170 bytes overhead)
- [x] create a web UI for management and monitoring (frontend: Svelte MPA)
- [ ] create backend service using http2/grpc for APIs so it can be accessed from another language 
- [ ] create a client CLI with for [statistics/cluster/server](https://redis.io/commands/#server) management and running commands:
```		
any type:
  SET/SETSTR/SETINT key value
  SETX/SETSTRX/SETINTX key ttl value
   master/leader write, journal, and broadcasting: internalSET key idx value ttl
   currenttime+ttl set to an array, and checked every second, execute DEL if expired
   the idx is 32-bit integer, 1 bit is for ttl flag, 1 bit if on disk, remaining 6-bit is for special type
  EXISTS/GET/GETSTR key
   check the value on the hat_map
  DEL key
   master/leader write, journal, and broadcasting: internalDEL key idx
   deleted index saved on another map
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
```
- [ ] add option to shard/partition it or full replica, copy tarantool's vbucket/vshard logic
- [x] make sure all read/write operation synchronized, so no stale read/data corruption (in cost of performance)
- [x] check if serializer can support Go's map
- [ ] data persisted to disk using lmdb, leveldb, or rocksdb, preferably one with snappy compression
- [x] binary data that are >64KB always stored on disk
- [ ] write all pending transaction on journal (backup if program terminated unexpectedly)
- [x] update statistics (last hit, last write, hit rate, cumulative hit rate) to disk
- [ ] on-load check for expired data
- [ ] when service start, non-expired keys and (<1KB AND <1h last hit AND >1000 hit rate) values loaded from database to memory
- [ ] when service stopped/timer/sync-write forced, data written to disk
- [x] create iterator command to get all keys and keys based on certain prefix
- [x] create timer vacuum goroutine to clean expired data
- [ ] add OOM-triggered vacuum policy
- [ ] when master/leader disconnected from all slave, new master/leader elected by remaining slave
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
