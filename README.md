# hatrie_cache
Experimental distributed memcache using HAT-Trie (a data structure designed by Dr Nikolas Askitis)

_**warning**: this project obviously not ready for production_

## TODO:

- [x] bind [HAT-Trie](https://github.com/luikore/hat-trie) to Go using CGO
- [x] `hat_map<string,int+byte>` stores index or special types (deque/set/etc) to `[][]byte` (aka raws); raws can be serialized using [FlatBuffers](http://github.com/google/flatbuffers) or [FastBinaryEncoding](http://github.com/chronoxor/FastBinaryEncoding)
- [ ] add TTL map, check for expiration when read, delete if expired 
- [ ] need benchmark which how much faster: `[][]byte` compared to `map[int][]byte` (~170 bytes overhead)
- [ ] create a web UI for management and monitoring (frontend: [Svelte](https://svelte.dev/))
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
- [ ] make sure all read/write operation synchronized, so no stale read/data corruption (in cost of performance)
- [ ] check if serializer can support Go's map
- [ ] data persisted to disk using lmdb, leveldb, or rocksdb, preferably one with snappy compression
- [ ] binary data that are >64KB always stored on disk
- [ ] write all pending transaction on journal (backup if program terminated unexpectedly)
- [ ] update statistics (last hit, last write, hit rate, cumulative hit rate) to disk
- [ ] on-load check for expired data
- [ ] when service start, non-expired keys and (<1KB AND <1h last hit AND >1000 hit rate) values loaded from database to memory
- [ ] when service stopped/timer/sync-write forced, data written to disk
- [ ] create iterator command to get all keys and keys based on certain prefix
- [ ] create timer/oom vacuum goroutine to clean expired data
- [ ] when master/leader disconnected from all slave, new master/leader elected by remaining slave
- [ ] the distributed part using emitter.io, or offloaded to another MQ, or learn from [rqlite](https://github.com/rqlite/rqlite) (master-slave), or learn from [etcd](https://github.com/etcd-io/etcd/tree/master/raft) (multi-master)

## Use cases:

- storing session keys
- counting url hits, likes
- caching 
