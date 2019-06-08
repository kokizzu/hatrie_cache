# hatrie_cache
Experimental distributed memcache using HAT-Trie (a data structure designed by Dr Nikolas Askitis)

## TODO:

1. [x] bind [HAT-Trie](https://github.com/luikore/hat-trie) to Go using CGO
2a. [x] `hat_map<string,int+byte>` stores index or special types (deque/set/etc) to `[][]byte` (aka raws); raws can be serialized using [FlatBuffers](http://github.com/google/flatbuffers) or [FastBinaryEncoding](http://github.com/chronoxor/FastBinaryEncoding)
2b. [ ] need benchmark which how much faster and space efficient: [][]byte compared to map[int][]byte
3a. [ ] create a service for monitoring, 
3b. [ ] create APIs using http2/grpc so it can be accessed from another language; 
3c. [ ] create a client CLI
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
slice/arr type:
  PUSHSLICE key val...
  POPSLICE,SHIFTSLICE,HEADSLICE,TAILSLICE key
```
4. [ ] make sure all read/write operation synchronized (no stale read/data corruption)
5a. [ ] data persisted to disk using lmdb, or leveldb with snappy compression; 
5b. [ ] binary data that are >1MB always stored on disk
5c. [ ] write all transaction on journal
6a. [ ] when service start, data loaded from database to memory; 
6b. [ ] when service stopped/timer/sync-write forced, data written to disk
7a. [ ] add TTL map, check for expiration when read, delete if expired; 
7b.  [ ] on-load check for expired data; create vaccum daemon to clean expired data
8a. [ ] the distributed part using emitter.io, or offloaded to another MQ (master-slave), or learn from [etcd](https://github.com/etcd-io/etcd/tree/master/raft) (multi-master)
8b. [ ] when master/leader disconnected from all slave, new master/leader elected by reamining slave


## Use cases:

- storing session keys
- counting url hits, likes
- caching results