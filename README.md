# hatrie_cache
Experimental distributed memcache using HAT-Trie (a data structure designed by Dr Nikolas Askitis)

## TODO:

1. [x] bind [HAT-Trie](https://github.com/luikore/hat-trie) to Go using CGO
2a. [x] `hat_map<string,int+byte>` stores index or special types (deque/set/etc) to `[][]byte` (aka raws); raws can be serialized using [FlatBuffers](http://github.com/google/flatbuffers) or [FastBinaryEncoding](http://github.com/chronoxor/FastBinaryEncoding)
2b. [ ] need benchmark which how much faster and space efficient: [][]byte compared to map[int][]byte
3. [ ] create a service for monitoring, and APIs using http2/grpc so it can be accessed from another language; also a client CLI, make sure all operation synchronized
4. [ ] the distributed part using emitter.io, or offloaded to another MQ (master-slave), or learn from [etcd](https://github.com/etcd-io/etcd/tree/master/raft) (multi-master)
5. [ ] data persisted to disk using lmdb, or leveldb with snappy compression
6. [ ] when service start, data loaded from database to memory; when service stopped/timer/sync-write forced, data written to disk
7. [ ] add TTL map, check for expiration when read, delete if expired; on-load check for expired data; create vaccum daemon to clean expired data
8. [ ] initial supported commands:
```
any type:
  SET/SETSTR/SETINT key value
  SETX/SETSTRX/SETINTX key ttl value
   master/leader write, journal, and broadcasting: internalSET key idx value ttl
   currenttime+ttl set to an array, and checked every second, execute DEL if expired
   the idx is 32-bit integer, 1 bit is for ttl flag, remaining 7-bit is for special type
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
9. when master/leader disconnected from all slave, new master/leader elected by reamining slave
