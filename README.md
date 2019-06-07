# hatrie_cache
Distributed memcache using HAT-Trie (a data structure designed by Dr Nikolas Askitis)

## TODO:

1. bind [HAT-Trie](https://github.com/Tessil/hat-trie) to Go using CGO, example: stackoverflow.com/questions/1713214 or github.com/burke/howto-go-with-cpp
2. hat_map<string,int> stores index to `[][]byte` (aka rawValues) or special type (deque/set/etc) if negative, counter if positive; rawValues serialized using [FlatBuffers](github.com/google/flatbuffers) or [FastBinaryEncoding](github.com/chronoxor/FastBinaryEncoding)
3. create a service for monitoring, and APIs using http2/grpc so it can be accessed from another language; also a client CLI
4. the distributed part using emitter.io, or offloaded to another MQ (master-slave), or learn from [etcd](https://github.com/etcd-io/etcd/tree/master/raft)(multi-master)
5. data persisted to disk using lmdb, or leveldb with snappy compression
6. when service start, data loaded from database to memory; when service stopped/timer/sync-write forced, data written to disk
7. initial supported commands:
```
any type:
  SET key value
  SETX key ttl value
   master/leader write, journal, and broadcasting: internalSET key idx value ttl
   currenttime+ttl set to an array, and checked every second, execute DEL if expired
   the idx is 56-bit integer, 1 bit is for ttl flag, remaining 7-bit is for special type
  GET key
   check the value on the hat_map
  DEL key
   master/leader write, journal, and broadcasting: internalDEL key idx
   deleted index saved on another map
  EXISTS key
  TTL key 
   check if key exists -1 if expired or not exists, >0 if has ttl
counter type:
  INC key value=1
    maximum of 63-bit unsigned integer, 0 if overflow or negative
  INT key
    gets
```
8. when master/leader disconnected from all slave, new master/leader elected by reamining slave
