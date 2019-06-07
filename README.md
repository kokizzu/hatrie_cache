# hatrie_cache
Distributed memcache using HAT-Trie (a data structure designed by Dr Nikolas Askitis)

## TODO:

1. bind https://github.com/Tessil/hat-trie to Go using CGO, example: stackoverflow.com/questions/1713214 or github.com/burke/howto-go-with-cpp
2. hat_map<string,int> stores index to `[][]byte` (aka rawValues) or special type (deque/set/etc) if negative, counter if positive; rawValues serialized using flatbuffers or fastbinaryencoding
3. create a service for monitoring, and APIs using http2/grpc so it can be accessed from another language; also a client library
4. the distributed part using emitter.io, or github.com/etcd/raft, or offloaded to another MQ
5. data persisted to disk using lmdb, or leveldb
6. when service start, data loaded from database to memory; when service stopped/timer/sync-write forced, data written to disk
7. supported commands:
```
any type:
  SET key value ttl=-1
   leader write, journal, and broadcasting: internalSET key idx value ttl
   currenttime+ttl set to an array, and checked every second, execute DEL if expired
   the idx is 56-bit integer, 1 bit is for ttl flag, remaining 7-bit is for special type
  GET key
   check the value on the hat_map
  DEL key
   leader write, journal, and broadcasting: internalDEL key idx
   deleted index saved on another map
  TTL key
   check if key exists -1 if expired or not existshe
counter type:
  INC key value=1
    maximum of 56-bit unsigned integer
  INT key
    gets
```
8. in case of network partition, 
