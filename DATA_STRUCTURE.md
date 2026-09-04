# Data Structures and Command Reference

`hatrie_cache` uses one shared key namespace. A key holds one value family at a
time; a write of another family replaces it. The shared C HAT-trie stores a
compact type/index record, and typed backing pools hold larger payloads. TTL,
deletion, backup/restore, replication, and `BATCH` work across every family.

Send these JSON requests to `POST /api/commands`. The CLI equivalent is
`make cli ARGS='command ...'`.

## Request and response contract

### Request fields

| Field | Type | Meaning |
| --- | --- | --- |
| `command` | string | Required, case-insensitive command. |
| `key` | string | Required except for `BATCH`. |
| `value` | string | One text or textual numeric argument. |
| `values` | JSON array | Multiple values. |
| `subkey` | string | Secondary key, count, prefix, or numeric argument. |
| `pairs` | JSON object | Map fields or named creation options. |
| `priority` | integer | Priority for `PUSHPQ`. |
| `ttl_seconds` | integer | Relative expiry for `SETX`, `SETSTRX`, `SETINTX`, `EXPIRE`. |
| `unix_seconds` | integer | Absolute expiry for `EXPIREAT`. |
| `batch` | array | Ordered child requests for `BATCH`. |

### Response fields

```json
{"ok":true,"message":"ok","value":"..."}
```

Mutations usually omit `value`. Reads put their result in `value`; structured
results are JSON text in that field and should be decoded once by the client.
Invalid input or a type mismatch returns `{"ok":false,"message":"..."}`.
`BATCH` returns ordered child responses in `responses`; it is not transactional.

Input:

```json
{"command":"SETSTR","key":"profile:name","value":"Ivi"}
{"command":"GETSTR","key":"profile:name"}
```

Output:

```json
{"ok":true,"message":"ok"}
{"ok":true,"message":"ok","value":"Ivi"}
```

## Commands shared by every family

| Commands | Input | Output |
| --- | --- | --- |
| `GET`, `GETSTR` | `key` | Canonical value; structured data is JSON text. |
| `DUMP` | `key` | Tagged portable JSON entry for inspection/replication fallback. |
| `EXISTS` | `key` | `"1"` if live, otherwise `"0"`. |
| `DEL` | `key` | Removes the key, TTL, and backing payload. |
| `TTL` | `key` | Remaining seconds or the documented negative sentinel. |
| `EXPIRE` | `key`, `ttl_seconds` | Applies a relative TTL to a live key. |
| `EXPIREAT` | `key`, `unix_seconds` | Applies an absolute Unix TTL to a live key. |
| `PERSIST` | `key` | Removes a live key's TTL without changing its value. |
| `BATCH` | `batch` of public requests | Ordered `responses`; no rollback on failure. |

```json
{"command":"BATCH","batch":[
  {"command":"SETINT","key":"views","value":"41"},
  {"command":"INC","key":"views","value":"1"},
  {"command":"GET","key":"views"}
]}
```

The final child output is `{"ok":true,"message":"ok","value":"42"}`.

## Scalar values

| Data structure | Commands | Input | Output |
| --- | --- | --- | --- |
| Counter | `SETINT`, `SETINTX`, `INC` | Signed 32-bit `value`; `*X` also takes `ttl_seconds`; `INC` defaults to `1`. | Mutation succeeds; `GET` returns decimal value. Overflow is rejected. |
| String | `SET`, `SETSTR`, `SETX`, `SETSTRX` | Text `value`; `*X` also takes `ttl_seconds`. | Mutation succeeds; `GET`/`GETSTR` returns the text. |
| Bytes | Go API plus snapshot/persistence codecs; `GET`/`DUMP` inspect the canonical form. | Use the Go API when raw byte identity is needed. | Binary snapshot and replication paths preserve raw bytes. |

Input:

```json
{"command":"SETINTX","key":"rate:minute","value":"10","ttl_seconds":60}
{"command":"INC","key":"rate:minute","value":"2"}
```

Output: `{"ok":true,"message":"ok","value":"12"}`.

## Collections

| Data structure | Commands | Input | Output |
| --- | --- | --- | --- |
| Map | `PUTMAP`, `PEEKMAP`, `TAKEMAP` | Put `subkey` + `value`, or multiple fields in `pairs`; reads use `subkey`. | Peek returns without removal; take returns then removes. Objects/arrays are JSON text. |
| Slice/deque | `PUSHSLICE`, `POPSLICE`, `SHIFTSLICE`, `HEADSLICE`, `TAILSLICE` | Push one `value` or `values`; reads use `key`. | Pop/shift remove one end; head/tail read one end. |
| Set | `ADDSET`, `REMSET`, `HASSET`, `GETSET` | Add/remove one `value` or `values`; membership uses `value`. | Membership is `"1"`/`"0"`; get is a JSON array. |
| Priority queue | `PUSHPQ`, `PEEKPQ`, `POPPQ`, `GETPQ` | Push `value` with integer `priority`; reads use `key`. | Peek/pop return a JSON `{priority,value}` item; pop removes it; get is ordered JSON. |

Input:

```json
{"command":"PUTMAP","key":"user:7","pairs":{"name":"Ivi","role":"admin"}}
{"command":"PEEKMAP","key":"user:7","subkey":"role"}
{"command":"PUSHSLICE","key":"jobs","values":["build","verify"]}
{"command":"POPSLICE","key":"jobs"}
{"command":"ADDSET","key":"tags","values":["go","cache"]}
{"command":"HASSET","key":"tags","value":"go"}
{"command":"PUSHPQ","key":"queue","priority":10,"value":"urgent"}
{"command":"POPPQ","key":"queue"}
```

Output values are respectively `"admin"`, `"verify"`, `"1"`, and JSON
`{"priority":10,"value":"urgent"}`.

## Filters and probabilistic structures

| Data structure | Commands | Input | Output |
| --- | --- | --- | --- |
| Bloom filter | `CREATEBF`, `ADDBF`, `HASBF`, `INFOBF` | Create expected count in `value`; optional false-positive rate in `subkey` or `pairs`; add `value`/`values`. | `HASBF` is `"1"`/`"0"` and may be a false positive; info is JSON. |
| Token Bloom filter | `NewTokenBloomFilter`, `AddText`, `ContainsAllTokens`, `ContainsAnyTokens` | Import `hatrie_cache` or `hatrie_cache/hat/hatDataStructure`; add Unicode letter/digit text and use it as a conservative word-search prefilter. | Membership may be a false positive; empty `ContainsAllTokens` queries do not prune; snapshot uses the compact Bloom format. |
| Cuckoo filter | `CREATECF`, `ADDCF`, `HASCF`, `DELCF`, `INFOCF` | Capacity in `value`, optional false-positive rate in `subkey`; add/delete `value`/`values`. | Membership is `"1"`/`"0"`; delete is supported; info is JSON. |
| XOR filter | `CREATEXF`, `ADDXF`, `BUILDXF`, `HASXF`, `INFOXF` | Optional expected count in create `value`; add values; build before lookup. | Membership is `"1"`/`"0"` and may be a false positive; info reports build state. |
| Count-Min Sketch | `CREATECMS`, `INCRCMS`, `ESTCMS`, `INFOCMS` | Width in `value`, optional depth in `subkey`; increment item in `value`, optional count in `subkey`. | Estimate is a decimal approximate upper bound; info is JSON. |
| HyperLogLog | `CREATEHLL`, `ADDHLL`, `COUNTHLL`, `INFOHLL` | Optional precision in create `value`; add `value`/`values`. | Count is approximate cardinality; info is JSON. |
| Top-K | `CREATETOPK`, `ADDTOPK`, `ESTTOPK`, `GETTOPK`, `INFOTOPK` | Optional capacity; add item in `value`, optional increment in `subkey`. | Estimate is decimal; get is ordered JSON candidates; info is JSON. |
| Reservoir sample | `CREATERS`, `ADDRS`, `GETRS`, `INFORS` | Optional positive capacity; add `value`/`values`. | Get returns a JSON sample; info is JSON. |
| Quantile sketch | `CREATEQ`, `ADDQ`, `ESTQ`, `INFOQ` | Optional epsilon; add numeric `value`/`values`; estimate with quantile `value` in `[0,1]`. | Estimate is approximate numeric quantile; info is JSON. |

Input:

```json
{"command":"CREATEBF","key":"seen:email","value":"10000","subkey":"0.01"}
{"command":"ADDBF","key":"seen:email","value":"user@example.com"}
{"command":"HASBF","key":"seen:email","value":"user@example.com"}
{"command":"CREATECMS","key":"freq:path","value":"2048","subkey":"4"}
{"command":"INCRCMS","key":"freq:path","value":"/api/users","subkey":"3"}
{"command":"ESTCMS","key":"freq:path","value":"/api/users"}
```

Output: membership is `"1"`; the Count-Min estimate is at least `"3"`.

## Bitmaps and indexes

| Data structure | Commands | Input | Output |
| --- | --- | --- | --- |
| Roaring bitmap | `CREATERB`, `ADDRB`, `REMRB`, `HASRB`, `COUNTRB`, `GETRB`, `INFORB` | Create with key; add/remove/check uint32 IDs in `value`/`values`. | Membership is `"1"`/`"0"`; count is decimal; get is JSON IDs; info is JSON. |
| Sparse uint64 bitset | `CREATESB`, `ADDSB`, `REMSB`, `HASSB`, `COUNTSB`, `GETSB`, `INFOSB` | Create with key; add/remove/check uint64 IDs in `value`/`values`. | Same form as roaring, without the uint32 limit. |
| Radix-tree prefix index | `CREATERT`, `PUTRT`, `GETRT`, `DELRT`, `HASRT`, `PREFIXRT`, `INFORT` | Put `subkey` index + `value` payload. Get/delete/has use `subkey`; prefix has optional `subkey`. | Get returns payload; has is `"1"`/`"0"`; prefix and info are JSON. |
| Fenwick tree | `CREATEFW`, `ADDFW`, `GETFW`, `SUMFW`, `RANGEFW`, `INFOFW` | Create positive size in `value`; add: index `value`, delta `subkey`; range: start `value`, end `subkey`. | Point/prefix/range output is decimal; info is JSON. |

Input:

```json
{"command":"CREATERB","key":"cohort"}
{"command":"ADDRB","key":"cohort","values":["7","42"]}
{"command":"COUNTRB","key":"cohort"}
{"command":"CREATERT","key":"sessions"}
{"command":"PUTRT","key":"sessions","subkey":"user:7/profile","value":"active"}
{"command":"PREFIXRT","key":"sessions","subkey":"user:"}
{"command":"CREATEFW","key":"hourly","value":"24"}
{"command":"ADDFW","key":"hourly","value":"13","subkey":"7"}
{"command":"RANGEFW","key":"hourly","value":"8","subkey":"13"}
```

Output: `COUNTRB` is `"2"`; prefix contains the `user:7` entry; the Fenwick
range result is `"7"` for this single update.

## Command-by-command state transitions

This is the practical manual for a first-time user. `∅` means the key does not
exist. `->` describes the persistent state change, not the response. All
examples use the canonical command name; accepted compatibility aliases have
the same effect. The executable version of the ordinary public flows is
`TestDataStructureGuideExamples` in `data_structure_examples_test.go`.

To keep each table readable, the `Request` column shows fields in addition to
the command in the first column. A complete HTTP body adds it back, for
example `{"command":"SET","key":"name","value":"Ivi"}`. The `Reply`
column shows the meaningful `message` and/or `value`; the full JSON envelope is
always described in [Response fields](#response-fields).

### Key lifecycle and scalar values

| Command | Before state | Request | Reply | After state |
| --- | --- | --- | --- | --- |
| `GET` | `name=∅` | `{"key":"name"}` | `key not found` | `name=∅` |
| `DUMP` | `name="Ivi"` | `{"key":"name"}` | tagged JSON entry containing `Ivi` | unchanged |
| `EXISTS` | `name=∅` | `{"key":"name"}` | `value:"0"` | unchanged |
| `SET` | `name=∅` | `{"key":"name","value":"Ivi"}` | `stored string` | `name="Ivi"` |
| `SETX` | `name=∅` | `{"key":"name","value":"Ivi","ttl_seconds":60}` | `stored string with ttl` | `name="Ivi"`, expires in 60 s |
| `SETINT` | `views=∅` | `{"key":"views","value":"41"}` | `stored counter` | `views=41` |
| `SETINTX` | `views=∅` | `{"key":"views","value":"41","ttl_seconds":60}` | `stored counter with ttl` | `views=41`, expires in 60 s |
| `INC` | `views=41` | `{"key":"views","value":"1"}` | `value:"42"` | `views=42` |
| `DEL` | `name="Ivi"` | `{"key":"name"}` | `deleted` | `name=∅` |
| `TTL` | `name="Ivi"`, no expiry | `{"key":"name"}` | `value:"-1"` | unchanged |
| `EXPIRE` | `name="Ivi"` | `{"key":"name","ttl_seconds":60}` | `ttl updated` | same value, expires in 60 s |
| `EXPIREAT` | `name="Ivi"` | `{"key":"name","unix_seconds":1893456000}` | `ttl updated` | same value, expires at that Unix second |
| `PERSIST` | `name="Ivi"` with a TTL | `{"key":"name"}` | `ttl removed` | same value, no TTL |
| `BATCH` | `views=∅` | `{"batch":[{"command":"SETINT","key":"views","value":"41"},{"command":"INC","key":"views","value":"1"},{"command":"GET","key":"views"}]}` | three ordered `responses`; last is `42` | `views=42` |

`GETSTR` is an alias of `GET`; `SETSTR` is an alias of `SET`; `SETSTRX` is an
alias of `SETX`. `SET`, `SETINT`, and every `CREATE*` command replace a live
value of another type at the same key. `INC` rejects non-counters and 32-bit
overflow. A positive `ttl_seconds` is required for `*X` and `EXPIRE`.

### Map, deque, set, and priority queue

| Command | Before state | Request | Reply | After state |
| --- | --- | --- | --- | --- |
| `PUTMAP` | `user=∅` | `{"key":"user","pairs":{"name":"Ivi","role":"admin"}}` | `stored map fields` | `user={name:Ivi,role:admin}` |
| `PEEKMAP` | `user.role="admin"` | `{"key":"user","subkey":"role"}` | `value:"admin"` | unchanged |
| `TAKEMAP` | `user.role="admin"` | `{"key":"user","subkey":"role"}` | `removed`, `value:"admin"` | `user={name:Ivi}` |
| `PUSHSLICE` | `jobs=∅` | `{"key":"jobs","values":["build","verify","deploy"]}` | `pushed slice values` | `jobs=[build,verify,deploy]` |
| `POPSLICE` | `jobs=[build,verify,deploy]` | `{"key":"jobs"}` | `removed`, `value:"deploy"` | `jobs=[build,verify]` |
| `SHIFTSLICE` | `jobs=[build,verify]` | `{"key":"jobs"}` | `removed`, `value:"build"` | `jobs=[verify]` |
| `HEADSLICE` | `jobs=[build,verify]` | `{"key":"jobs"}` | `value:"build"` | unchanged |
| `TAILSLICE` | `jobs=[build,verify]` | `{"key":"jobs"}` | `value:"verify"` | unchanged |
| `ADDSET` | `tags=∅` | `{"key":"tags","values":["go","cache","go"]}` | `value:"2"` newly added | `tags={go,cache}` |
| `REMSET` | `tags={go,cache}` | `{"key":"tags","value":"go"}` | `value:"1"` removed | `tags={cache}` |
| `HASSET` | `tags={cache}` | `{"key":"tags","value":"go"}` | `value:"0"` | unchanged |
| `GETSET` | `tags={cache}` | `{"key":"tags"}` | `value:"[\\"cache\\"]"` | unchanged |
| `PUSHPQ` | `queue=∅` | `{"key":"queue","priority":10,"value":"urgent"}` | `value:"1"` item added | `queue=[{priority:10,value:urgent}]` |
| `PEEKPQ` | one `urgent` item | `{"key":"queue"}` | JSON `{"priority":10,"value":"urgent"}` | unchanged |
| `POPPQ` | one `urgent` item | `{"key":"queue"}` | JSON `{"priority":10,"value":"urgent"}` | `queue=[]` |
| `GETPQ` | one `urgent` item | `{"key":"queue"}` | JSON array of priority/value items | unchanged |

`PUSHSLICE` appends; `POPSLICE` removes from the tail; `SHIFTSLICE` removes
from the head. Sets ignore duplicate additions. Priority-queue reads include
both priority and value, so a client can see why an item was selected.

### Filters and sketches

| Command | Before state | Request | Reply | After state |
| --- | --- | --- | --- | --- |
| `CREATEBF` | `seen=∅` | `{"key":"seen","value":"1000","subkey":"0.01"}` | `created bloom filter` | empty Bloom filter, capacity target 1000 |
| `ADDBF` | Bloom filter `{}` | `{"key":"seen","value":"alice"}` | `value:"1"` added | filter may report `alice` present |
| `HASBF` | Bloom filter with `alice` added | `{"key":"seen","value":"alice"}` | `value:"1"` | unchanged; a `1` can be a false positive for unadded values |
| `INFOBF` | live Bloom filter | `{"key":"seen"}` | JSON configuration/statistics | unchanged |
| `CREATECF` | `active=∅` | `{"key":"active","value":"1000","subkey":"0.01"}` | `created cuckoo filter` | empty Cuckoo filter |
| `ADDCF` | Cuckoo filter `{}` | `{"key":"active","value":"alice"}` | `value:"1"` added | filter may report `alice` present |
| `HASCF` | Cuckoo filter with `alice` | `{"key":"active","value":"alice"}` | `value:"1"` | unchanged |
| `DELCF` | Cuckoo filter with `alice` | `{"key":"active","value":"alice"}` | `value:"1"` removed | filter reports `alice` absent |
| `INFOCF` | live Cuckoo filter | `{"key":"active"}` | JSON configuration/statistics | unchanged |
| `CREATEXF` | `allow=∅` | `{"key":"allow","value":"1000"}` | `created xor filter` | empty, unbuilt XOR filter |
| `ADDXF` | unbuilt XOR filter | `{"key":"allow","value":"alice"}` | `value:"1"` staged | staged source includes `alice`; still unbuilt |
| `BUILDXF` | staged `alice` | `{"key":"allow"}` | `built xor filter` plus JSON info | built filter can answer queries |
| `HASXF` | built filter with `alice` staged | `{"key":"allow","value":"alice"}` | `value:"1"` | unchanged; querying before build is an error |
| `INFOXF` | live XOR filter | `{"key":"allow"}` | JSON info including build state | unchanged |
| `CREATECMS` | `freq=∅` | `{"key":"freq","value":"256","subkey":"4"}` | `created count-min sketch` | empty width-256/depth-4 sketch |
| `INCRCMS` | estimate(`path`)=0 | `{"key":"freq","value":"path","subkey":"3"}` | `value:"3"` estimate | estimate(`path`) is at least 3 |
| `ESTCMS` | `path` incremented by 3 | `{"key":"freq","value":"path"}` | `value:"3"` in this flow | unchanged; count-min answers are approximate upper bounds |
| `INFOCMS` | live Count-Min Sketch | `{"key":"freq"}` | JSON configuration/statistics | unchanged |
| `CREATEHLL` | `visitors=∅` | `{"key":"visitors","value":"14"}` | `created hyperloglog` | empty HLL, precision 14 |
| `ADDHLL` | empty HLL | `{"key":"visitors","values":["alice","bob"]}` | current approximate cardinality | HLL represents two distinct inputs |
| `COUNTHLL` | HLL with two inputs | `{"key":"visitors"}` | positive approximate cardinality | unchanged |
| `INFOHLL` | live HLL | `{"key":"visitors"}` | JSON configuration/statistics | unchanged |
| `CREATETOPK` | `popular=∅` | `{"key":"popular","value":"3"}` | `created top-k` | empty Top-K, capacity 3 |
| `ADDTOPK` | empty Top-K | `{"key":"popular","value":"alpha","subkey":"5"}` | JSON estimate, `count:5` | `alpha` tracked with count 5 |
| `ESTTOPK` | `alpha` tracked | `{"key":"popular","value":"alpha"}` | JSON estimate, `count:5` | unchanged |
| `GETTOPK` | `alpha` tracked | `{"key":"popular"}` | JSON ranked candidate list | unchanged |
| `INFOTOPK` | live Top-K | `{"key":"popular"}` | JSON configuration/statistics | unchanged |
| `CREATERS` | `sample=∅` | `{"key":"sample","value":"3"}` | `created reservoir sample` | empty sample, capacity 3 |
| `ADDRS` | empty capacity-3 sample | `{"key":"sample","values":["alpha","beta"]}` | JSON update | sample contains `alpha`,`beta` |
| `GETRS` | sample has two values | `{"key":"sample"}` | JSON `alpha`,`beta` array | unchanged |
| `INFORS` | live reservoir sample | `{"key":"sample"}` | JSON configuration/statistics | unchanged |
| `CREATEQ` | `latency=∅` | `{"key":"latency","value":"0.01"}` | `created quantile sketch` | empty quantile sketch, epsilon 0.01 |
| `ADDQ` | empty quantile sketch | `{"key":"latency","values":["10","20","30"]}` | JSON update/estimate | sketch represents those observations |
| `ESTQ` | values 10,20,30 observed | `{"key":"latency","value":"0.5"}` | JSON approximate median | unchanged |
| `INFOQ` | live quantile sketch | `{"key":"latency"}` | JSON configuration/statistics | unchanged |

Bloom, Cuckoo, and XOR filters answer membership with possible false positives;
they cannot return their original inserted values. Count-Min, HyperLogLog,
Top-K, reservoir, and quantile structures deliberately trade exact history for
bounded memory. The reservoir becomes random once more input values than its
capacity arrive.

### Bitmaps, radix tree, and Fenwick tree

| Command | Before state | Request | Reply | After state |
| --- | --- | --- | --- | --- |
| `CREATERB` | `cohort=∅` | `{"key":"cohort"}` | `created roaring bitmap` | empty uint32 bitmap |
| `ADDRB` | empty bitmap | `{"key":"cohort","values":["7","42"]}` | `value:"2"` added | `{7,42}` |
| `REMRB` | `{7,42}` | `{"key":"cohort","value":"7"}` | `value:"1"` removed | `{42}` |
| `HASRB` | `{42}` | `{"key":"cohort","value":"42"}` | `value:"1"` | unchanged |
| `COUNTRB` | `{7,42}` | `{"key":"cohort"}` | `value:"2"` | unchanged |
| `GETRB` | `{7,42}` | `{"key":"cohort"}` | JSON `[7,42]` | unchanged |
| `INFORB` | live bitmap | `{"key":"cohort"}` | JSON size/container information | unchanged |
| `CREATESB` | `ids=∅` | `{"key":"ids"}` | `created sparse bitset` | empty uint64 set |
| `ADDSB` | empty sparse bitset | `{"key":"ids","values":["7","18446744073709551615"]}` | `value:"2"` added | two uint64 IDs present |
| `REMSB` | IDs include 7 | `{"key":"ids","value":"7"}` | `value:"1"` removed | ID 7 absent |
| `HASSB` | IDs include 7 | `{"key":"ids","value":"7"}` | `value:"1"` | unchanged |
| `COUNTSB` | two IDs present | `{"key":"ids"}` | `value:"2"` | unchanged |
| `GETSB` | IDs 7 and max uint64 | `{"key":"ids"}` | JSON ID array | unchanged |
| `INFOSB` | live sparse bitset | `{"key":"ids"}` | JSON size/container information | unchanged |
| `CREATERT` | `sessions=∅` | `{"key":"sessions"}` | `created radix tree` | empty string-prefix index |
| `PUTRT` | empty radix tree | `{"key":"sessions","subkey":"user:7/profile","value":"active"}` | `value:"1"` stored | one indexed key/value |
| `GETRT` | indexed profile is active | `{"key":"sessions","subkey":"user:7/profile"}` | `value:"active"` | unchanged |
| `DELRT` | indexed profile is active | `{"key":"sessions","subkey":"user:7/profile"}` | `value:"1"` removed | indexed profile absent |
| `HASRT` | indexed profile is active | `{"key":"sessions","subkey":"user:7/profile"}` | `value:"1"` | unchanged |
| `PREFIXRT` | entries start `user:7/` and `team:2/` | `{"key":"sessions","subkey":"user:"}` | JSON matching entries | only `user:` entries returned; state unchanged |
| `INFORT` | live radix tree | `{"key":"sessions"}` | JSON count/size information | unchanged |
| `CREATEFW` | `hourly=∅` | `{"key":"hourly","value":"24"}` | `created fenwick tree` | 24 zero-valued positions |
| `ADDFW` | cell 13 is 0 | `{"key":"hourly","value":"13","subkey":"7"}` | JSON update | cell 13 is 7 |
| `GETFW` | cell 13 is 7 | `{"key":"hourly","value":"13"}` | `value:"7"` | unchanged |
| `SUMFW` | only cell 13 is 7 | `{"key":"hourly","value":"13"}` | `value:"7"` | unchanged; sum is positions 1 through 13 |
| `RANGEFW` | only cell 13 is 7 | `{"key":"hourly","value":"8","subkey":"13"}` | `value:"7"` | unchanged; inclusive range 8 through 13 |
| `INFOFW` | live Fenwick tree | `{"key":"hourly"}` | JSON size/update information | unchanged |

Roaring bitmaps accept unsigned 32-bit IDs; sparse bitsets accept unsigned
64-bit IDs. A radix tree is for string-key prefix lookup, not the cache's main
key index. A Fenwick tree is for fast point updates and prefix/range sums; its
positions are one-based in these commands.

### Replication-only protocol commands

| Command | Before state | Request | Reply | After state |
| --- | --- | --- | --- | --- |
| `INTERNALSET` | authenticated replica has old entry | encoded JSON snapshot entry | `internal value stored` | key matches encoded source entry |
| `INTERNALDEL` | authenticated replica has key | replication delete for key | `internal value deleted` | key is absent |
| `INTERNALSETV2` | authenticated replica has old entry | binary compatibility record | internal replication acknowledgement | key matches source entry |
| `INTERNALSETV3` | authenticated replica has old entry | compact keyless binary record | internal replication acknowledgement | key matches source entry |
| `INTERNALBATCH` | replica has multiple old entries | encoded internal command list | one internal batch acknowledgement | listed changes applied in order |
| `INTERNALBATCHV2` | replica has multiple old entries | binary internal command list | one internal batch acknowledgement | listed changes applied in order |
| `INTERNALDIGESTV1` | replica needs anti-entropy page | authenticated topology-scoped digest request | read-only digest page | unchanged |

These seven commands are intentionally not runnable examples. They are
authenticated replication wire protocol, not stable application API; use
public commands, backup/restore, or replication endpoints instead.

### Accepted command aliases

The canonical names above are preferred in new code. The following aliases are
accepted for compatibility and have exactly the same before/after effect as the
canonical command in the state tables.

| Canonical command | Accepted aliases |
| --- | --- |
| `GET` | `GETSTR` |
| `SET` | `SETSTR` |
| `SETX` | `SETSTRX` |
| `PUSHPQ` | `PUSHPRIORITY` |
| `PEEKPQ` | `PEEKPRIORITY` |
| `POPPQ` | `POPPRIORITY` |
| `GETPQ` | `GETPRIORITY` |
| `CREATEBF` | `RESERVEBF`, `BFRESERVE` |
| `ADDBF` | `BFADD` |
| `HASBF` | `BFHAS`, `BFEXISTS` |
| `INFOBF` | `BFINFO` |
| `CREATECF` | `RESERVECF`, `CFRESERVE` |
| `ADDCF` | `CFADD` |
| `HASCF` | `CFHAS`, `CFEXISTS` |
| `DELCF` | `REMCF`, `CFDEL` |
| `INFOCF` | `CFINFO` |
| `CREATEXF` | `RESERVEXF`, `XFRESERVE`, `CREATEXOR` |
| `ADDXF` | `XFADD` |
| `BUILDXF` | `XFBUILD` |
| `HASXF` | `XFHAS`, `XFEXISTS` |
| `INFOXF` | `XFINFO` |
| `CREATERB` | `CREATEROARING`, `RBRESERVE` |
| `ADDRB` | `RBADD` |
| `REMRB` | `DELRB`, `RBREM`, `RBDEL` |
| `HASRB` | `RBHAS`, `RBEXISTS` |
| `COUNTRB` | `RBCOUNT` |
| `GETRB` | `RBGET` |
| `INFORB` | `RBINFO` |
| `CREATESB` | `CREATESPARSEBITSET`, `SBRESERVE` |
| `ADDSB` | `SBADD` |
| `REMSB` | `DELSB`, `SBREM`, `SBDEL` |
| `HASSB` | `SBHAS`, `SBEXISTS` |
| `COUNTSB` | `SBCOUNT` |
| `GETSB` | `SBGET` |
| `INFOSB` | `SBINFO` |
| `CREATERT` | `CREATERADIX`, `RTCREATE` |
| `PUTRT` | `RTPUT` |
| `GETRT` | `RTGET` |
| `DELRT` | `REMRT`, `RTDEL`, `RTREM` |
| `HASRT` | `RTEXISTS`, `RTHAS` |
| `PREFIXRT` | `SCANRT`, `RTPREFIX`, `RTSCAN` |
| `INFORT` | `RTINFO` |
| `CREATECMS` | `RESERVECMS`, `CMSRESERVE` |
| `INCRCMS` | `ADDCMS`, `CMSADD` |
| `ESTCMS` | `QUERYCMS`, `CMSQUERY`, `CMSCOUNT` |
| `INFOCMS` | `CMSINFO` |
| `CREATEHLL` | `RESERVEHLL`, `HLLRESERVE` |
| `ADDHLL` | `HLLADD` |
| `COUNTHLL` | `ESTHLL`, `HLLCOUNT`, `HLLCARD` |
| `INFOHLL` | `HLLINFO` |
| `CREATETOPK` | `RESERVETOPK`, `TOPKRESERVE` |
| `ADDTOPK` | `TOPKADD` |
| `ESTTOPK` | `QUERYTOPK`, `TOPKCOUNT` |
| `GETTOPK` | `TOPK` |
| `INFOTOPK` | `TOPKINFO` |
| `CREATERS` | `CREATESAMPLE`, `RESERVERS`, `RSRESERVE` |
| `ADDRS` | `RSADD` |
| `GETRS` | `RSGET`, `SAMPLE` |
| `INFORS` | `RSINFO` |
| `CREATEQ` | `CREATEQS`, `CREATEQUANTILE`, `RESERVEQ`, `QSRESERVE` |
| `ADDQ` | `ADDQS`, `QADD`, `QSADD` |
| `ESTQ` | `QUERYQ`, `QQUERY`, `QSQUERY`, `QUANTILE` |
| `INFOQ` | `QINFO`, `INFOQS`, `QSINFO` |
| `CREATEFW` | `CREATEFENWICK`, `RESERVEFW`, `FWRESERVE` |
| `ADDFW` | `FWADD` |
| `GETFW` | `FWGET` |
| `SUMFW` | `PREFIXFW`, `FWPREFIX`, `FWSUM` |
| `RANGEFW` | `FWRANGE` |
| `INFOFW` | `FWINFO` |

## Type replacement and internal commands

The first typed command creates or replaces the value at a key. `SETSTR`
followed by `ADDSET`, for example, makes the key a set; it does not retain a
hidden string. A type-specific command against an incompatible live value
returns an error and leaves it unchanged. Use `DUMP` to inspect the tagged form.

`INTERNALSET`, `INTERNALSETV2`, `INTERNALSETV3`, `INTERNALDEL`,
`INTERNALBATCH`, `INTERNALBATCHV2`, and `INTERNALDIGESTV1` are authenticated
internal replication protocol commands. Their input/output is encoded
snapshot/journal data, not stable application API. Do not send them from an
application client; use public commands, `BATCH`, backup/restore, or the
replication endpoints.

See [BENCHMARK.md](BENCHMARK.md) for benchmark coverage,
[DS_SPLIT_PROPOSAL.md](DS_SPLIT_PROPOSAL.md) for the shared-index rationale,
and [INDEX_PROPOSAL.md](INDEX_PROPOSAL.md) for typed SQL index design.
