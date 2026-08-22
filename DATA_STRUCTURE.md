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
| Priority queue | `PUSHPQ`, `PEEKPQ`, `POPPQ`, `GETPQ` | Push `value` with integer `priority`; reads use `key`. | Peek/pop return selected item; pop removes it; get is ordered JSON. |

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

Output values are respectively `"admin"`, `"verify"`, `"1"`, and
`"urgent"`.

## Filters and probabilistic structures

| Data structure | Commands | Input | Output |
| --- | --- | --- | --- |
| Bloom filter | `CREATEBF`, `ADDBF`, `HASBF`, `INFOBF` | Create expected count in `value`; optional false-positive rate in `subkey` or `pairs`; add `value`/`values`. | `HASBF` is `"1"`/`"0"` and may be a false positive; info is JSON. |
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

See [BENCHMARK.md](BENCHMARK.md) for benchmark coverage and
[DS_SPLIT_proposal.md](DS_SPLIT_proposal.md) for the shared-index rationale.
