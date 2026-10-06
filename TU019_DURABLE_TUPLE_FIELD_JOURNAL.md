# Durable Tuple Field-Operation Journal

T-U19 adds durable, replayable field mutations for versioned tuples. The
feature is intentionally opt-in: existing commands and tuple storage behavior
remain unchanged unless a caller sends the new commands.

## Commands

Commands are available through `HatTrie.ExecuteCommand` and
`CommandJournal.ExecuteCommand`.

### `TUPLESET`

Stores a validated `hatDataStructure.VersionedTuple`.

- `key`: normal Hatrie key.
- `binary_value`: embedded callers may use `Request.BinaryValue`.
- `value`: JSON/HTTP callers may send standard base64 of the marshaled tuple.
- TTL options are rejected because the durable replay contract is a tuple
  value, not an expiring cache entry.

Example request payload:

```json
{
  "command": "TUPLESET",
  "key": "order:1",
  "value": "<base64 of MarshalVersionedTuple output>"
}
```

### `TUPLEGET`

Returns the stored versioned tuple as standard base64 in `response.value`.
It does not expose the tuple's internal byte layout as JSON fields.

### `TUPLEUPDATE`

Applies one atomic, bounded batch. `values` contains objects with unique field
indexes:

```json
{
  "command": "TUPLEUPDATE",
  "key": "order:1",
  "values": [
    {"index": 0, "kind": "add_int64", "delta": 5},
    {"index": 1, "kind": "set", "value": "YWZ0ZXI="},
    {"index": 2, "kind": "splice", "start": 1, "remove": 2, "insert": "WFk="}
  ]
}
```

Supported operations are:

- `set`: replaces one field with base64 bytes.
- `splice`: removes `remove` bytes at `start` and inserts base64 bytes.
- `add_int64`: adds a signed 64-bit delta to an exactly eight-byte big-endian
  integer field.

The batch limit is 4096 operations. Duplicate indexes, malformed base64,
negative positions/counts, integer overflow, invalid tuple bytes, and unknown
operation kinds are rejected before mutation. A rejected batch leaves the
stored value unchanged.

## Replay and compatibility

The journal stores `TUPLESET` payloads as canonical base64 and stores validated
`TUPLEUPDATE` requests as their operation description. Replay applies the
entire update while holding the trie mutation lock, so concurrent updates do
not lose each other's changes. The versioned tuple envelope is retained across
updates; the journal does not embed a complete schema definition, so the
caller must keep the tuple format/version contract compatible across replay.

## Cost and limits

Field updates avoid resending the complete tuple when only a few fields
change, but JSON callers still pay base64 expansion for binary values and
operation metadata. Each update creates the replacement tuple bytes; this is a
correctness boundary, not an in-place mutation. The benchmark below measures
the low-level replay path and the JSON request-size tradeoff:

```text
make codex-tu19-benchmark
```

See the T-U19 section in `BENCHMARK.md` for the captured raw output.

## Security notes

The command path enforces key validation, bounded operation count, bounded
numeric parsing, strict base64 decoding, duplicate-index rejection, and
int64 overflow checks. It does not execute user code or interpret arbitrary
schema metadata. As with other journal commands, the journal file and its
transport still need the deployment's existing filesystem and authentication
controls.
