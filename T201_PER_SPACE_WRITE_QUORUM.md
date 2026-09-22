# T201 Per-Space Write Quorum

T201 adds an opt-in synchronous replication quorum for selected logical
keyspaces. A keyspace is represented by a key prefix so existing HTTP, gRPC,
and embedded command schemas remain wire-compatible.

## Configuration

Configure either `MonitoringOptions` or `CacheGRPCOptions`:

```go
policy := &hatCache.WriteQuorumPolicy{
	Rules: []hatCache.WriteQuorumRule{
		{KeyPrefix: "critical:", Quorum: 2},
		{KeyPrefix: "critical:billing:", Quorum: 3},
	},
}

options := hatCache.MonitoringOptions{
	Replicator:        replicator,
	WriteQuorumPolicy: policy,
}
```

`KeyPrefix` must be non-empty and `Quorum` must be positive. The longest
matching prefix wins, so `critical:billing:` overrides `critical:`. Duplicate
prefixes are rejected. The policy is not serialized into command requests and
does not change the HTTP or gRPC wire format.

## Semantics

- `nil` or an empty policy is off by default and preserves existing behavior.
- The policy applies to single public write commands and atomic public batches.
- For an atomic batch, the highest matching policy quorum across its records is
  required before the batch is considered replicated.
- The effective requirement is the maximum of the global `WriteQuorum`, the
  request `InsertQuorum`, and the matching policy quorum.
- Internal replication commands are excluded from policy matching.
- A configured quorum still requires a synchronous replicator; asynchronous
  replication is rejected with the existing quorum error.
- The local write is retained when remote acknowledgements are insufficient,
  matching the existing global quorum behavior.

This lets ordinary cache traffic keep asynchronous throughput while selected
critical records receive stronger durability and fail-fast acknowledgement
semantics.

## Verification

The focused tests cover longest-prefix selection, invalid rules, critical
single writes, ordinary asynchronous writes, and atomic batches. The lookup
benchmark uses three rules on Linux/amd64 with an AMD Ryzen 9 5950X:

| Mode | Median ns/op | B/op | Allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Policy disabled | 7.5 | 0 | 0 | 1.00x |
| Configured matching key | 75.4 | 0 | 0 | 10.05x |
| Configured non-matching key | 76.3 | 0 | 0 | 10.17x |

The absolute lookup overhead is about 68-69 ns and the feature adds no heap
allocation. These numbers measure policy resolution only; a synchronous
replication request is dominated by the replica network round trip and is not
pretended to be free.

Run the focused checks with:

```sh
make format-t201
make test-t201
make benchmark-t201
```
