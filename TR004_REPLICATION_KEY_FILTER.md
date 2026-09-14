# TR-04 Replication Key-Prefix Filters

This adopts the useful part of Tarantool's space/key-range replication idea for
operator-defined regional partitions. It is an explicit filter, not automatic
sharding: the normal topology, leader election, backup, and restore contracts
remain unchanged.

## Configuration

The default is disabled. Configure the importable API with literal prefixes:

```go
replicator := hatriecache.NewHTTPReplicator(hatriecache.HTTPReplicatorOptions{
	Self:                 "node-a",
	Topology:             topology,
	ReplicationKeyPrefixes: []string{"region:eu:", "region:asia:"},
})
```

For the monitoring server, use a comma-separated environment value or flag:

```text
REPLICATION_KEY_PREFIXES=region:eu:,region:asia:
-replication-key-prefixes region:eu:,region:asia:
```

An empty value disables filtering. Prefix matching is case-sensitive and uses
the key bytes literally. The API copies the caller slice at construction;
empty entries are ignored, duplicate entries are removed, and at most 64
prefixes or 4,096 total prefix bytes are retained. The CLI trims whitespace
around comma-separated entries, so use the Go API when leading or trailing
spaces are significant key bytes.

## Behavior

- Live command replication skips successful mutations outside the configured
  prefixes before routing or network work.
- Command fan-out anti-entropy and its fallback paths only send selected source
  entries.
- Digest requests carry the bounded prefix set, and the receiving peer applies
  the same scope to roots, pages, and deletion comparison.
- Merkle whole-dataset fast sync is bypassed while filtering is enabled because
  its whole-tree root cannot represent a filtered view.
- Stale keys inside the selected prefixes can still be deleted. Keys outside
  the selected prefixes are preserved, including when an older peer ignores
  the new digest metadata.
- This only filters command replication. Journal-only replication, local
  storage, snapshots, backups, and restores are not filtered.

Use `REPLICATION_MODE=command` or `REPLICATION_MODE=dual`. Keep the filter
configuration in deployment configuration and test a representative sync
before enabling deletion-based anti-entropy in production. The filter is not
an authorization boundary; retain replication authentication and topology
validation.

## Cost

The disabled matcher is a fast zero-allocation branch. On the benchmark host,
the five-sample medians were 0.476 ns/op with filtering disabled, 3.381 ns/op
for one matching prefix, 11.20 ns/op for a four-prefix match at the end, and
11.79 ns/op for a four-prefix miss. The benefit is proportional to skipped
mutation and digest traffic; configuration with many prefixes pays a linear
prefix-check cost but remains allocation-free on the hot path. See
[BENCHMARK.md](BENCHMARK.md#tr-04-replication-key-prefix-filter).
