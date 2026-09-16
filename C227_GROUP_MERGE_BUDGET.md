# C227: External Group-Merge Memory Budget

External `GROUP BY` spilling already bounds the in-memory input run with
`MaxGroupBytes`. Before this change, merging many spill runs could still retain
one decoded aggregate record per open run without an independent limit. That
could turn a bounded spill query into an unexpectedly large merge frontier.

`SQLQueryOptions.MaxGroupMergeBytes` is an opt-in guard for that frontier:

```go
options := hatSql.SQLQueryOptions{
	MaxGroupBytes:      16 << 20,
	MaxGroupMergeBytes: 64 << 20,
	SpillDirectory:     "/var/lib/hatrie/spill",
	MaxSpillBytes:      4 << 30,
}
```

The default is `0`, which preserves the existing unbounded merge behavior. A
negative value is rejected during query-option validation.

## What It Bounds

The guard estimates the decoded current record retained by each active merge
reader. The estimate uses the already-built formatted group key and fixed
aggregate-state widths, so checking does not allocate a temporary row or JSON
buffer. It is checked after the initial reader frontier is opened and after a
reader advances.

This is a merge-frontier budget, not a complete process RSS limit. Decoder
buffers, the caller's result row, the operating system page cache, and other
query operators are outside this estimate. A single oversized encoded record
can also be decoded before its retained estimate is checked.

If the estimate exceeds the configured budget, the query returns an error like:

```text
SQL group merge memory budget exceeded: estimated retained records exceed maximum 67108864 bytes
```

All temporary group spill and merge files are removed on this failure. The
option applies only when the existing external group-spill path is selected;
it does not change ordinary in-memory aggregation or unrelated sort spilling.

## Choosing Values

`MaxGroupBytes` controls how many partial records are placed in each run.
`MaxGroupMergeBytes` must cover the records retained by the configured merge
fan-in, plus normal application headroom. A budget that is too small is a
deliberate query rejection, not an automatic repartitioning policy. Keep it at
zero when the caller already controls query size elsewhere or wants the legacy
behavior.

## Measurement

The benchmark uses 2,048 groups, a 16 KiB input-run budget, one process thread,
five 250 ms samples per case, and `-benchmem`:

| Path | Median ns/op | Median B/op | Median allocs/op | Time x vs disabled | Heap x vs disabled | Alloc x vs disabled |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `MaxGroupMergeBytes = 0` | 14,276,884 | 5,833,192 | 68,090 | 1.00x | 1.00x | 1.00x |
| `MaxGroupMergeBytes = 1 MiB` | 14,606,824 | 5,833,178 | 68,090 | 1.02x | 1.00x | 1.00x |

The enabled path has no additional allocations and a low-single-digit runtime
cost in this spill-heavy workload. The first implementation accidentally used
the JSON row-size estimator on every check and measured about 2.9x runtime and
5.1x allocations; that implementation was removed before this version was
accepted.

Run the focused verification with:

```text
make test-c227-group-merge
make benchmark-c227-group-merge
```
