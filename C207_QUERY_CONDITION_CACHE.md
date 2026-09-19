# C207 Query Condition Cache

Status: verified as already implemented.

ClickHouse describes its query condition cache as reusing filter matches for
repeated `WHERE` clauses on unchanged data, effectively acting as an ephemeral
index. See the official [ClickHouse query condition cache
overview](https://clickhouse.com/blog/alexey-favorite-features-2025).

Hatrie Cache implements the same core idea for eligible columnar sources. It
stores row positions that matched a predicate and keys them by:

- source kind and source key;
- the non-empty source version;
- normalized predicate expression;
- collation; and
- batch row count.

The executor requires a `SourceVersionResolver`, reads the source version
before scanning, and reads it again before reuse. A version change, missing
version, or resolver without version support bypasses the cache. Both
materialized result reads and streaming reads use the same validation path.
Entries and matched positions are bounded by caller-provided capacity and
`maximumMatchedRows`; the option is disabled by default.

## Verification

Existing tests cover cache reuse, version invalidation, streaming reuse,
unversioned sources, drifting versions, LRU capacity, and matched-row bounds:

```text
make test-sql-query-condition-cache
make race-c207
make vet-c207
```

All passed. No production code change was needed for this backlog item.

## Benchmark

Five benchmark samples were run with
`make benchmark-sql-query-condition-cache` on Linux/amd64. Median samples:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| No condition cache | 425,625 | 13,291 | 74 |
| Warm condition-cache hit | 18,532 | 13,150 | 72 |
| Cold condition-cache path | 486,974 | 15,378 | 88 |

The warm hit is about `23.0x` faster, with `1.01x` the bytes and `0.97x` the
allocations of the no-cache run. The cold path is about `1.14x` slower, uses
`1.16x` the bytes, and uses `1.19x` the allocations because it populates the
bounded match vector. These measurements are for a 20,000-row selective
columnar filter and are not an end-to-end application guarantee.
