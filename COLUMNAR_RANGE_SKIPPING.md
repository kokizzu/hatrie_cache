# Columnar Numeric Range Skipping

`hatSql` now uses the existing `SegmentedColumnarSourceResolver` metadata to
skip numeric columnar segments that cannot satisfy a direct filter. This is a
ClickHouse-style data-skipping optimization for ordinary field-only `CACHE`
queries; it does not enable automatic sharding or change any configuration
default.

## Scope

The optimization applies to direct numeric comparisons joined by `AND`:

```sql
SELECT id, payload
FROM CACHE('events') AS event
WHERE event.id >= 1792 AND event.id < 2048
```

The resolver supplies `ColumnarNumericSegments` with a `RowsPerSegment` and
optional `Minimum`/`Maximum` bounds for each field. A segment is skipped only
when the available valid bounds prove that it cannot match. Missing metadata,
invalid metadata, unsupported expressions, `OR`, joins, ordering, aggregates,
and typed-column paths retain the established executor.

The row-level numeric matcher still evaluates every admitted row. Segment
metadata is therefore only a conservative candidate filter, and cannot change
SQL NULL, NaN, type-conversion, or comparison semantics. `EXPLAIN ANALYZE`
reports skipped rows as `COLUMNAR NUMERIC SEGMENT SKIP`.

The same behavior is used by materialized results and `ExecuteSQLQueryRows`.
There is no new flag: the resolver must opt in by implementing the existing
segmented-columnar contract, and resolvers that do not provide metadata keep
the prior scan.

## Benchmark

Command:

```text
make benchmark-columnar-segment-skip-local-clean
```

Machine: AMD Ryzen 9 5950X, Linux/amd64. Five benchmark samples used eight
256-row segments (2,048 rows) and selected the final segment with
`event.id >= 1792`.

Raw samples:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Without segment skip | 102,859 | 94,105 | 543 |
| Without segment skip | 104,224 | 94,104 | 543 |
| Without segment skip | 109,223 | 94,104 | 543 |
| Without segment skip | 107,711 | 94,104 | 543 |
| Without segment skip | 107,848 | 94,104 | 543 |
| With segment skip | 57,699 | 94,104 | 543 |
| With segment skip | 67,777 | 94,104 | 543 |
| With segment skip | 55,852 | 94,104 | 543 |
| With segment skip | 55,591 | 94,104 | 543 |
| With segment skip | 57,335 | 94,104 | 543 |

Median comparison:

| Path | Median ns/op | B/op | allocs/op | Relative CPU | Relative memory | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Full numeric segment scan | 107,711 | 94,104 | 543 | 1.00x | 1.00x | 1.00x |
| Numeric range segment skip | 57,335 | 94,104 | 543 | 1.88x faster | 1.00x | 1.00x |

The gain is CPU-only for this in-memory benchmark because the result still
materializes the same two rows. Larger or remote-backed columnar sources can
also avoid loading skipped segment columns when their resolver honors the
metadata boundary.

## Verification

The focused tests cover exact results, `EXPLAIN ANALYZE` skip reporting, the
callback API, and existing aggregate segment behavior. Package normal, race,
and vet checks must remain green before delivery.
