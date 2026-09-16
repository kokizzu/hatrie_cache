# CHG02 Small-Cardinality `GROUP BY` Index

The streamable and columnar grouped-aggregation paths now use an adaptive
index for their state lookup. The first four distinct normalized group keys
are kept in an inline array and searched linearly. On the fifth key, the
entries are promoted to the existing `map[string]int` representation.

This is automatic and has no configuration, wire-format, storage-format, or
SQL semantic change. It applies to the streamable grouped path, the single-
level columnar grouped path, and each worker in the two-level columnar grouped
path. First-seen group order, collation-normalized keys, duplicate handling,
limits, and aggregate results remain unchanged.

## Why four entries

The direct index benchmark measured the inline path as materially faster and
allocation-free for one and four groups. At eight groups the map was already
roughly comparable, so the small representation promotes at four entries
instead of carrying a longer linear scan. The threshold is an internal
constant and should only be changed with a new benchmark on representative
hardware.

The inline array is intentionally fixed in the index object. This avoids a
separate backing allocation for low-cardinality queries and avoids retaining a
temporary slice allocation after promotion. Large group counts still use the
normal map; the optimization does not attempt to replace the map for general
cardinality.

## Verification

The focused regression tests cover mixed normalized key strings, the exact
small capacity, promotion, and lookup of every promoted entry. The public SQL
test covers grouped result preservation. The benchmark and raw samples are in
[BENCHMARK.md](BENCHMARK.md#chg02-small-cardinality-group-by-index).

Run the repeatable checks with:

```text
make test-chg02
make test-chg02-package
make race-chg02
make vet-chg02
make benchmark-chg02
```
