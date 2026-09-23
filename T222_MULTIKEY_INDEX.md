# T222: Multikey Array Indexes

The round-2 idea "multikey indexes over array-valued fields" was already
implemented before this ledger reached T222. This record verifies the existing
implementations and their current tradeoffs; it does not add a duplicate index.

## Implementations

- `hatDataStructure.StringMultikeyIndex` stores one sorted posting list per
  distinct string element and supports bounded, atomic updates and deletes.
- `hatDataStructure.TupleMultikeyIndex` keeps typed values separate, suppresses
  duplicate values in one tuple, and uses sorted compact postings.
- `CreateSQLJSONMultikeyIndex` indexes distinct JSON array elements for
  `ARRAY_CONTAINS`, refreshes from source generations, and rechecks candidates
  before returning rows. Unsupported scalar, range, ordered, and non-binary
  collation paths retain their existing fallbacks.

Detailed API and safety documentation remains in
[SQL_MULTIKEY_INDEX.md](SQL_MULTIKEY_INDEX.md),
[TG10_MULTIKEY_INDEX.md](TG10_MULTIKEY_INDEX.md), and
[TU23_TYPED_MULTIKEY_INDEX.md](TU23_TYPED_MULTIKEY_INDEX.md).

## Correctness Verification

The focused checks cover duplicate suppression, typed equality, atomic bounds,
updates/deletes, concurrent string-index access, SQL candidate rechecking, and
tuple format validation:

```text
make test-t222
make race-t222
make vet-t222
```

The SQL and typed tuple test suites passed before this documentation change.

## Benchmark

The fresh runs used `-benchmem -count=5` on Linux/amd64 with an AMD Ryzen 9
5950X. Values below are medians from the raw runs recorded in
`BENCHMARK.md`.

### SQL JSON array membership

| Workload | ns/op | bytes/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Full scan | 17,581,711 | 8,315,049 | 170,239 | 1.00x |
| Warm multikey index | 118,780 | 105,408 | 525 | 148.0x faster; 78.8x fewer bytes; 324.3x fewer allocations |

The index path is therefore a strong win for repeated membership queries, while
index storage and maintenance add a per-distinct-element cost to writes.

### Typed multikey lookup and build

| Workload | ns/op | bytes/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| String index lookup | 97.83 | 0 | 0 | 97.0x faster than map-of-sets; 2,077x faster than scan |
| Map-of-sets lookup | 9,495 | 0 | 0 | Baseline |
| Linear scan | 203,288 | 1 | 0 | Baseline for scan |
| Sorted string-index build | 3,469,377 | 2,197,396 | 21,347 | 1.69x slower and 2.03x more bytes than map-of-sets build |
| Map-of-sets build | 2,047,967 | 1,080,927 | 1,583 | Lower build cost baseline |

This makes the lookup path a clear win, but the compact sorted-posting index is
not free to build. It is appropriate when the index is reused enough to amortize
build and update work. The SQL benchmark measures a warmed index and does not
hide that maintenance cost.

Reproduce both groups with:

```text
make benchmark-t222
```

## Limits

Array fan-out can increase index memory and update cost. Empty arrays produce no
postings, duplicate elements are stored once per row, and bounded update limits
reject an operation atomically. Nested-array extraction, automatic schema
lifecycle management, and arbitrary non-binary collation remain caller/planner
responsibilities. These limits are intentional to preserve correctness and to
keep the opt-in index predictable.
