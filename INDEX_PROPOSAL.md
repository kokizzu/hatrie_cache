# Typed SQL Index Proposal

## Status

This is a design proposal; it is **not implemented**. It covers indexes for
`WHERE`, equality joins, `GROUP BY`, `ORDER BY`, and `LIMIT` over JSON rows from
`CACHE('key')`.

## Current behavior

The current opt-in API is:

```go
trie.CreateSQLJSONFieldIndex("users", "team_id")
```

It is a generic JSON field index. On a changed source value it reparses the
JSON, constructs an equality map from canonical values to copied `SQLRow`s,
and creates a sorted row slice for comparisons. That is a **full rebuild** on
each changed JSON source. It already serves qualified equality/range `WHERE`
predicates, indexed equality joins (inner and left), and compatible `ORDER BY`
plans. It does not stream `GROUP BY` runs, and ordered paths still clone rows
into a query result before `LIMIT` can reduce the working set.

The generic index is a correct baseline, not the target for large or frequently
updated data: it duplicates row maps, has `interface{}` comparison costs, and
cannot know whether a field is an integer, date, datetime, string, or boolean.

## Goals

- Retain exact SQL comparison, NULL, collation, and ordering semantics.
- Avoid copying `SQLRow` maps into an index; store compact row references.
- Make equality lookup, range lookup, grouping, and ordered scans fast.
- Select an implementation by measured CPU, retained memory, build time, and
  write/update cost, rather than assuming HAT-trie is best for every type.
- Keep typed indexes opt-in and disabled by default.

## Proposed API and metadata

Add a typed internal SQL index specification while preserving
`CreateSQLJSONFieldIndex` as the generic compatibility API:

```go
type SQLIndexType string

const (
    SQLIndexString   SQLIndexType = "string"
    SQLIndexInt64    SQLIndexType = "int64"
    SQLIndexDate     SQLIndexType = "date"
    SQLIndexDateTime SQLIndexType = "datetime"
    SQLIndexBool     SQLIndexType = "bool"
)

type SQLJSONIndexSpec struct {
    CacheKey string
    Fields   []string // one field initially; ordered composite prefix later
    Type     SQLIndexType
}

trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{
    CacheKey: "users", Fields: []string{"created_at"}, Type: SQLIndexDateTime,
})
```

This is deliberately a Go/admin API first, not public SQL DDL. It prevents an
untrusted query from causing expensive index construction. Each index records
the source generation, declared type, collation/null ordering, and a row-id
mapping valid for that source snapshot.

## Common representation

Every candidate index maps an encoded field value to a **posting list** of row
ordinals. The source JSON rows remain owned by the query snapshot; the index
does not retain copied `SQLRow` maps. A query obtains candidate row ordinals,
then materializes only rows needed for predicates, projections, grouping, or
output.

Posting-list default:

- Store sorted row ordinals as delta-varints for sparse equality groups.
- Switch to a Roaring bitmap when a group is dense or has a wide ordinal span.
- Keep the decision per value group and benchmark its threshold.

This gives compact equality/join candidates while retaining fast set union,
intersection, and range-result construction.

## Candidate structures by logical type

### String equality and prefix

Use a dedicated **HAT-trie** from canonical, collation-aware string bytes to a
posting-list identifier. HAT-trie is a strong candidate when strings share
prefixes and when the index must support `LIKE 'prefix%'`, lexical ranges,
`ORDER BY name`, or `GROUP BY name`.

| Operation | Proposed path |
| --- | --- |
| `WHERE name = ?` / equality join | Exact HAT-trie lookup, then one posting list. |
| `WHERE name LIKE 'abc%'` | HAT-trie prefix traversal over matching value keys. |
| `WHERE name >= ? AND name < ?` | Lexical range traversal where SQL collation matches index collation. |
| `GROUP BY name` | Traverse distinct value keys in lexical order and stream one aggregate group per posting list. |
| `ORDER BY name [ASC|DESC] LIMIT n` | Ordered traversal, reverse traversal for DESC, stop after enough qualifying rows. |

For high-cardinality random strings with equality-only queries, a compact
open-addressed 64-bit hash table plus verified string storage may beat HAT-trie
on CPU and memory. It must be benchmarked against the HAT-trie candidate; the
hash table cannot directly provide prefix/range/order scans.

### Integer

Use a packed **sorted vector** of `(int64 value, row ordinal)` entries, sorted
by value then ordinal, plus an optional value-to-posting directory for equality
hot paths. Binary search finds a `WHERE` range; contiguous entries supply an
ordered scan. This is normally more space-efficient than a HAT-trie for dense
or random numeric keys because it has no per-byte trie nodes.

Encode signed values directly in memory. For a future byte-keyed ordered
structure, flip the sign bit and use big-endian bytes so lexical order equals
numeric order; do not index decimal text.

| Operation | Proposed path |
| --- | --- |
| Equality | Directory or equal-range binary search, then posting list. |
| `<`, `<=`, `>`, `>=`, `BETWEEN` | Two binary searches over the sorted vector. |
| `GROUP BY integer_field` | Stream equal-value runs; no hash aggregation table. |
| `ORDER BY integer_field` | Forward/reverse vector scan; `LIMIT` can stop early. |

### Date and datetime

Use the same sorted-vector design with normalized typed values:

- **date**: signed `int32` days from Unix epoch after strict `YYYY-MM-DD`
  parsing.
- **datetime**: signed `int64` UTC microseconds after strict RFC3339 parsing
  and timezone normalization.

Dates and datetimes must not be indexed as arbitrary strings: differing offsets
and formats can make lexical order disagree with chronological order. Typed
encoding gives correct range `WHERE`, chronological `ORDER BY`, time-bucket
`GROUP BY`, and efficient recent-first `LIMIT` scans.

### Boolean and low-cardinality fields

Use two Roaring bitmaps (true/false) or compact sorted ordinal lists. This is
more memory-efficient than a HAT-trie and makes `WHERE enabled = true` and
bitmap intersection with another index very fast. It is also useful as a
prefilter before an ordered scan.

## Query planner use

### WHERE and joins

- Equality: probe one posting list.
- Range: binary-search the sorted vector or traverse an ordered string index.
- Multiple predicates: intersect row-ordinal postings, starting with the most
  selective estimate; keep a residual predicate for exact SQL evaluation.
- Equality join: probe the indexed right-side value for each left-side value.

### GROUP BY

Use an index only when the `GROUP BY` expressions match an index field prefix
and the plan can consume rows in index order. Equal keys are contiguous, so the
executor can aggregate one run at a time and emit it before reading the next
run. This avoids the ordinary hash table for `COUNT`, `SUM`, `MIN`, `MAX`, and
similar aggregations.

`HAVING` still executes after each aggregate is finalized. A join, incompatible
expression, or an order-destroying operator falls back to the existing hash
grouping path. Composite `(team_id, created_at)` indexes are a later phase for
multi-column grouping prefixes.

### ORDER BY and LIMIT

Use an ordered index scan only when `ORDER BY` matches its leading fields,
direction, collation, and NULL ordering. ASC scans forward; DESC scans
backward. With a compatible `LIMIT`, the executor can stop after it has emitted
enough qualifying rows, avoiding a full materialize-and-sort step.

If a residual `WHERE` predicate is unselective, an ordered scan may inspect too
many rows. The planner must compare a cardinality estimate with the ordinary
scan-plus-sort alternative. Tie order must include row ordinal so pagination is
deterministic.

## Build and update strategy

The cache currently replaces a JSON document as one value. Without a stable
row identity, an arbitrary replacement cannot be updated safely in place: array
position is not a durable identity. Therefore the first implementation should
build a new typed index from one immutable source generation, validate it, then
atomically publish it. Queries keep using the prior generation during a build.

True incremental maintenance is a later feature and requires one of:

- a declared unique row-id field with per-row mutation APIs, or
- a durable row store that emits insert/update/delete deltas.

For mutation-heavy sources, use a small unsorted delta index plus a background
merge into the sorted base only after benchmarking. Do not add a general B-tree
or a second HAT-trie for numeric values before this workload proves necessary.

## Defaults, limits, and operations

| Setting | Sane default |
| --- | --- |
| Typed indexing | Off; an operator explicitly creates each index. |
| Type inference | Off; declaration prevents surprising date/string semantics. |
| Index refresh | Lazy build on first eligible query after a source-generation change; atomic publish. |
| Query fallback | Existing full scan remains correct when an index is absent, stale, building, or too expensive. |
| Resource limits | Per-index rows/bytes/build-duration limits with metrics and an explicit failure, never an unbounded allocation. |
| Persistence | Rebuildable derived state by default; do not include it in backups until recovery-time benchmarks justify it. |

Expose index state, source generation, type, entries, bytes, build duration,
last use, and planner counters. SQL `EXPLAIN` should show `TYPED INDEX SCAN`,
`INDEX GROUP`, or `INDEX ORDER`, plus the fallback reason when it declines one.

## Benchmark and acceptance plan

### Initial generic-index baseline

`make bench-sql-typed-index-baseline` measures 100,000 numeric JSON rows on an
AMD Ryzen 9 5950X. It compares the current generic field index to a resolver
that intentionally exposes only full scans:

| Query | Generic index | Full scan | Improvement |
| --- | ---: | ---: | ---: |
| selective numeric range | 635 us, 3.08 MB, 1,991 allocs | 315 ms, 232 MB, 3.40M allocs | 495x faster, 75.5x less allocated memory |
| `ORDER BY id DESC LIMIT 10` | 192 ms, 177 MB, 2.00M allocs | 472 ms, 247 MB, 3.80M allocs | 2.45x faster, 1.40x less allocated memory |

The generic index is therefore a strong correctness and range-query baseline.
Its ordered path still materializes and clones a large row set, making ordered
limit scans the first typed-index target. Re-run the benchmark on deployment
hardware before making acceptance decisions.

Measure the current generic index, a full scan, the HAT-trie string index, and
the numeric/date sorted-vector index. Use 100k, 1m, and 10m rows; uniform and
Zipf distributions; low/medium/high cardinality; prefix-heavy and random
strings; and narrow/wide numeric and datetime ranges.

For each candidate measure:

- Build wall time, allocations, retained heap, and bytes per indexed row.
- Equality, range, prefix, join, `GROUP BY`, `ORDER BY`, and `ORDER BY ...
  LIMIT` throughput plus p50/p99 latency.
- Cold rebuild, changed-source rebuild, and concurrent query behavior.
- Correctness against the existing SQL evaluator for NULLs, collation,
  duplicates, timezones, ascending/descending order, ties, and pagination.

Accept a candidate only if full SQL tests remain correct and it improves the
target workload without an unacceptable build, memory, write, backup, or
replication tradeoff. Keep the existing generic index as the fallback.

## Recommendation

Start with a typed string HAT-trie index for equality/prefix/order/group use
cases and a packed sorted-vector index for Integer, date, and datetime range,
order, and group use cases. Use compact posting lists for both. Do not use a
HAT-trie for numeric data by default; a sorted vector is the more likely
space-efficient and fast representation. This proposal is **not implemented**.
