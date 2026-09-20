# Adaptive Low-Cardinality String Storage

This is a partial ClickHouse-inspired adoption of adaptive low-cardinality
encoding for `hatSql.TypedTable`.

## Usage

Set `DictionaryAdaptive` on a string column when the workload may contain
repeated values but its cardinality is not known in advance:

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "events",
	Columns: []hatSql.TypedTableColumn{
		{Name: "region", Kind: hatSql.TypedTableString, DictionaryAdaptive: true},
		{Name: "points", Kind: hatSql.TypedTableInt64},
	},
})
```

The option is off by default. `DictionaryEncoded: true` takes precedence when
both options are set.

## Admission Rule

The column starts with the existing plain `[]string` representation. During
the first 256 inserted rows, it keeps a bounded distinct-value slice. If the
sample has at most 32 distinct non-NULL values, the column is converted to the
existing dictionary representation: each row stores a `uint32` code and the
distinct values are retained once. If the sample exceeds 32 values, the probe
is discarded and the column stays plain for its lifetime.

The admission decision is deliberately one-shot and conservative. After an
adaptive column has been promoted, its active dictionary cardinality is still
checked on inserts and updates. If it exceeds the same 32-value bound, the
column is demoted once to plain `[]string` storage, the dictionary codes and
indexes are released, and the column stays plain for the rest of its lifetime.
This bounds high-churn dictionary retention without background work or
representation oscillation. The demotion conversion is paid once at the
transition; `DictionaryEncoded` columns never demote.

Updates before admission add observed values to the bounded probe; they can
only make promotion less likely. Updates after demotion use the plain string
path and do not recreate the dictionary.

NULL values remain NULL and do not become dictionary entries. `DictionaryEncoded`
columns with only NULL values are also readable without indexing an empty
dictionary.

## Benchmark

Command:

```text
make benchmark-chu16-adaptive-dictionary
```

The benchmark creates a table and upserts 512 rows per iteration on Linux
amd64, AMD Ryzen 9 5950X, with five samples and `-benchmem`. The benchmark
reports cumulative allocations (`B/op`), not retained heap after garbage
collection. Repeated inputs use `team-a`; unique inputs use `value-N`.

| Layout | Median ns/op | B/op | allocs/op | Relative CPU vs matching plain | Relative B/op vs matching plain |
|---|---:|---:|---:|---:|---:|
| Plain repeated | 164,845 | 241,585 | 1,083 | 1.00x | 1.00x |
| Adaptive repeated | 168,034 | 235,513 | 1,090 | 1.02x | 0.97x |
| Plain unique | 166,011 | 241,585 | 1,083 | 1.00x | 1.00x |
| Adaptive unique | 163,393 | 243,777 | 1,091 | 0.98x | 1.01x |
| Static dictionary repeated | 170,459 | 227,152 | 1,086 | 1.03x | 0.94x |
| Static dictionary unique | 219,635 | 304,169 | 1,118 | 1.32x | 1.26x |

Raw `ns/op` samples, in command order:

```text
plain-repeated:       169049 161700 167332 164845 161204
plain-unique:         167385 165035 166011 165529 166950
dictionary-repeated:  168640 171035 172627 170459 168088
dictionary-unique:    225222 219635 215296 218189 222349
adaptive-repeated:    163588 170980 172922 167578 168034
adaptive-unique:      162729 163393 167420 166593 161480
```

The structural memory win is larger when incoming repeated strings have
independent backing arrays: promoted columns retain one dictionary value and
one 4-byte code per row instead of one string slot and backing value per row.
The benchmark's precomputed inputs intentionally keep string backing arrays
alive, so its `B/op` difference is not a retained-heap measurement.

### Runtime Demotion

Command:

```text
make benchmark-chu16-runtime-demotion
```

This compares the origin baseline with the candidate implementation using
five benchmark samples. The baseline is reconstructed from `origin/master`;
the candidate is the opt-in runtime-demotion path. `B/op` is cumulative Go
allocation reported by the benchmark, not retained RSS. The representation
counts are custom benchmark counters: dictionary values retained versus plain
string slots after the workload.

| Workload | Baseline median ns/op | Candidate median ns/op | Relative CPU | Baseline B/op | Candidate B/op | Baseline allocs/op | Candidate allocs/op | Representation change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 256 repeated then 33 unique | 166,289 | 179,379 | 1.08x slower | 183,466 | 197,804 | 660 | 662 | 34 dictionary values -> 0; 0 -> 289 string slots |
| 256 repeated then 4,097 unique | 3,246,057 | 2,465,946 | 1.32x faster | 3,391,844 | 2,817,844 | 8,899 | 8,844 | 4,097 dictionary values -> 0; 0 -> 4,352 string slots |

Raw samples, baseline then candidate:

```text
promotion-then-churn baseline: 152301 154689 166289 171774 174910 ns/op; 183466-183467 B/op; 660 allocs/op
promotion-then-churn candidate: 176183 172175 179379 181227 183567 ns/op; 197802-197804 B/op; 662 allocs/op
long-churn baseline:            3200583 3375336 3263282 3246057 3031734 ns/op; 3391843-3391846 B/op; 8899 allocs/op
long-churn candidate:           2607802 2495798 2410311 2347509 2465946 ns/op; 2817842-2817853 B/op; 8844 allocs/op
```

The short transition costs about 7.9% CPU, 7.8% cumulative allocation bytes,
and two allocations because demotion copies the existing values once. The
long-churn workload is about 32% faster, uses about 17% fewer cumulative
allocation bytes, and retains no high-cardinality dictionary values. This is
an opt-in tradeoff for workloads where cardinality can grow after admission;
the static dictionary mode and the default plain mode are unchanged. The
table does not claim exact process-heap savings: `TypedTable.MemoryUsage` is a
logical admission metric and intentionally excludes Go map capacity,
allocator fragmentation, and other process overhead.

## Scope

This covers adaptive admission for typed-table string columns. It does not
change the default representation, automatically select columns across a
schema, re-promote a demoted column, or replace the separate immutable
low-cardinality column API. Callers opt in per column with
`DictionaryAdaptive: true`.
