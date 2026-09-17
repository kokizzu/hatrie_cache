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

The decision is deliberately one-shot and conservative. It does not run an
ongoing eviction or demotion policy, so high-churn workloads do not pay a
background re-encoding cost. Updates before admission add observed values to
the bounded probe; they can only make promotion less likely.

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

## Scope

This covers adaptive admission for typed-table string columns. It does not
change the default representation, automatically alter schemas, add runtime
demotion, or replace the separate immutable low-cardinality column API.
