# T-U23 Typed Tuple Multikey Index

`hatDataStructure.NewTupleMultikeyIndex` provides a bounded secondary index
for multiple typed values belonging to one item. It reuses the existing
sorted posting-list representation, so lookup results are ordered by item ID
and singleton postings do not require a separate slice allocation.

## API

```go
index := hatDataStructure.NewTupleMultikeyIndex(
    hatDataStructure.TupleMultikeyIndexOptions{
        MaxKeysPerItem: 8,
        MaxItems:       1_000_000,
    },
)

err := index.Set(42, []hatDataStructure.TupleFieldValue{
    hatDataStructure.TupleString("red"),
    hatDataStructure.TupleInt64(7),
    hatDataStructure.TupleDate(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)),
})
ids, err := index.Lookup(hatDataStructure.TupleInt64(7), nil)
```

`Set` replaces all values for an item. Repeated values are deduplicated,
updates and deletes remove old postings, and invalid values or bounds are
rejected before any mutation. `Lookup` accepts a reusable destination slice.
`TupleNull()` is a valid indexed key. Different types such as string `"7"`,
signed integer `7`, and unsigned integer `7` never share a posting.

Dates use the existing tuple day normalization. Timestamps use their UTC
nanosecond instant. Floating-point values use their IEEE bit representation,
so this index provides exact typed identity rather than SQL coercion.

## Scope

This is the maintained typed multikey data structure and its safety bounds.
It does not automatically discover nested arrays in a space, maintain a
schema catalog, or add a SQL planner rule. Callers still extract the values
to index and choose when to update the index.

## Benchmark

Command:

```text
make verify-tu23
```

The benchmark uses 100,000 items with two values per item and five samples on
Linux amd64, AMD Ryzen 9 5950X. The typed string and string-index cases return
1,000 IDs; the typed int64 case returns 3,125 IDs, so their raw times are not
directly comparable without accounting for result-copy work.

| Operation | Median | Memory | Allocations | Comparison |
| --- | ---: | ---: | ---: | --- |
| Existing string index lookup | 88.95 ns/op | 0 B/op | 0 | Baseline |
| Typed string lookup | 146.3 ns/op | 16 B/op | 1 | 1.65x time, encoding allocation |
| Typed int64 lookup | 418.7 ns/op | 0 B/op | 0 | Allocation-free; returns 3.125x more IDs |
| Map-of-sets lookup | 9,459 ns/op | 0 B/op | 0 | 106.3x slower than string baseline |
| Linear scan | 121,904 ns/op | 0 B/op | 0 | 1,370x slower than string baseline |
| Typed index build | 4.078 ms/op | 2.437 MB/op | 31,347 | 1.21x time vs string index |
| String index build | 3.360 ms/op | 2.197 MB/op | 21,347 | Baseline |

The typed string overhead is the cost of constructing a collision-free type
tag and length-prefixed key for the existing string posting map. Fixed-width
typed values avoid that allocation. The feature is therefore useful when
typed identity or numeric/date lookup is required; callers that already have
canonical strings should continue using `StringMultikeyIndex`.
