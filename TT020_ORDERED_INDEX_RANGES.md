# Generic Ordered Index Ranges

`hatDataStructure.OrderedIndex.Range(start, end)` returns an allocation-free
iterator over entries whose keys compare inclusively between `start` and
`end`. Both bounds are located with binary search, and iteration uses the
existing immutable entry backing slice.

For a composite key, use the smallest and largest suffix values to scan one
prefix without visiting neighboring prefixes:

```go
iterator, ok := index.Range(
    compositeKey{Region: "eu", Stamp: minStamp},
    compositeKey{Region: "eu", Stamp: maxStamp},
)
if !ok {
    return
}
for {
    entry, next, err := iterator.Next()
    if err != nil {
        return err
    }
    if !next {
        break
    }
    use(entry)
}
```

The range is a read-only snapshot. A concurrent mutation follows the existing
ordered-index iterator contract and invalidates the iterator. Inverted or
empty bounds return no iterator. The API adds no per-entry allocation and no
new retained backing storage.

See the raw comparison in [BENCHMARK.md](BENCHMARK.md#tt-020-generic-multi-part-ordered-ranges).
