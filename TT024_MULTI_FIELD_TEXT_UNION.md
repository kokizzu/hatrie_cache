# TT-024 Multi-Field Text Union

`HatTrie` now implements `TextProximityMultiFieldUnionIndexedSourceResolver`.
The SQL planner can therefore use positional text indexes for an `OR` whose
phrase or proximity predicates reference different fields, including mixed
branches such as `(text predicate AND filter) OR (text predicate AND filter)`.

The resolver returns conservative candidates in source order. The normal SQL
executor still evaluates the complete Boolean expression, so filters and
three-valued SQL semantics remain authoritative. If any referenced field is
not indexed, the resolver is unavailable and the existing scan path is kept.

## Benchmark

Command:

```text
make benchmark-tt024-cache-cross-field
```

Workload: 50,000 JSON rows, positional indexes on `title` and `body`, two
selective text branches, three benchmark samples per case, `-benchmem`.

Raw pre-change baseline, before the concrete multi-field resolver existed:

```text
BenchmarkTT024CrossFieldHatTrieFullScan-32  7  147775029 ns/op  93767435 B/op  1550618 allocs/op
BenchmarkTT024CrossFieldHatTrieFullScan-32  7  146441042 ns/op  93765830 B/op  1550615 allocs/op
BenchmarkTT024CrossFieldHatTrieFullScan-32  7  145840758 ns/op  93765848 B/op  1550615 allocs/op
```

Raw post-change result:

```text
BenchmarkTT024CrossFieldHatTrieFullScan-32      7  145128149 ns/op  93767405 B/op  1550617 allocs/op
BenchmarkTT024CrossFieldHatTrieFullScan-32      7  152309997 ns/op  93765866 B/op  1550616 allocs/op
BenchmarkTT024CrossFieldHatTrieFullScan-32      7  154578711 ns/op  93765876 B/op  1550617 allocs/op
BenchmarkTT024CrossFieldHatTrieIndexedUnion-32  1281  942141 ns/op  819202 B/op  7098 allocs/op
BenchmarkTT024CrossFieldHatTrieIndexedUnion-32  1291  923647 ns/op  818742 B/op  7090 allocs/op
BenchmarkTT024CrossFieldHatTrieIndexedUnion-32  1251  865048 ns/op  820623 B/op  7124 allocs/op
```

Using medians, the indexed path is approximately **159x faster**, uses **115x
less allocated memory**, and performs **219x fewer allocations** than the
pre-change full-scan baseline. The post-change full-scan control remains in the
same range, so the improvement comes from candidate pruning rather than a
changed query workload. Results are workload-specific; low-selectivity text or
missing indexes correctly falls back to the scan path.

## Verification

```text
make format-tt024-cache-cross-field
make test-tt024-cache-cross-field
make test-tt024-cache-cross-field-package
make test-tt024-sql-cross-field
make race-tt024-cache-cross-field
make vet-tt024-cache-cross-field
```
