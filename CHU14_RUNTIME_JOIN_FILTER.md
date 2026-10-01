# CH-U14: Streaming Runtime Join Filter

This adopts a narrow ClickHouse-style runtime-filter optimization for direct
inner equality joins. It is opt-in through
`SQLQueryOptions.RuntimeJoinBloomFilter` and remains disabled by default.

When both sides are direct `CACHE` or `EXTERNAL` sources with streaming
resolver support, the executor streams the smaller right-side input into an
exact hash bucket map plus a 1% false-positive Bloom filter. It then streams the
left side and skips keys that cannot match. Bloom hits still use the exact hash
map, so false positives can add work but cannot change SQL results.

The established materialized executor remains the fallback for joins with
filters, aggregates, ordering, windows, CTEs, unions, typed source fields,
unsupported source kinds, or no streaming resolver. Existing equality indexes
also retain priority over this filter. No setting is changed automatically.

## Verification

Test-first evidence was captured on the parent branch: the external-source
regression initially failed because the stream callback was never called. The
same test passes after the eligibility gate was widened. The focused suite is:

```sh
make format-chg06-external-runtime-filter
make test-chg06-external-runtime-filter
```

## Measurement

Five samples with `-benchtime=100ms` on Linux/amd64, AMD Ryzen 9 5950X, using a
100,000-row left source and a 512-row right source with 512 matching keys:

| Path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Materialized fallback, same feature branch | 30,006,363 | 48,934,912 | 305,691 | baseline |
| Streaming runtime filter | 9,635,467 | 3,466,262 | 107,235 | 3.12x faster, 14.13x lower bytes, 2.84x fewer allocations |

The parent-branch materialized-only control was 39,253,903 ns/op,
48,936,824 B/op, and 305,696 allocs/op in a separate five-sample run. The
paired same-branch comparison is the primary result because it controls for
run-to-run host noise.

The tradeoff is the right-side hash map and Bloom construction cost. It is
beneficial when the probe side is much larger and the join is selective; for
small or balanced joins the option can cost more CPU. Callers should enable it
only for workloads where the measured input shape justifies the extra setup.
