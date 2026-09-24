# MZ-006 Antichain Timestamps

Materialize uses antichains for partially ordered timestamps. A scalar
frontier is insufficient when independent partitions advance at different
coordinates: neither timestamp may dominate the other, so both minimal
elements must remain available.

`hatPipeline.Antichain[T]` is an opt-in generic implementation. The caller
provides `func(T, T) bool` for the partial-order `less-or-equal` relation.
`Insert` drops a timestamp already covered by the frontier and removes older
elements dominated by the new timestamp. `Covers` answers whether any minimal
element is less than or equal to a query timestamp. `Snapshot` returns an
independent copy, and all methods are safe for concurrent callers.

```go
type Timestamp struct {
    Epoch  uint64
    Offset uint64
}

frontier, err := hatPipeline.NewAntichain(func(left, right Timestamp) bool {
    return left.Epoch <= right.Epoch && left.Offset <= right.Offset
})
changed, err := frontier.Insert(Timestamp{Epoch: 10, Offset: 4})
ready, err := frontier.Covers(Timestamp{Epoch: 10, Offset: 9})
```

The API is opt-in and does not alter existing source/frontier behavior. It
stores values by value in its own slice; pointer-containing timestamp values
still follow the caller's ownership rules. The comparator runs while the
frontier lock is held and must be deterministic and must not call back into
the same antichain.

## Benchmark

Command targets:

```text
make benchmark-mz006-antichain-baseline
make benchmark-mz006-antichain
```

Workload: insert 4,096 descending product timestamps, then query a timestamp
below every retained element. Five samples, Go benchmark `-benchmem`, AMD
Ryzen 9 5950X, Linux.

| Path | Median ns/op | B/op | allocs/op | Retained elements |
| --- | ---: | ---: | ---: | ---: |
| Naive append-only history: insert | 9,730 | 65,536 | 1 | 4,096 |
| `Antichain.Insert`: insert | 55,922 | 80 | 2 | 1 |
| Naive append-only history: negative `Covers` | 1,849 | 0 | 0 | 4,096 |
| `Antichain.Covers`: negative lookup | 6.390 | 0 | 0 | 1 |

The antichain retains 819.2x fewer measured bytes and makes this negative
frontier lookup about 289x faster. Maintaining dominance costs about 5.75x
more CPU for this synthetic insert loop; the append-only comparison does not
perform the required dominance work and is not semantically equivalent. This
is why the API remains opt-in: it is appropriate for long-lived frontiers and
bounded memory, while callers with extremely frequent updates should batch or
use a simpler scalar frontier when their timestamp order permits it.

Correctness and race coverage:

```text
make verify-mz006-antichain
```

This runs focused tests, the focused race test, and `go vet` for
`hat/hatPipeline`.
