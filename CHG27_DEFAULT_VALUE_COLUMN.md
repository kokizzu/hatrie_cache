# ClickHouse-Inspired Default-Value Suppression

`hatDataStructure.DefaultValueColumn[T]` stores a typed column compactly when
most rows equal one configured default value. Sparse storage uses one bit per
row and a packed value array for non-default rows. The column adaptively
switches to dense storage at the configured density threshold, or when the
retained sparse backing would exceed the dense estimate.

The default constructor uses a `0.75` density threshold and a 64-row minimum
before adaptive conversion. `NewDefaultValueColumnWithOptions` allows callers
to select the default value, capacity hint, and threshold. `Set` is atomic
with respect to the column's in-memory state; invalid indexes do not change
the column. The type is intentionally not synchronized for concurrent
mutation.

## Benchmark

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X, using
`-benchtime=300ms -benchmem`, with 4,096 `int64` rows and one non-default per
64 rows:

| Workload | Dense `[]int64` | DefaultValueColumn | Result |
| --- | ---: | ---: | --- |
| Append | 4,719 ns/op, 32,792 B/op, 2 allocs/op | 11,591 ns/op, 5,328 B/op, 4 allocs/op | 2.46x slower CPU, 6.16x less benchmark allocation |
| Random lookup | 0.718 ns/op, 0 B/op, 0 allocs/op | 2.932 ns/op, 0 B/op, 0 allocs/op | 4.09x slower CPU, no heap cost |
| Retained backing estimate | 32,768 B | 5,128 B | 6.39x lower |

Retained backing estimates across evenly distributed densities were:

| Non-default density | Retained bytes | Versus dense |
| ---: | ---: | ---: |
| 1% | 5,128 | 6.39x lower |
| 25% | 11,272 | 2.91x lower |
| 50% | 21,512 | 1.52x lower |
| 75% | 32,768 | dense fallback |
| 100% | 32,768 | dense fallback |

The feature is an opt-in storage choice, not a replacement for hot dense
columns: it trades append and lookup CPU for lower retained memory on sparse
data. `StorageBytes` is a diagnostic estimate of backing arrays and excludes
slice/object headers and memory referenced by element values.

Run the focused tests and benchmark with:

```sh
make test-chg27-default-column
make benchmark-chg27-default-column
```
