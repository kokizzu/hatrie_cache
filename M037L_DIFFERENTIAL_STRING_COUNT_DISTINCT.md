# M037l Differential String COUNT(DISTINCT)

`GroupCountDistinctStringDifferentialRows` extends the existing Materialize-style
signed differential grouped aggregate path to string values. It retains the
exact multiplicity of every `(group, value)` pair, so duplicate insertions do
not change the visible distinct count and retractions are rejected or applied
atomically when their multiplicity is invalid.

Empty strings are valid values. Callback failures, negative group counts,
negative value multiplicities, and checked counter overflow return no partial
output. The helper preserves input order and update timestamps and does not
mutate input rows.

The path is an importable opt-in primitive. Existing SQL planning and the
existing int64 differential path are unchanged.

## Measurement

Command:

```text
make benchmark-differential-group-min-max
```

Fixture: 1,024 string insertions across 128 groups and 192 values, followed by
matching retractions in reverse order. Five samples, one CPU, Linux/amd64, AMD
Ryzen 9 5950X.

| implementation | median ns/op | B/op | allocs/op | result vs rebuild |
| --- | ---: | ---: | ---: | ---: |
| naive per-update rebuild | 22,133,892 | 612,482 | 2,573 | 1.00x |
| incremental string multiplicity | 408,869 | 647,912 | 2,828 | 54.13x CPU, 1.06x bytes, 1.10x allocs |

The large CPU win comes from retaining multiplicities instead of rescanning the
entire update prefix for every change. The measured memory and allocation cost
is the intentional tradeoff for exact string membership state; the feature is
not enabled implicitly for existing callers.

Raw samples:

```text
BenchmarkM037lDifferentialStringCountDistinct/naive_rebuild   22133892 ns/op 612482 B/op 2573 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/naive_rebuild   22118295 ns/op 612481 B/op 2573 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/naive_rebuild   22465227 ns/op 612483 B/op 2573 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/naive_rebuild   22106556 ns/op 612483 B/op 2573 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/naive_rebuild   22497107 ns/op 612482 B/op 2573 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/incremental       401602 ns/op 647912 B/op 2828 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/incremental       398132 ns/op 647912 B/op 2828 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/incremental       429031 ns/op 647912 B/op 2828 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/incremental       426350 ns/op 647912 B/op 2828 allocs/op
BenchmarkM037lDifferentialStringCountDistinct/incremental       408869 ns/op 647912 B/op 2828 allocs/op
```
