# C237 Projection Explain And Estimated I/O Cost

## Scope

C237 asks for explain output that makes projection selection and estimated
I/O visible. Projection selection was already present in the base branch: an
exact query can use a fresh projection only when its source-version contract
still matches. This change fills the remaining explain gap by adding an
explicit `estimated_io_cost` field to costed explain output.

The feature is observability-only. It does not change normal query execution,
projection freshness checks, or the default explain shape.

## Output

`EXPLAIN COST` and `EXPLAIN PIPELINE COST` now expose these cost columns:

```text
estimated_cost
estimated_io_cost
estimated_memory_bytes
```

The JSON field is `estimated_io_cost`. `SQLExplainCostOptions.IOCostPerRow`
controls its unit; zero or a negative value selects the default of `1`.
The estimate is heuristic and comparable within a plan. It is not a byte
count, a storage-engine read count, or a wall-clock prediction.

Only source-scan-like nodes receive an I/O pointer today: `SCAN`, `INDEX SCAN`,
and other node names containing `SCAN`. Compute operators such as `FILTER`,
`JOIN`, `AGGREGATE`, `SORT`, `LIMIT BY`, and `PROJECTION HIT` leave the field
not applicable. This avoids claiming that CPU or in-memory work is storage
I/O and avoids an allocation for those steps.

The field is copied by explain-dataflow and result-cache plan clones, so a
caller cannot mutate a cached or returned plan through a shared estimate
pointer.

## Benchmark

Commands:

```sh
make benchmark-c237-projection-explain
```

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X. The before values
were captured immediately before this change; after values are the C237
implementation. Medians are used because single benchmark samples are noisy.

| Benchmark | Before median ns/op | After median ns/op | CPU ratio after/before | Before B/op | After B/op | Memory ratio | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Regular explain | 8,758 | 8,590 | 0.981x | 10,504 | 11,018 | 1.049x | 58 | 58 |
| Costed explain | 9,349 | 9,467 | 1.013x | 11,483 | 12,133 | 1.057x | 66 | 67 |

Raw samples:

```text
Regular before: 8648, 8758, 8606, 8775, 8926 ns/op; 10504 B/op; 58 allocs/op
Regular after:  8434, 8575, 8671, 8590, 8598 ns/op; 11018 B/op; 58 allocs/op
Costed before:  9671, 9342, 9349, 9295, 9400 ns/op; 11483 B/op; 66 allocs/op
Costed after:   9539, 9305, 9467, 9527, 9344 ns/op; 12133 B/op; 67 allocs/op
```

The cost is limited to `EXPLAIN COST`: regular explain has the same allocation
count and its CPU result was within normal run noise. Costed explain adds one
pointer allocation for the scan estimate in this workload, about 1.3% CPU and
5.7% heap by the recorded medians. This is an intentional small diagnostic
cost, not a runtime-query optimization claim.

The existing projection catalog path remained unchanged and retained its
measured shape in the same benchmark:

```text
direct query:    7545 ns/op; 8864 B/op; 47 allocs/op median
projection hit:  3408 ns/op; 4560 B/op; 17 allocs/op median
```

## Verification

Focused correctness and safety checks:

```sh
make format-c237-projection-explain
make test-c237-projection-explain
make race-c237-projection-explain
make vet-c237-projection-explain
```

The repository-wide `go test ./...` pass was started with an isolated cache,
but was stopped after it reported unrelated existing failures in `hat/hatCache`
for exact index-estimate expectations and then remained silent in a later
package. The focused C237, projection-selection, pipeline, and result-cache
checks passed.
