# M038h Differential Distinct Small-Batch Fast Path

`DistinctDifferentialRows` now uses a fixed two-entry slice accumulator for
batches containing at most two updates. It preserves signed multiplicity,
enter/leave transitions, shallow row cloning, and negative/overflow errors.
Batches larger than two updates retain the existing map-based implementation,
so this optimization does not change the large-batch memory shape.

## Correctness

The focused tests cover:

- a row entering the distinct set;
- an enter followed by a leave;
- two independent keys;
- negative multiplicity rejection; and
- explicit fallback for larger batches.

Verification targets:

```text
make test-m038-distinct-fastpath
make race-m038-distinct-fastpath
make vet-m038-distinct-fastpath
make test-m038-distinct-package
```

## Measurement

Five `-benchmem` samples on Linux/amd64, AMD Ryzen 9 5950X. The baseline is
the previous map algorithm executed against the same fixture. Allocations are
unchanged because the result slice and returned row ownership still need to be
created; the optimization removes map work and reduces CPU time.

| Workload | Map baseline | Slice path | Improvement |
| --- | ---: | ---: | ---: |
| One row with payload | 235.4 ns/op, 384 B/op, 3 allocs/op | 218.0 ns/op, 384 B/op, 3 allocs/op | 1.08x faster |
| One row with nil payload | 58.66 ns/op, 48 B/op, 1 alloc/op | 39.57 ns/op, 48 B/op, 1 alloc/op | 1.48x faster |
| Two independent keys | 89.26 ns/op, 80 B/op, 1 alloc/op | 57.79 ns/op, 80 B/op, 1 alloc/op | 1.54x faster |

Raw samples:

```text
one-row map:       240.1, 236.4, 235.4, 233.1, 234.5 ns/op
one-row slice:     217.3, 218.2, 218.0, 211.7, 218.0 ns/op
nil-row map:        58.77, 58.41, 58.66, 58.62, 58.94 ns/op
nil-row slice:      39.57, 39.13, 39.69, 40.85, 39.41 ns/op
two-keys map:       88.64, 89.77, 89.30, 89.09, 89.26 ns/op
two-keys slice:     57.57, 57.91, 56.91, 57.79, 60.21 ns/op
```

The optimization is local to the reusable differential primitive. It does not
claim to accelerate a full SQL query or change planner admission.
