# Single-Replica Read Fast Path

`ExecuteParallelReplicaRead` now has a synchronous path when the caller
provides exactly one eligible replica. The callback runs on the caller's
goroutine after the same input, context, and cancellation validation as the
general path. The result still contains one completed attempt, and callback
errors still wrap `ErrParallelReplicaReadFailed`.

The multi-replica path is unchanged: it retains concurrent launches,
hedging, cancellation, attempt ordering, duplicate detection, and the
existing result contract. This optimization is therefore useful for
single-node deployments and queries whose routing policy leaves one healthy
candidate without adding another configuration flag.

## Measurement

The benchmark was run on Linux/amd64 with an AMD Ryzen 9 5950X. Each result
below is the median of three `go test -benchmem -count=3` samples. The
baseline was captured before the branch and the final values were captured
after it. The three-node row is a control proving that the general path was
not changed.

| Workload | Before | After | Result | Before memory | After memory | Before allocations | After allocations |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| One node, success | 886.1 ns/op | 47.57 ns/op | 18.62x faster | 448 B/op | 48 B/op | 8 | 1 |
| Three nodes, success control | 1,900 ns/op | 1,903 ns/op | 1.00x, 0.2% slower | 880 B/op | 880 B/op | 10 | 10 |
| One node, failure | 1,323 ns/op | 189.2 ns/op | 6.99x faster | 545 B/op | 144 B/op | 10 | 3 |

Raw baseline samples:

```text
single_success: 870.6 886.1 890.8 ns/op; 448 B/op; 8 allocs/op
three_success_control: 1859 1989 1900 ns/op; 880 B/op; 10 allocs/op
single_failure: 1301 1323 1423 ns/op; 545 B/op; 10 allocs/op
```

Raw final samples:

```text
single_success: 49.77 47.57 47.53 ns/op; 48 B/op; 1 alloc/op
three_success_control: 1872 1916 1903 ns/op; 880 B/op; 10 allocs/op
single_failure: 186.4 189.2 189.3 ns/op; 144 B/op; 3 allocs/op
```

Commands:

```text
make test-parallel-read-fastpath-c208
make benchmark-parallel-read-fastpath-c208
make verify-parallel-read-fastpath-c208
```

The verification target runs the complete `hat/hatReplication` package under
normal tests, the race detector, and `go vet`.
