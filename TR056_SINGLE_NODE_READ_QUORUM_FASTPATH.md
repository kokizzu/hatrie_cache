# Single-Node Read Quorum Fast Path

`ExecuteReadQuorum` now executes a valid one-node quorum directly when
`len(nodes) == 1` and `required == 1`. The callback runs synchronously and
the result keeps the existing decision, attempt, value, and error contract.
Invalid inputs, nil or canceled contexts, and every multi-node quorum remain
on the existing validation and concurrent execution path.

This is useful when a caller keeps the quorum API while routing a request to
one currently eligible replica. The direct path avoids the normalized-node
map, response slice, goroutine, `WaitGroup`, and value-group construction.
It does not change equality semantics because a single successful response
never compares two values.

## Measurement

The benchmark was run on Linux/amd64 with an AMD Ryzen 9 5950X. Each result
below is the median of three `go test -benchmem -count=3` samples. The
three-node row is a control for the unchanged general quorum path.

| Workload | Before | After | Result | Before memory | After memory | Before allocations | After allocations |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| One node, success | 679.3 ns/op | 62.37 ns/op | 10.90x faster | 320 B/op | 48 B/op | 6 | 1 |
| Three nodes, success control | 1,520 ns/op | 1,529 ns/op | 1.00x, 0.6% slower | 864 B/op | 864 B/op | 12 | 12 |
| One node, failure | 1,041 ns/op | 193.0 ns/op | 5.39x faster | 432 B/op | 160 B/op | 8 | 3 |

Raw baseline samples:

```text
single_success: 699.5 677.7 679.3 ns/op; 320 B/op; 6 allocs/op
three_success_control: 1516 1525 1520 ns/op; 864 B/op; 12 allocs/op
single_failure: 1019 1041 1050 ns/op; 432 B/op; 8 allocs/op
```

Raw final samples:

```text
single_success: 64.13 61.06 62.37 ns/op; 48 B/op; 1 alloc/op
three_success_control: 1539 1529 1516 ns/op; 864 B/op; 12 allocs/op
single_failure: 193.0 190.2 197.7 ns/op; 160 B/op; 3 allocs/op
```

Commands:

```text
make test-read-quorum-fastpath-c209
make benchmark-read-quorum-fastpath-c209
make verify-read-quorum-fastpath-c209
```
