# Connection Pool Idle Fast Path

`T095b` is a small Tarantool Net.box-style pool optimization. `Acquire` already
uses a nonblocking receive from the typed idle channel before entering its
wait/select path. The old helper acquired `ConnectionPool.mu` around that
receive even though channel ownership is synchronized by the channel itself.

The optimized helper performs only the nonblocking receive. `Acquire` keeps its
closed-channel check before the receive and repeats it after receiving an idle
connection. If `Close` wins the race, the existing discard path closes the
connection, decrements the open count, and returns the slot. No public API,
pool limit, lifecycle behavior, or error contract changes.

## Verification

Focused tests:

```text
make test-connection-pool-c212
make verify-connection-pool-c212
```

The latter runs normal tests, the race detector, and `go vet` for
`hat/hatReplication`.

## Measurement

Command:

```text
make benchmark-connection-pool-c212
```

Linux/amd64, AMD Ryzen 9 5950X, five samples per case, `-benchmem`:

| Workload | Before median | After median | Improvement | Before memory | After memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Idle acquire + release | 49.28 ns/op | 41.09 ns/op | **1.20x faster** | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |
| Direct dial + close control | 3.761 ns/op | 3.584 ns/op | control noise | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |

Raw idle samples:

```text
Before: 46.19, 50.45, 49.28, 51.37, 48.46 ns/op
After:  42.76, 39.50, 41.55, 41.09, 40.55 ns/op
```

The benchmark measures local pool coordination only; it does not claim a
network or TLS speedup. The change was kept because it improves the hot reuse
path without increasing memory, allocation count, or API complexity.
