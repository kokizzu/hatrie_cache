# Delay Queue Ready-Pop Fast Path

`T089b` fuses `DelayQueue.PopReady`'s readiness check and root removal. The
previous implementation called `Peek`, checked the deadline, and then called
`Pop`, so the root was copied and validated twice. The optimized path checks the
root in place, removes it once, performs the same 4-ary `siftDown`, and clears
the removed tail value so generic values do not remain referenced.

The zero value, deadline ordering, stable equal-deadline ordering, future-item
behavior, heap ordering, and public API are unchanged. `VisibilityQueue.Lease`
uses this path for ready work.

## Verification

```text
make test-delay-queue-c215
make verify-delay-queue-c215
```

The verification target runs the full `hat/hatDataStructure` package, the race
detector, and `go vet`.

## Measurement

Command: `make benchmark-delay-queue-c215`.

Linux/amd64, AMD Ryzen 9 5950X, five samples per case, `-benchmem`:

| Workload | Before median | After median | Improvement | Before memory | After memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| One ready item: `PopReady` | 22.54 ns/op | 10.64 ns/op | **2.12x faster** | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |
| `VisibilityQueue` lease + ack | 104.9 ns/op | 91.20 ns/op | **1.15x faster** | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |
| 256-item resident lease + ack | 530.45 ns/op | 518.3 ns/op | **1.02x faster** | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op |

Raw isolated `PopReady` samples were `22.43, 22.60, 22.40, 22.54, 24.39
ns/op` before and `11.78, 11.39, 10.64, 10.34, 10.45 ns/op` after. The
resident comparison used ten samples per side; the before samples were
`519.8, 526.3, 552.2, 534.6, 538.3, 557.1, 509.2, 536.6, 508.2, 508.4
ns/op`, and the after samples were `576.0, 503.1, 506.0, 523.4, 526.1,
543.5, 506.1, 513.2, 551.6, 512.8 ns/op`.
