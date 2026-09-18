# MZ-009 Timestamp-Domain Leases

This adds an opt-in lease registry for independent source timestamp domains.
Only the current owner and fencing epoch may advance a domain. Expiry and
release preserve the domain watermark, so a replacement owner cannot reuse an
older timestamp sequence.

```go
registry, err := hatPipeline.NewTimestampDomainLeaseRegistry(
	hatPipeline.TimestampDomainLeaseOptions{},
)
if err != nil {
	panic(err)
}

lease, err := registry.Acquire("orders", "source-a", time.Minute)
if err != nil {
	panic(err)
}
timestamp, err := registry.Next(lease)
if err != nil {
	panic(err)
}
_ = timestamp
```

`Observe` accepts an externally assigned timestamp when it is monotonic;
`Renew` extends an active lease without changing its epoch; `Release` and
`Forget` support lifecycle cleanup. Domain names, owners, domain count, and
lease TTLs are bounded. Snapshots are sorted by domain and detached from
internal state.

This does not change existing source timestamps or query behavior. Callers
must explicitly create a registry and use its lease token.

## Measurement

Run:

```text
make benchmark-mz009-timestamp-domain-lease
```

Five samples used an injected fixed clock on an AMD Ryzen 9 5950X. The
mutex-protected counter is the relevant lower-bound comparison; the raw
unsynchronized counter is included only to show the synchronization gap.

| Path | Samples (ns/op) | Median ns/op | B/op | Allocs/op | Relative to mutex |
| --- | --- | ---: | ---: | ---: | ---: |
| Raw counter | 0.2723, 0.2773, 0.2765, 0.2535, 0.2902 | 0.2765 | 0 | 0 | 0.07x |
| Mutex counter | 3.972, 3.906, 3.810, 3.753, 3.974 | 3.906 | 0 | 0 | 1.00x |
| Fenced lease `Next` | 27.80, 28.40, 28.10, 27.47, 27.74 | 27.80 | 0 | 0 | 7.12x |

The lease path costs about 23.9 ns/op over a plain mutex counter but retains
ownership, expiry, fencing, and monotonic-watermark guarantees with zero heap
allocations. The benchmark injects the clock; production callers should treat
wall-clock lookup as additional cost.
