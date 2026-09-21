# CH-015 Filesystem Cache Admission

`RemotePartCache` can now use an opt-in frequency gate before retaining a
loaded immutable remote part in RAM. This is useful when a filesystem or
object-store-backed scan contains many cold parts but only a small hot set.
The loader and the existing byte, entry, priority, and pinning policies remain
caller-owned.

## Configuration

`RemotePartCacheOptions.MinAccesses` controls admission:

- `0` keeps the legacy eager behavior. Every successful part load that fits is
  retained immediately.
- A positive value retains a part only after that many successful accesses.
  `2` is a practical starting point for workloads with repeated hot reads and
  large one-shot scans.

```go
cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
	MaxBytes:    512 << 20,
	MaxEntries:  4096,
	MinAccesses: 2,
})
if err != nil {
	return err
}
```

The cache remains disabled unless `MaxBytes` is positive. Admission does not
change the returned bytes: a cold caller still receives the loaded part, but a
part below the threshold is counted as uncached and is not retained. Concurrent
callers that join the same single-flight load count toward the threshold, so a
hot concurrent read does not require duplicate loads before admission.

Candidate metadata is bounded from the configured entry limit and is pruned
without retaining part payloads. Existing priority eviction and pinned-entry
protection still apply after a part qualifies. If a qualifying part cannot fit
or all possible victims are pinned, it remains uncached.

`RemotePartCacheStats.Admissions` counts successful loads retained by the
cache. `Uncached` exposes loads returned to callers without retention.

## Tradeoff

Frequency admission is deliberately opt-in because it can add a read and
decode for the second access to a cold key. It is most useful when avoiding
cache pollution matters more than keeping every first-read part warm. Leave
`MinAccesses` at `0` for workloads whose working set is already well behaved or
where first-read reuse is common.

## Measurement

The matched benchmark was run with `make benchmark-c245` on Linux/amd64 with
an AMD Ryzen 9 5950X and five samples per benchmark. The cold benchmark uses a
64 KiB part and a one-byte cache budget so the part cannot be retained; this
isolates admission overhead. The retained-memory test uses 32 unique 3-byte
parts and one access per part.

| Path | Median ns/op | Median B/op | Median allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Existing default warm hit after C245 | 64.86 | 0 | 0 | No allocation regression versus the pre-change 65.57 ns/op baseline |
| `MinAccesses: 2` warm hit | 61.39 | 0 | 0 | Same allocation-free hit path; small run-to-run CPU variation |
| Cold eager admission | 9,263 | 65,712 | 3 | Control |
| Cold `MinAccesses: 2` admission | 10,143 | 65,713 | 3 | 1.095x CPU cost and one additional byte in this uncached path |
| 32 one-hit parts retained | n/a | n/a | n/a | Eager: 32 entries / 96 data bytes; frequency: 0 entries / 0 data bytes |

The frequency setting therefore has a measurable cold-path cost, but it avoids
retaining the data bytes of one-hit parts. The default remains unchanged, so
applications do not pay this cost unless they enable the setting.

### Raw samples

```text
Pre-C245 BenchmarkC243RemotePartCacheHit (five runs):
65.54 ns/op  0 B/op  0 allocs/op
65.92 ns/op  0 B/op  0 allocs/op
65.57 ns/op  0 B/op  0 allocs/op
65.56 ns/op  0 B/op  0 allocs/op
68.32 ns/op  0 B/op  0 allocs/op

BenchmarkC243RemotePartCacheHit-32                 4564213  64.75 ns/op  0 B/op  0 allocs/op
BenchmarkC243RemotePartCacheHit-32                 4459216  64.75 ns/op  0 B/op  0 allocs/op
BenchmarkC243RemotePartCacheHit-32                 4621596  65.26 ns/op  0 B/op  0 allocs/op
BenchmarkC243RemotePartCacheHit-32                 4609028  65.00 ns/op  0 B/op  0 allocs/op
BenchmarkC243RemotePartCacheHit-32                 4637025  64.86 ns/op  0 B/op  0 allocs/op

BenchmarkC245RemotePartCacheAdmissionWarmHit-32    4902502  61.18 ns/op  0 B/op  0 allocs/op
BenchmarkC245RemotePartCacheAdmissionWarmHit-32    4864922  61.13 ns/op  0 B/op  0 allocs/op
BenchmarkC245RemotePartCacheAdmissionWarmHit-32    4905524  61.39 ns/op  0 B/op  0 allocs/op
BenchmarkC245RemotePartCacheAdmissionWarmHit-32    4683591  71.53 ns/op  0 B/op  0 allocs/op
BenchmarkC245RemotePartCacheAdmissionWarmHit-32    4296172  63.62 ns/op  0 B/op  0 allocs/op

BenchmarkC245RemotePartCacheCold/eager-32          33374  9263 ns/op  65712 B/op  3 allocs/op
BenchmarkC245RemotePartCacheCold/eager-32          34358  9132 ns/op  65712 B/op  3 allocs/op
BenchmarkC245RemotePartCacheCold/eager-32          31971  9506 ns/op  65712 B/op  3 allocs/op
BenchmarkC245RemotePartCacheCold/eager-32          36170  8892 ns/op  65713 B/op  3 allocs/op
BenchmarkC245RemotePartCacheCold/eager-32          38348  9567 ns/op  65713 B/op  3 allocs/op

BenchmarkC245RemotePartCacheCold/frequency-32      28874 10143 ns/op  65713 B/op  3 allocs/op
BenchmarkC245RemotePartCacheCold/frequency-32      29511 10025 ns/op  65713 B/op  3 allocs/op
BenchmarkC245RemotePartCacheCold/frequency-32      21969 11925 ns/op  65713 B/op  3 allocs/op
BenchmarkC245RemotePartCacheCold/frequency-32      25208 11747 ns/op  65713 B/op  3 allocs/op
BenchmarkC245RemotePartCacheCold/frequency-32      35701  9220 ns/op  65714 B/op  3 allocs/op
```
