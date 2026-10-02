# CH-019 Remote-Part Checksum Admission

`hatStorage.RemotePartCache` can optionally verify a loaded immutable remote
part before admitting it to the cache. Enable it with
`RemotePartCacheOptions.VerifyChecksums`:

```go
cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
	MaxBytes:        64 << 20,
	VerifyChecksums: true,
})
```

The default is `false` to preserve existing callers that use opaque checksum
strings and to keep the normal cache-miss path free of hashing work. When
enabled, the current supported format is `sha256:<64 hexadecimal digits>`;
this is the format emitted by `UploadRemotePartMultipart`. A loaded payload is
accepted only after both its declared size and checksum match. A mismatch or
unsupported checksum returns an error, increments `Stats().ChecksumFailures`,
and publishes neither the bytes nor a cache entry.

Verification happens only on a loader miss. A cache hit returns the already
verified immutable entry without hashing again. The checksum comparison is
constant-time, and hex decoding uses fixed-size storage so verification adds no
per-miss allocation beyond the existing cache-copy path.

This is a partial CH-019 adoption. The cache does not discover alternate
replicas, repair manifests, or delete remote objects. A caller that owns those
policies should retry another replica, record the failed source, and invalidate
or replace the bad reference according to its consistency policy.

## Measurements

The focused benchmark uses a 64 KiB payload on Linux/amd64 with an AMD Ryzen 9
5950X and five samples per case. The baseline is the pre-feature cache path;
the post-feature control has verification disabled, which is the default.

| Workload | Median ns/op | Median B/op | Median allocs/op | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Baseline cold miss | 9,018 | 65,808 | 4 | pre-feature control |
| Post-feature cold miss, verification disabled | 7,768 | 65,808 | 4 | default path; no measurable memory/allocation cost |
| Post-feature cold miss, SHA-256 verification enabled | 38,406 | 65,808 | 4 | 4.94x slower than the post-feature control; same memory/allocation profile |
| Verified cache hit | 63.38 | 0 | 0 | no rehashing and no per-read allocation |

The improvement here is integrity, not cold-miss throughput. Verification is
opt-in because hashing a 64 KiB cold payload costs about 30.6 microseconds over
the unverified post-feature control. The fixed-size decoder removed the initial
extra allocation from the implementation, reducing the verified path from
65,840 to 65,808 B/op and from 5 to 4 allocs/op.

### Raw baseline output

```text
BenchmarkCH019RemotePartCacheColdMissBaseline-32 153777 9018 ns/op 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissBaseline-32 134552 8894 ns/op 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissBaseline-32 145676 9210 ns/op 65809 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissBaseline-32 134308 7939 ns/op 65809 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissBaseline-32 128179 9582 ns/op 65809 B/op 4 allocs/op
```

### Raw post-feature output

```text
BenchmarkCH019RemotePartCacheColdMissNoVerification-32 148267 7065 ns/op 9276.63 MB/s 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissNoVerification-32 183144 7308 ns/op 8968.18 MB/s 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissNoVerification-32 155706 7816 ns/op 8384.71 MB/s 65809 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissNoVerification-32 142617 7768 ns/op 8436.44 MB/s 65810 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissNoVerification-32 145898 9042 ns/op 7247.95 MB/s 65809 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissWithVerification-32 31257 38750 ns/op 1691.27 MB/s 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissWithVerification-32 31620 39664 ns/op 1652.30 MB/s 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissWithVerification-32 30235 38406 ns/op 1706.39 MB/s 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissWithVerification-32 32149 37175 ns/op 1762.92 MB/s 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheColdMissWithVerification-32 32394 38013 ns/op 1724.04 MB/s 65808 B/op 4 allocs/op
BenchmarkCH019RemotePartCacheHitWithVerification-32 18745982 63.38 ns/op 1034036.94 MB/s 0 B/op 0 allocs/op
BenchmarkCH019RemotePartCacheHitWithVerification-32 18996555 63.40 ns/op 1033694.35 MB/s 0 B/op 0 allocs/op
BenchmarkCH019RemotePartCacheHitWithVerification-32 18758750 59.40 ns/op 1103236.10 MB/s 0 B/op 0 allocs/op
BenchmarkCH019RemotePartCacheHitWithVerification-32 16529581 66.73 ns/op 982135.63 MB/s 0 B/op 0 allocs/op
BenchmarkCH019RemotePartCacheHitWithVerification-32 18961641 57.67 ns/op 1136413.13 MB/s 0 B/op 0 allocs/op
```
