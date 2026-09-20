# C246: TTL-Driven Recompression

Status: implemented as an opt-in `hatDataStructure` policy.

ClickHouse-style TTL recompression is now represented by
`TTLRecompressionPolicy` and `TupleCompressor.RecompressIfDue`. A record can
be rewritten with a colder codec after its recompression horizon while still
remaining readable, and can be deleted later at an independent deletion
horizon. This avoids coupling storage savings to row removal.

## API

```go
policy := hatDataStructure.TTLRecompressionPolicy{
    RecompressAfter: 7 * 24 * time.Hour,
    DeleteAfter:     30 * 24 * time.Hour,
}

decision := policy.Decide(createdAt, now)
switch decision {
case hatDataStructure.TTLRecompressionKeep:
    // Keep the current frame.
case hatDataStructure.TTLRecompressionRewrite:
    frame, _, err = hot.RecompressIfDue(frame, createdAt, now, policy, cold)
case hatDataStructure.TTLRecompressionDelete:
    // Remove the record from the owning storage.
}
```

`RecompressIfDue` uses the receiver to decode the current frame and `target`
to encode the colder representation. The keep path returns the original frame
without decoding or allocating. The delete path returns `nil` without
decoding, so deletion does not depend on the old payload being readable. The
rewrite path returns a new frame; the caller can atomically persist it before
releasing the old frame.

`RecompressAfter == 0` disables rewriting and `DeleteAfter == 0` disables
deletion. Both horizons must be non-negative, and when both are configured
the rewrite horizon must precede the delete horizon. Invalid policies are
rejected by `Validate` and conservatively keep records in `Decide`.

## Scope And Defaults

The policy is intentionally importable and storage-owner driven. It does not
silently alter LevelDB/Pebble compaction, cache TTL deletion, tuple framing,
backup formats, or wire defaults. A storage owner supplies record creation
times and decides when to scan, rewrite, or delete. That separation lets a
caller schedule recompression during low load without making deletion more
aggressive.

Tuple frames retain the existing size bounds, checksum validation, and
compression options. The new method only composes those existing safe
operations; it does not bypass frame validation or allow an unbounded decode.

## Measurements

Five 200 ms samples were collected on an AMD Ryzen 9 5950X, Linux amd64. The
rewrite benchmarks use a 32 KiB repetitive payload and compare the helper with
the equivalent manual decode/re-encode control.

| Operation | Median time | Heap | Allocs | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Policy decision | 15.77 ns/op | 0 B/op | 0 allocs/op | Allocation-free lifecycle decision |
| Keep fast path | 21.83 ns/op | 0 B/op | 0 allocs/op | No decode or rewrite before the horizon |
| Recompression helper | 18,961 ns/op | 32,907 B/op | 2 allocs/op | Codec-dominated rewrite |
| Manual decode/re-encode control | 19,984 ns/op | 32,904 B/op | 2 allocs/op | Baseline for the same rewrite |

The helper is within benchmark noise of the manual control; its value is the
separate, validated lifecycle decision, not a claim of a large rewrite CPU
speedup. The important hot-path result is zero allocation before a rewrite is
due.

Raw samples:

```text
BenchmarkTTLRecompressionDecision-32 16550566 15.91 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionDecision-32 15719178 14.88 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionDecision-32 15020208 15.23 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionDecision-32 14764033 16.84 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionDecision-32 14416144 15.77 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionKeepFastPath-32 12255920 23.68 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionKeepFastPath-32 10833732 19.85 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionKeepFastPath-32 10851616 21.83 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionKeepFastPath-32 12065115 20.81 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionKeepFastPath-32 11315329 23.00 ns/op 0 B/op 0 allocs/op
BenchmarkTTLRecompressionRewrite-32 11688 20094 ns/op 1630.77 MB/s 32909 B/op 2 allocs/op
BenchmarkTTLRecompressionRewrite-32 11877 20649 ns/op 1586.92 MB/s 32908 B/op 2 allocs/op
BenchmarkTTLRecompressionRewrite-32 12075 18763 ns/op 1746.38 MB/s 32907 B/op 2 allocs/op
BenchmarkTTLRecompressionRewrite-32 12957 18710 ns/op 1751.37 MB/s 32903 B/op 2 allocs/op
BenchmarkTTLRecompressionRewrite-32 12906 18961 ns/op 1728.14 MB/s 32903 B/op 2 allocs/op
BenchmarkTTLRecompressionManualRewrite-32 13005 18678 ns/op 1754.41 MB/s 32903 B/op 2 allocs/op
BenchmarkTTLRecompressionManualRewrite-32 12662 18832 ns/op 1739.98 MB/s 32904 B/op 2 allocs/op
BenchmarkTTLRecompressionManualRewrite-32 12746 20639 ns/op 1587.69 MB/s 32904 B/op 2 allocs/op
BenchmarkTTLRecompressionManualRewrite-32 11270 21216 ns/op 1544.51 MB/s 32911 B/op 2 allocs/op
BenchmarkTTLRecompressionManualRewrite-32 11762 19984 ns/op 1639.73 MB/s 32909 B/op 2 allocs/op
```

## Verification

Focused tests cover future timestamps, independent rewrite/delete horizons,
payload preservation, delete-without-decode, invalid policies, and missing
rewrite targets. Run:

```text
make format-c246-ttl-recompression
make test-c246-ttl-recompression
make race-c246-ttl-recompression
make vet-c246-ttl-recompression
make benchmark-c246-ttl-recompression
```

The focused tests and race/vet checks pass. A separate full
`hatDataStructure` package run currently has an unrelated existing failure in
`TestSpillableArrangementSpillsReadsDeletesAndCompacts`, where the expected
disk size reduction is not observed; the C246 tests do not touch that path.
