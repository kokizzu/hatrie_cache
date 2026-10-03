# TT-035 Request Deadlines

`hatPeer` can optionally propagate a caller's absolute context deadline to the
remote handler. The feature is disabled by default and must be enabled on both
peer sessions:

```go
session, err := NewCompactPeerSession(conn, CompactPeerSessionOptions{
	EnableRequestDeadlines: true,
})
```

When the caller context has a deadline, the request carries its Unix-nanosecond
deadline in a protocol-v2 frame. The remote handler receives a context with
that deadline and can stop work through `ctx.Done()`. `CallTemplate` uses the
prepared-template writer for requests without a deadline; a deadline-bearing
template uses the regular writer so the metadata is encoded correctly.

Both peers must opt in. A legacy decoder rejects a deadline frame with
`ErrCompactProtocolDeadlineUnsupported`; ordinary v1 frames are byte-for-byte
unchanged when the option is enabled. Manual request cancellation remains a
separate opt-in feature.

## Measured Tradeoff

Measured with:

```text
go test -run '^$' -bench '^BenchmarkTT035' -benchmem -count=5 ./hat/hatPeer
```

Environment: Linux amd64, AMD Ryzen 9 5950X 16-Core Processor. The table uses
the median of five benchmark samples.

| Encoding path | Median CPU | Wire bytes/op | Heap bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Legacy v1 request | 18.75 ns | 19 | 0 B | 0 |
| Deadline v2 request | 27.36 ns | 28 | 0 B | 0 |
| Deadline overhead | 1.46x CPU | +9 bytes (+47.4%) | no change | no change |

Raw samples:

```text
legacy:   19.68 18.66 18.51 18.75 20.59 ns/op
deadline: 27.50 27.75 26.93 27.36 25.98 ns/op
```

The default remains the lower-cost legacy path. Enable deadline propagation
when bounded remote work is more important than the small per-request encoding
overhead. The request deadline is advisory to the remote handler; storage and
application work must still honor the supplied context.

## Verification

The focused suite covers protocol round trips, legacy compatibility, invalid
metadata, `Call`, `CallTemplate`, disabled opt-in behavior, and remote handler
cancellation. It passes under normal and race-enabled execution and was run
100 times. The clean baseline `hatPeer` package currently has unrelated
pre-existing failures in `TestCompactProtocolRoundTrip` and a mutual-TLS test;
TT-035 verification therefore uses the focused suite plus vet and stress runs.
