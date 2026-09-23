# TT-G11 WAL Segment Compression

Segmented command journals now default to CRC-protected Zstandard for immutable
archived segments. The active journal remains writable and uncompressed; only a
segment at rotation is compressed. Existing uncompressed segments remain
readable, and the frame CRC is checked during inspection and replay.

The default is exposed through `DefaultCommandJournalSegmentCompression` and
can be overridden without changing the journal format:

```go
options := hatCache.CommandJournalOptions{
    SegmentMaxBytes:    64 << 20,
    SegmentCompression: hatCache.CommandJournalSegmentCompressionNone,
}
```

Use `SegmentCompressionNone` when rotation CPU or immediate local reads matter
more than retained WAL size. Compression is relevant only when
`SegmentMaxBytes` is positive; unsegmented journals are unchanged.

The encoder uses one Zstandard worker and keeps the frame checksum enabled.
This bounds compression concurrency and reduces allocations while retaining the
same compressed bytes in the measured fixture.

## Measured Tradeoff

The focused benchmark used 282,624 highly repetitive journal bytes on Linux
amd64 with an AMD Ryzen 9 5950X and five samples per sub-benchmark. These are
codec measurements, not a claim about end-to-end disk latency.

| Path | Median ns/op | Median heap/op | Median allocs/op | Compressed bytes |
| --- | ---: | ---: | ---: | ---: |
| Raw copy control | 47.37 | 48 B | 1 | 282,624 |
| Existing Zstandard settings | 2,908,295 | 19,133,259 B | 71 | 120 |
| Default single-worker Zstandard | 2,220,986 | 18,562,891 B | 48 | 120 |

The selected encoder is `1.31x` faster than the previous Zstandard settings,
uses `1.03x` less measured heap, and uses `1.48x` fewer allocations. The frame
is `2,355x` smaller than the raw fixture. The remaining cost is compression
work during segment rotation; `SegmentCompressionNone` is the fallback when
that cost is unacceptable.

Raw output is recorded in `BENCHMARK.md` under `TT-G11 WAL Segment Compression`.
