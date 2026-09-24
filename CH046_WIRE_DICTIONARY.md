# CH046 Wire Dictionary Encoding

## Status

Implemented as an opt-in columnar wire encoding. The default remains the raw
v1 stream for compatibility and predictable CPU/memory usage.

```go
writer := hatSql.NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
    destination,
    columns,
    1024,
    hatSql.SQLColumnarBlockStreamOptions{
        Compression: hatSql.SQLColumnarBlockStreamCompressionDictionary,
    },
)
```

Only `SQLRowBinaryString` columns are dictionary candidates. The writer builds
one dictionary per block, keeps NULL markers, and falls back to raw payload for
an individual column when the dictionary would be larger. The stream remains
version 2 whenever this option is selected, so high-cardinality workloads still
pay the version-2 per-column framing overhead even when their columns fall back
to raw.

## Measured Tradeoff

The benchmark uses 4,096 rows, two repeated string columns, and one int64
column on an AMD Ryzen 9 5950X. Values are representative medians from five
runs; the exact command is `make benchmark-ch046-dictionary-after`.

| Path | Wire bytes | Encode | Encode memory | Decode | Decode memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Raw v1 | 119,217 | 0.45 ms | 351.7 KB / 196 allocs | 1.6 ms | 1.80 MB / 28,483 allocs |
| Dictionary, repeated strings | 41,809 | 3.2 ms | 1.60 MB / 25,018 allocs | 3.7 ms | 2.64 MB / 51,113 allocs |
| Flate adaptive | 9,025 | 12.5 ms | 58.0 MB / 1,229 allocs | not measured here | not measured here |

Dictionary mode sends **2.85x fewer bytes** than raw v1 for the repeated-string
workload, at approximately **7x encode CPU**, **4.6x encode memory**, **2.3x
decode CPU**, and **1.5x decode memory**. It is useful when bandwidth is the
constraint and the caller can spend CPU; it is not a general performance
default.

For a high-cardinality workload with unique strings, the dictionary candidate
is rejected per column. The measured version-2 stream was about 125,034 bytes,
3.3 ms, and 2.09 MB, so dictionary mode is roughly 1.05x larger and 7x slower
than raw. Use the zero-value options for latency-sensitive or high-cardinality
transfers.

## Safety Rules

- Dictionary entry count cannot exceed the block row count.
- Entry lengths, decoded output, and cumulative block bytes stay within the
  existing 64 MiB limit.
- For materialized columns, invalid IDs, NULL markers, truncated varints,
  truncated entries, and trailing bytes fail the reader before the block is
  published; projected-away columns are only length-bounded.
- Skipped projected columns remain length-bounded and are not materialized.
