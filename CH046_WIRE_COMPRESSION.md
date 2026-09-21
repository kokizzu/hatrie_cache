# CH-046 Wire Compression

## Status

The columnar stream now supports versioned per-column compression, while the
existing raw v1 format remains the default. This is deliberate: the benchmark
shows that Flate saves bandwidth but imposes a large CPU and allocation cost on
the current Go implementation. The same v2 framing now also supports explicit
per-block dictionary encoding for repeated string columns; its measured
tradeoff is documented in [CH046_NATIVE_WIRE_PROTOCOL.md](CH046_NATIVE_WIRE_PROTOCOL.md).

## Configuration

Existing constructors preserve the raw v1 format. Use the options constructor
when the network link is the bottleneck:

```go
writer := NewSQLColumnarBlockStreamWriterWithColumnsAndOptions(
    destination,
    columns,
    256,
    SQLColumnarBlockStreamOptions{
        Compression: SQLColumnarBlockStreamCompressionAuto,
    },
)
```

Available modes:

| Mode | Wire version | Behavior |
| --- | ---: | --- |
| `SQLColumnarBlockStreamCompressionNone` | 1 | Raw payloads; default and compatibility fallback |
| `SQLColumnarBlockStreamCompressionAuto` | 2 | BestSpeed Flate is kept only when it is smaller |
| `SQLColumnarBlockStreamCompressionFlate` | 2 | Always Flate-compresses each column |

`CompressionLevel: 0` selects `flate.BestSpeed`; valid explicit levels are the
standard Go Flate range `-2..9`.

Readers accept both v1 and v2 streams. V2 records an encoding byte before each
column payload. Decompression is limited by the existing 64 MiB block budget,
and projected-out columns are consumed without decompression.

Dictionary encoding is configured independently:

```go
SQLColumnarBlockStreamOptions{
	Dictionary: SQLColumnarBlockStreamDictionaryAuto,
}
```

It applies only to `SQLRowBinaryString` columns, keeps output only when it is
smaller than raw, and falls back to raw v2 for high-cardinality blocks. When
both dictionary and compression are enabled, a winning dictionary is emitted
as dictionary v2; columns that do not qualify continue through the configured
compression path.

## Benchmark

Environment: AMD Ryzen 9 5950X, Linux amd64, 4,096 rows, three columns, 256
rows per block, five benchmark samples per mode. The pre-change run used the
original constructor and raw v1 framing.

| Mode | Raw samples (ns/op) | Median ns/op | B/op | allocs/op | Wire bytes | CPU vs pre-change | Wire vs pre-change |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Pre-change raw v1 | 462110, 432332, 460968, 456208, 473646 | 460968 | 351479 | 195 | 119217 | 1.00x | 1.00x |
| Current default raw v1 | 432796, 436732, 415888, 397036, 392424 | 415888 | 351660 | 196 | 119217 | 1.11x faster | 1.00x |
| Explicit Auto / BestSpeed | 12618325, 12935069, 11413642, 11496941, 10824834 | 11496941 | 58011656 | 1228 | 9025 | 24.9x slower | 13.2x smaller |
| Explicit Flate / HuffmanOnly | 7345903, 7688601, 7124065, 7703173, 7697284 | 7688601 | 35688752 | 1133 | 46225 | 16.7x slower | 2.58x smaller |

The current default is effectively unchanged in memory and wire size. Auto is
not the default because its 13.2x bandwidth reduction costs about 25x CPU and
165x allocations in this workload. Huffman-only is cheaper but still costs
about 17x CPU and 102x allocations for only a 2.58x wire reduction.

The benchmark targets are:

```text
make benchmark-ch046-before
make benchmark-ch046-after
```

Dictionary encoding is available as `SQLColumnarBlockStreamDictionaryAuto`,
but remains opt-in because its repeated-string benchmark is about 2.07x slower
to encode and uses 2.88x more allocations for a 2.58x wire-size reduction.
