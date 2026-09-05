# Adaptive RowBinary Codec

`EncodeSQLRowBinaryAdaptive` evaluates the legacy, first-order delta, and
second-order delta encodings for a complete batch, then wraps the smallest
payload in an `HSA1` envelope. The envelope stores the selected codec and
payload length, so decoding is deterministic and does not rely on guessing
from arbitrary legacy bytes.

```go
wire, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
if err != nil {
    return err
}
rows, err = hatSql.DecodeSQLRowBinaryAdaptive(columns, wire)
```

Existing `EncodeSQLRowBinary` and `DecodeSQLRowBinary` remain unchanged. Use
those functions for compatibility with an older reader. Use the direct delta
functions when the data shape is already known and the extra selection pass is
not worthwhile.

## Tradeoff

The benchmark uses 128 rows with an increasing `int64` id, increasing
`DateTime`, and a repeated string. Run it with:

```text
make benchmark-sql-row-binary-adaptive
```

Representative five-sample ranges on the repository benchmark host:

| Path | Time | Wire bytes | Allocations | Allocated bytes |
| --- | ---: | ---: | ---: | ---: |
| Legacy encode | 7.7-8.0 us | 2,944 | 11 | 8,440 |
| Adaptive encode | 24.3-25.0 us | 1,181 | 20 | 17,784 |
| Legacy decode | 29.3-29.9 us | 2,944 | 648 | 51,320 |
| Adaptive decode | 29.8-30.4 us | 1,181 | 641 | 50,304 |

Adaptive encoding is about `2.5x` smaller on this workload, but costs about
`3.1x` encode CPU and `2.1x` allocation bytes. It is therefore not the global
default. It is appropriate when bandwidth is the limiting resource and the
batch can absorb a selection pass; otherwise choose the legacy or direct delta
API explicitly.
