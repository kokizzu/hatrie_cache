# Adaptive RowBinary Encoding

`hatSql.EncodeSQLRowBinaryAdaptive` chooses among the legacy, delta, and
double-delta RowBinary codecs by encoding candidate representations and
selecting the smallest one for the actual batch. The decoder reads the codec
identifier from the header, so the choice is self-describing.

```go
encoded, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
decoded, err := hatSql.DecodeSQLRowBinaryAdaptive(columns, encoded)
```

For large batches, `EncodeSQLRowBinaryAdaptiveSampled` evaluates only a
positive prefix and encodes the full batch using the selected codec:

```go
encoded, err := hatSql.EncodeSQLRowBinaryAdaptiveSampled(columns, rows, 32)
```

The full variant gives the best decision for data whose shape changes across a
batch, but temporarily encodes each candidate sample and uses more CPU during
selection. The sampled variant reduces candidate work and memory proportional
to the sample size, but can choose a less compact codec when later rows differ
from the prefix. It is opt-in so existing wire behavior remains unchanged.

Both variants preserve the schema-aware RowBinary type contract and enforce the
existing row limits. A positive sample size is required for non-empty sampled
batches and is clamped to the batch length. This feature changes codec choice,
not schema validation, null semantics, or decoded values.

The benchmark in `hat/hatSql/row_binary_adaptive_sampled_test.go` compares full
and 32-row sampling on stable and shape-shifting data, reporting allocations,
CPU, and encoded wire bytes. Focused tests cover codec selection, round trips,
sample clamping, invalid sizes, and compatibility with legacy encoding.
