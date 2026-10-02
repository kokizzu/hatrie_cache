# CH-046 Native Block Protocol

Hatrie Cache now exposes an importable `hatSql` native block codec for typed,
column-framed transfer:

```go
payload, err := hatSql.EncodeSQLNativeBlock(columns, rows, hatSql.SQLNativeBlockOptions{
	Sequence:  sequence,
	Progress:  sourceFrontier,
	Complete:  false,
})
block, err := hatSql.DecodeSQLNativeBlock(payload)
```

Each block carries its schema, sequence, progress, and completion flag. Each
column has an explicit length-delimited frame. Nullable columns use a packed
validity bitmap followed by only non-NULL values, while scalar value encoding
reuses the existing RowBinary type rules. The decoder enforces bounded row and
column counts, a 256 MiB block limit, valid schema metadata, bitmap trailing
bits, complete payload consumption, and no trailing bytes.

This is a new wire framing option, not a silent change to the legacy
RowBinary/protobuf protocols. Callers must negotiate it with their peer before
using it; that preserves compatibility with existing clients and allows a
transport to skip or route complete column frames. The current implementation
does not claim a default CPU win until the paired benchmark is run on a
buildable parent tree.

## Verification

Run `make format-native-block`, `make test-native-block`, and
`make benchmark-native-block` from the repository root. The focused tests cover
schema and row round trips, nullable values, metadata, deterministic encoding,
and malformed trailing input. The benchmark compares native-block and legacy
RowBinary encoding with `-benchmem`, five samples, and a 1,024-row mixed typed
workload.
