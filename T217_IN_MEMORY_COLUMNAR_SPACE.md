# T217 In-Memory Columnar Space

T217 adds an importable `hatDataStructure.ColumnarSpace` for append-heavy
analytical data. It combines ClickHouse-style typed column buffers,
Materialize-style batch-oriented processing, and a Tarantool-like independent
space boundary without changing existing row-oriented or SQL defaults.

## Scope And Defaults

The feature is opt-in: callers construct a `ColumnarSpace` with a fixed
schema. It is volatile, append-only storage. It does not replace
`StorageSpace`, persist data, replicate rows, or automatically become a SQL
source. Those boundaries keep backup, recovery, and compatibility behavior
unchanged.

Supported encodings are:

- `ColumnarInt64`: contiguous 64-bit values;
- `ColumnarFloat64`: contiguous 64-bit values;
- `ColumnarBool`: bit-packed values;
- `ColumnarString`: one `uint32` offset array and one contiguous UTF-8 payload;
- `ColumnarBytes`: one `uint32` offset array and one contiguous byte payload.

Null values use a lazy bit-packed validity bitmap. A null string or byte cell
does not retain the caller's payload. `Append` validates the complete row
before touching any column, so wrong-width, wrong-type, and capacity failures
are atomic.

## Example

```go
space, err := hatDataStructure.NewColumnarSpace(
	hatDataStructure.ColumnarSpaceOptions{
		Name:     "events",
		Capacity: 100_000,
		Columns: []hatDataStructure.ColumnarColumnSpec{
			{Name: "id", Kind: hatDataStructure.ColumnarInt64},
			{Name: "region", Kind: hatDataStructure.ColumnarString},
			{Name: "active", Kind: hatDataStructure.ColumnarBool},
		},
	},
)
if err != nil {
	return err
}
defer space.Close()

err = space.Append([]hatDataStructure.ColumnarValue{
	{Kind: hatDataStructure.ColumnarInt64, Valid: true, Int64: 42},
	{Kind: hatDataStructure.ColumnarString, Valid: true, String: "apac"},
	{Kind: hatDataStructure.ColumnarBool, Valid: true, Bool: true},
})
```

`Column` returns a detached snapshot for projection or vectorized scanning;
`ValueAt` returns an independent cell for point inspection. `MemoryBytes`
reports retained column buffers, excluding Go map, mutex, schema, and allocator
metadata, so it is useful for comparing layouts rather than a full RSS report.

## Verification And Measurement

```text
make test-t217
make race-t217
make vet-t217
make benchmark-t217
```

The benchmark compares 4,096 rows with three fields against
`[]map[string]interface{}`. On Linux/amd64 with an AMD Ryzen 9 5950X, the
final three-sample run measured:

| Workload | Columnar | Row maps | Result |
| --- | ---: | ---: | --- |
| Build CPU | `612,346-619,238 ns/op` | `1,401,240-1,457,907 ns/op` | about `2.3x` faster |
| Build heap | `305,494-305,499 B/op` | `1,606,959-1,606,962 B/op` | about `5.3x` lower |
| Build allocations | `4,126` | `24,322` | about `5.9x` fewer |
| Project `int64` column | `1,071-1,089 ns/op` | `33,757-34,244 ns/op` | about `32x` faster |

The retained-buffer probe reported `122,884` bytes for columnar buffers. The
row-map comparison reports only a `102,400`-byte scalar payload estimate and
excludes map headers, interface words, and allocator overhead; it is therefore
not a claim that the probe measures total row-map RSS. The build allocation
measurement is the stronger memory comparison.
