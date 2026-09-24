# M052 Dataflow Plan Codec

Reusable SQL dataflow plans can now use a compact versioned binary format for
transfer or storage:

```go
encoded, err := hatSql.EncodeSQLDataflowPlan(plan)
if err != nil {
	return err
}
decoded, err := hatSql.DecodeSQLDataflowPlan(encoded)
```

`SQLDataflowPlan.MarshalBinary` and `UnmarshalBinary` expose the same binary
format through Go's standard binary-marshaler interfaces. The payload starts
with `HDP1`, uses bounded unsigned varints, and is validated as a complete
plan before it is returned. The decoder rejects truncated data, trailing
bytes, invalid fragment IDs or dependencies, oversized strings, more than
1,000,000 fragments, and payloads larger than 16 MiB. Failed
`UnmarshalBinary` calls leave the receiver unchanged.

The existing JSON representation remains available as an explicit fallback:

```go
jsonPayload, err := hatSql.EncodeSQLDataflowPlanJSON(plan)
decoded, err := hatSql.DecodeSQLDataflowPlanJSON(jsonPayload)
```

`DecodeSQLDataflowPlan` accepts either binary or a JSON object, so a rollout
can read old JSON snapshots while writing the binary format. The codec carries
plan metadata only; it does not execute the source string or grant access to a
runner. Callers must still authenticate and authorize the surrounding
transport.

## Measurement

Five benchmark samples on Linux amd64, AMD Ryzen 9 5950X, with the same
32-fragment plan. The JSON baseline was measured before implementation; the
final run includes both encodings. `binary_bytes` and `json_bytes` are the
wire payload sizes, not heap bytes.

| Operation | Baseline JSON | Final binary | Relative result |
| --- | ---: | ---: | ---: |
| Encode | 8,290 ns/op, 4,173 B/op, 2 allocs | 2,187 ns/op, 2,688 B/op, 1 alloc | 3.79x faster, 35.6% fewer bytes, 1 fewer alloc |
| Decode | 45,771 ns/op, 7,489 B/op, 111 allocs | 6,078 ns/op, 4,456 B/op, 104 allocs | 7.53x faster, 40.5% fewer bytes, 7 fewer allocs |
| Wire payload | 3,698 bytes | 2,349 bytes | 36.5% smaller |

Raw five-sample medians were calculated from these runs:

```text
baseline JSON encode: 8632 8585 8277 8132 8290 ns/op
baseline JSON decode: 44291 46104 45771 45864 43920 ns/op
final JSON encode:    8837 8299 8100 8310 8937 ns/op
final JSON decode:    46295 47935 45768 47656 45890 ns/op
binary encode:        2187 2215 2208 2105 2102 ns/op
binary decode:        7091 6008 5940 6512 6078 ns/op
```

The binary format is now the default for the new plan codec APIs. JSON remains
the intentional compatibility choice when human readability or an existing
JSON-only peer matters. Run `make benchmark-m048-codec` to repeat the
comparison.
