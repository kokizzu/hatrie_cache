# M-U46 Differential Checkpoint Export/Import

M-U46 adds a bounded, deterministic checkpoint format for signed differential
rows. `EncodeDifferentialCheckpoint` and `DecodeDifferentialCheckpoint` use
HDF1 by default. `DifferentialDataflow.ImportCheckpoint` validates and applies
one complete checkpoint at a frontier.

## API

```go
checkpoint := hatSql.DifferentialCheckpoint{
	Frontier: 100,
	Rows: []hatSql.DifferentialRow{
		{Key: "acct:7", Time: 90, Diff: 1, Row: hatSql.Row{
			"active": true,
			"balance": int64(42),
		}},
	},
}

payload, err := hatSql.EncodeDifferentialCheckpoint(checkpoint)
if err != nil {
	return err
}

decoded, err := hatSql.DecodeDifferentialCheckpoint(payload)
if err != nil {
	return err
}
_ = decoded

if err := flow.ImportCheckpoint(payload); err != nil {
	return err
}
```

`EncodeDifferentialCheckpointWithOptions` and
`DecodeDifferentialCheckpointWithOptions` accept explicit bounds. Zero values
select these defaults:

| Option | Default |
| --- | ---: |
| `MaxEncodedBytes` | 64 MiB |
| `MaxRows` | 1,048,576 |
| `MaxFieldsPerRow` | 1,024 per map |
| `MaxValueDepth` | 16 |

Negative limits are rejected. Bounds are applied before unbounded allocation,
and decoding returns no partial checkpoint when validation fails.

## HDF1 Format

HDF1 contains a fixed `HDF1` magic prefix, a version byte, the frontier, a
uvarint row count, canonical rows, and a four-byte little-endian CRC32C over
the preceding bytes. Rows and map fields are sorted by their canonical binary
representation, so equivalent input order produces identical payloads. The
decoder rejects unknown versions, truncated data, trailing data, invalid
values, checksum failures, and over-limit payloads.

The value codec preserves the supported concrete Go types used by SQL rows:
nil, booleans, strings, byte slices, signed and unsigned integer widths,
`float32`, `float64`, `time.Duration`, `time.Time`, `json.Number`, nested
`hatSql.Row`/`map[string]interface{}`, `[]interface{}`, and the supported
typed slices (`[]string`, `[]int64`, `[]uint64`, `[]float64`, `[]bool`).
`json.RawMessage` is retained as raw bytes. Unsupported map keys, structs, and
other container types return `ErrDifferentialCheckpointUnsupported`.

The CRC detects accidental corruption; it is not authentication. Protect HDF1
with the existing authenticated transport or an authenticated storage layer
when payloads can be supplied by an untrusted peer. The bounded decoder does
not execute code or allocate from lengths outside the configured limits.

## Import Semantics

`ImportCheckpoint` first decodes and validates the complete payload. Every row
timestamp must be at or before the checkpoint frontier. It then submits the
rows through the existing batch-atomic sink and advances the dataflow frontier
only after the sink succeeds. A sink that can partially apply a batch must
provide its own rollback or transactional boundary; the dataflow cannot undo
external side effects.

An empty checkpoint is a valid frontier-only advance. Re-importing the same
frontier is allowed; importing a lower frontier returns
`ErrDifferentialDataflowFrontierRegression`. Decode errors, invalid rows, sink
errors, and frontier regressions leave the dataflow rows, frontier, and stats
unchanged.

The HDF1 API is explicit and does not silently downgrade malformed input to
JSON. Applications that need a legacy JSON compatibility path can continue to
marshal `DifferentialCheckpoint` with the existing JSON codec at their
transport boundary and select that codec explicitly. This avoids ambiguous
format detection and accidental downgrade on corrupt input.

## Measurements

Commands:

```sh
make benchmark-mu46-baseline
make benchmark-mu46
make measure-mu46-payload
```

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture has 256
rows and four scalar fields per row. The baseline uses `github.com/goccy/go-json`
on the same public `DifferentialCheckpoint` value.

| Workload | Format | Raw ns/op samples | Median ns/op | B/op | allocs/op | Payload |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Encode | JSON baseline | 188484; 190419; 195096; 199312; 190237 | 190419 | 52,505 | 258 | 27,219 B |
| Encode | HDF1 | 84106; 87283; 90556; 87686; 84216 | 87283 | 72,408 | 9 | 15,944 B |
| Decode | JSON baseline | 243759; 245686; 245731; 239643; 249165 | 245686 | 178,081 | 3,585 | 27,219 B |
| Decode | HDF1 | 152051; 154818; 153280; 152073; 150683 | 152073 | 129,232 | 4,326 | 15,944 B |

Relative to JSON, HDF1 is 2.18x faster to encode, 1.62x faster to decode, and
uses 1.71x less wire space. Encoding uses 1.38x more allocated bytes but has
28.7x fewer allocations. Decoding uses 1.38x less allocated bytes but 1.21x
more allocations, so the format is a CPU/bandwidth win rather than an
allocation win in every path.

The encoder was measured before and after replacing one heap-backed row slice
per input row with one row buffer plus sortable offsets and stack storage for
small map key lists:

| Encoder version | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Initial HDF1 implementation | 114,784 | 111,280 | 787 |
| Final offset-backed encoder | 87,283 | 72,408 | 9 |

That focused change is 1.32x faster, uses 1.54x less allocated memory, and
performs 87.4x fewer allocations while preserving the exact 15,944-byte
payload and all round-trip tests.
