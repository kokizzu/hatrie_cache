# Schema Migration Dry Run

`hatSchema.Preview` validates and applies a migration to an independent schema
copy without publishing it. This gives operators and tooling a cheap dry run
before calling `hatSchema.Apply` or storing the result.

```go
candidate, err := hatSchema.Preview(&current, migration)
if err != nil {
    return err
}
fmt.Printf("candidate schema version: %d\n", candidate.Version)
```

The preview requires the migration to have a positive sequential version, a
name, both `Up` and `Down` changes, and valid changes for the current schema.
The input schema is unchanged on success and on failure. The returned schema
does not share mutable source, column, or constraint backing with the input.

`Preview` validates schema shape and migration dependencies only. It does not
scan cache rows or prove that existing data satisfies a new constraint. Use a
separate data validation or restore rehearsal when the migration changes row
compatibility.

`hatSchema.Apply` uses the same preview path and publishes the returned schema
only after every migration change succeeds, so the dry run and apply operation
have identical validation semantics.

## Measured Cost

`make benchmark-tt044-migration-preview` measured a one-source add-column
migration over five one-second samples:

| Operation | Median time | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| `Preview` | 375.6 ns | 880 | 4 |

The operation is intended for control-plane tooling and has no cost on normal
reads or writes. Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md).
