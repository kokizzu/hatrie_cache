# CH-016: SQL Async Insert Adapter

`AsyncInsertBuffer.SubmitSQL` exposes the existing bounded, journal-backed
async-insert queue through the SQL `INSERT` grammar. It is opt-in: constructing
an `AsyncInsertBuffer` is still required, and existing command, HTTP, gRPC,
journal, and server defaults do not change.

## Example

```go
buffer, err := hatCache.NewAsyncInsertBuffer(journal, trie, hatCache.AsyncInsertBufferOptions{})
if err != nil {
    return err
}
defer buffer.Close(context.Background())

receipt, err := buffer.SubmitSQL(
    context.Background(),
    "INSERT INTO cache (key, value) VALUES ('user:1', 'Ada')",
)
if err != nil {
    return err
}
response, err := receipt.Wait(context.Background())
if err != nil {
    return err
}
if !response.OK {
    return errors.New(response.Message)
}
```

The receipt completes only after the compiled command is durably journaled and
applied. Queue capacity, batching, flush, close, replay, and backpressure keep
the same semantics as direct `Submit` calls.

## Supported Scope

The adapter accepts one literal, journaled `INSERT` statement. It rejects
`SELECT`, `UPDATE`, `DELETE`, `CALL`, `RETURNING`, `ON CONFLICT`,
`INSERT ... SELECT`, and multi-statement input with
`ErrAsyncInsertSQLUnsupported`. Use `ExecuteSQLMutation` for those richer SQL
semantics.

## Benchmark

Command: `make benchmark-ch016-async-insert`.

Linux/amd64, AMD Ryzen 9 5950X, five samples per case, `-benchtime=100ms`:

| Path | Median ns/op | B/op | allocs/op | Relative CPU | Relative memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| SQL adapter | 15,909 | 3,631 | 14 | 1.02x vs manual | 1.00x vs manual |
| Manual `CompileSQL` + `Submit` | 16,154 | 3,630 | 14 | 1.00x | 1.00x |
| Precompiled command + `Submit` | 13,458 | 1,417 | 6 | 1.20x vs manual | 0.39x vs manual |

The adapter reuses the same lexer token slice as `CompileSQL`, so it adds no
measurable allocation or memory cost versus the existing manual SQL path. The
precompiled-command case is faster because it intentionally excludes SQL
parsing and validation.

Raw `ns/op` samples:

```text
sql_adapter:         15773, 14753, 17122, 16601, 15909
manual_compile:      16154, 19294, 16032, 16770, 15793
precompiled_command: 12998, 13409, 13458, 14227, 13671
```

The first implementation reparsed the source several times and was rejected:
it measured 20,394 ns/op, 10,446 B/op, and 35 allocs/op versus 16,558 ns/op,
3,630 B/op, and 14 allocs/op for the manual path. The final implementation
removed that regression before delivery.
