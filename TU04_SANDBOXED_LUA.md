# T-U04: bounded Lua UDF runtime

`LANGUAGE LUA` is an optional stored-function runtime. It is available only in
a binary built with `-tags luajit`; a normal build rejects it instead of
silently selecting another interpreter. The runtime is useful when a SQL
predicate needs Lua expression semantics, but `GO` remains the default for
portable and lower-overhead predicates.

## Safety contract

The Lua state is created without standard libraries. The function cannot use
`io`, `os`, `package`, `debug`, FFI, a module loader, the network, or the
filesystem. Function source is still restricted to one `return` expression,
declared scalar arguments, and scalar results.

Every registry applies bounded defaults:

| Option | Default | Purpose |
| --- | ---: | --- |
| `LuaExecutionLimit` | 10,000,000 instructions | Stops runaway loops in one batch. |
| `LuaMemoryLimitBytes` | 64 MiB | Rejects observed VM memory above the limit and preflights string/byte input. |
| `LuaMaxBatchCalls` | 100,000 rows | Caps the input and result tables. |
| `LuaMaxSourceBytes` | 64 KiB | Caps persisted or newly registered source. |

The limits are normalized when `NewSQLFunctionRegistryWithOptions` receives a
zero or negative value. The memory check samples Lua's VM counter from the
instruction hook; it is deliberately bounded and does not claim to be a
process-wide allocator quota. Operators that need a tighter ceiling can set a
smaller limit, which also increases the sampling frequency. String and byte
inputs are rejected before they are copied into Lua when their estimated
payload alone exceeds the limit.

Quota failures are returned as `SQLFunctionError`. The runtime resets its
per-batch budget and remains usable after an instruction-limit failure. The
state is serialized by a mutex, so concurrent SQL callers cannot interleave
Lua stack operations.

## Example

Build and test the optional runtime:

```text
go test -tags luajit ./hat/hatCache -run '^TestSQLLuaFunction'
```

Register a bounded function:

```go
registry := hatCache.NewSQLFunctionRegistryWithOptions(
    hatCache.SQLFunctionRegistryOptions{
        LuaExecutionLimit:   2_000_000,
        LuaMemoryLimitBytes: 8 << 20,
        LuaMaxBatchCalls:    10_000,
    },
)
definition := hatCache.SQLFunctionDefinition{
    Name:          "adult_lua",
    Arguments:     []string{"age", "disabled"},
    ArgumentTypes: []string{"INTEGER", "BOOLEAN"},
    Language:      "LUA",
    Source:        "return age >= 18 and not disabled",
}
if err := registry.Register(definition); err != nil {
    log.Fatal(err)
}
```

Do not compile with `-tags luajit` when the deployment does not require Lua.
That keeps the default binary free of the CGO/LuaJIT dependency and makes the
runtime disable-by-default at build time.

## Measurement

The existing vectorized Lua benchmark was run five times per size on Linux
amd64, Go 1.26.5, and LuaJIT. The baseline was the clean pre-change commit;
the feature run used the bounded defaults. Values below are medians of the raw
samples in `BENCHMARK.md`.

| Batch | Before ns/op | After ns/op | After / before | Before B/op | After B/op | Allocs before / after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 402,637 | 407,242 | 1.01x | 16,394 | 16,394 | 2 / 2 |
| 10,000 | 4,265,219 | 3,945,631 | 0.93x | 163,868 | 163,867 | 2 / 2 |
| 100,000 | 45,643,895 | 42,736,013 | 0.94x | 1,606,966 | 1,605,851 | 12 / 10 |

The samples are noisy because the feature and baseline ran sequentially on a
shared host. There is no measured allocation regression; the large-batch
median was faster. The security budget adds a small hook cost in the smallest
case, which is acceptable for an optional CGO runtime and can be avoided by
using the default `GO` UDF path.
