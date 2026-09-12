# C214: Bounded WebAssembly UDFs

SQL `LANGUAGE WASM` functions now use a bounded Wasm memory by default. Execution deadlines are opt-in because wazero's interruptible execution instrumentation has a measurable cost on the normal scalar path.

## Configuration

```go
registry := hatCache.NewSQLFunctionRegistryWithOptions(hatCache.SQLFunctionRegistryOptions{
	WASMMemoryLimitPages: 512, // 32 MiB per Wasm memory; zero uses 256 pages.
	WASMExecutionTimeout: 500 * time.Millisecond,
})
```

- `WASMMemoryLimitPages` is measured in 64 KiB Wasm pages. `0` uses 256 pages, or 16 MiB, per Wasm memory.
- `WASMExecutionTimeout` applies to one `EvaluateSQLFunction` batch. A positive value enables wazero interruption; zero or a negative value keeps the fast path and leaves execution deadlines disabled.
- When an interruptible call reaches its deadline, wazero closes the Wasm module. The function must be re-registered or the registry reloaded before it can run again.
- Increasing the memory limit does not change the SQL ABI. It only permits modules that declare or grow larger memories.

The default protects memory growth without changing the existing hot path. Deadline protection should be enabled for untrusted or potentially non-terminating modules, and its cost should be included in capacity planning.

## Benchmark

Measured on an AMD Ryzen 9 5950X, Linux amd64, Go, five samples per case, `-benchtime=1s -benchmem`. The baseline is `origin/master` before C214; the bounded result is the same 1k/10k/100k `plus_one` batch benchmark.

| Batch | Baseline | Bounded memory | Baseline / bounded | Allocations |
|---:|---:|---:|---:|---:|
| 1,000 | 67,172 ns/op | 67,693 ns/op | 0.99x | 2,747 vs 2,747 |
| 10,000 | 701,657 ns/op | 693,269 ns/op | 1.01x | 29,747 vs 29,747 |
| 100,000 | 7,173,396 ns/op | 6,734,508 ns/op | 1.07x | 299,748 vs 299,748 |

The small timing differences are within normal benchmark variance; allocation counts are unchanged. The explicit interruptible 10,000-row benchmark measured 5,374,607 ns/op, 2,162,263 B/op, and 59,753 allocs/op versus 693,269 ns/op, 401,813 B/op, and 29,747 allocs/op for the default path: about 7.75x CPU time, 5.38x bytes, and 2.01x allocations. That tradeoff is why deadlines are not enabled by default.

Run the comparison with:

```text
make benchmark-c214
```

## Verification

The focused tests cover default policy, rejection of modules over the memory limit, successful execution at the limit, and interruption of an infinite loop:

```text
make test-c214-wasm-limits
```
