# TG50: Importable WebAssembly Sandbox

This feature provides a public `hat/hatSandbox` package for running small,
numeric WebAssembly functions with explicit resource limits. It is inspired by
Tarantool's protected execution model, but uses the repository's existing
Wazero dependency and does not embed LuaJIT or expose host services.

## Boundary

`hatSandbox.New` rejects modules with imported functions or memories. A module
therefore cannot reach the filesystem, network, environment, process, or an
application callback through this API. The default limits are:

| Limit | Default |
| --- | ---: |
| Module bytes | 1 MiB |
| Linear memory | 256 WebAssembly pages (16 MiB) |
| Parameters per call | 16 |
| Results per call | 4 |
| Call timeout | 100 ms |

Calls are serialized per sandbox. This keeps shared module state deterministic
and avoids making callers reason about concurrent access to WebAssembly linear
memory. A caller can create multiple sandboxes when independent concurrency is
needed.

## Example

```go
options := hatSandbox.DefaultSandboxOptions()
sandbox, err := hatSandbox.New(ctx, wasmBytes, options)
if err != nil {
	return err
}
defer sandbox.Close()

values, err := sandbox.Call(ctx, "plus_one", 41)
if err != nil {
	return err
}
// values is []uint64{42} for a function returning one i64.
```

The API intentionally accepts and returns numeric WASM values only. It is a
small execution primitive, not a general plugin ABI. Host imports, WASI, and
automatic SQL registration are outside this package. The existing SQL WASM
function path remains separate.

Use a lifetime context for `New`, call `Close` when the instance is no longer
needed, and treat a timeout or cancellation as a failed execution. The
execution timeout is a bound on a call, not a guarantee that arbitrary work
will complete successfully before the deadline.

## Measured Cost

The benchmark compares a direct Go scalar increment with the reusable sandbox
call and with creating and closing a sandbox for every operation. It is a
boundary-cost measurement, not an assertion that WASM is faster than Go.
Results were measured on AMD Ryzen 9 5950X, linux/amd64, five samples per
benchmark, using `go test -bench '^BenchmarkTG50' -benchmem -count=5`.

Raw `ns/op` samples:

```text
DirectScalarBaseline  0.7214  0.7230  0.7237  0.7216  0.7340
SandboxCall        4288.0  4221.0  4004.0  4197.0  4425.0
SandboxCreateClose 185750 184831 172627 179322 175533
```

| Benchmark | Median ns/op | B/op | allocs/op | Relative CPU vs direct |
| --- | ---: | ---: | ---: | ---: |
| Direct scalar baseline | 0.7237 | 0 | 0 | 1.00x |
| Reusable sandbox call | 4,221 | 12,224 | 12 | 5,833x |
| Create, call, and close | 179,322 | 321,317 | 297 | 247,800x |

The practical rule is to compile once and reuse a sandbox for short, isolated
functions. This feature is opt-in and does not replace direct Go callbacks on
hot paths.

## Verification

The focused package checks cover successful calls, invalid modules, module-size
limits, parameter/result limits, import rejection, cancellation, idempotent
close, race detection, and `go vet`.
