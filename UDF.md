# SQL UDF runtimes

`CREATE FUNCTION` initially supports `LANGUAGE GO`, a restricted Go-like,
single-return-expression language. It is compiled to a small native Go
expression tree: source code is **not** passed to the Go compiler or allowed to
load packages, allocate, loop, mutate state, touch files/network, or call host
functions.

```sql
CREATE FUNCTION eligible(age INTEGER, score INTEGER)
LANGUAGE GO AS 'return age > 10 && score < 9';
```

Supported operators are `==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`, and the
SQL null checks `IS NULL` and `IS NOT NULL`. Arguments are checked before any
row is evaluated. A bad argument or source reference is reported with a
Rust-style source location, for example:

```text
error: argument "age" expects INTEGER, got TEXT
 --> function eligible:1:8
  |
1 | return age > 10 && score < 9
  |        ^
```

## Benchmark and decision (2026-08-22)

The candidate predicate was `age > 10 && score < 9` on this Linux amd64 host,
using Go 1.26.5 and LuaJIT 2.1. The LuaJIT figures include the important data
transfer cost where stated. They are not a claim that every real UDF has the
same profile.

| Runtime / bridge | Result |
| --- | ---: |
| Native Go loop | 0.46–0.52 ns/row |
| Experimental small Go callback VM | 11.65–12.39 ns/row |
| Implemented `LANGUAGE GO` evaluator, including result vector (1K / 10K / 100K rows) | 91–95 / 909–980 / 8,403–9,100 ns per batch (about 84–98 ns/row) |
| LuaJIT loop, data already in Lua | 4.07–4.46 ns/row |
| LuaJIT, positional Go-to-Lua input and Lua-to-Go results (1K rows) | 211.5 ns/row |
| Same LuaJIT bridge (10K rows) | 219.7 ns/row |
| Same LuaJIT bridge (100K rows) | 213.5 ns/row |
| LuaJIT, object-shaped rows (1K / 10K / 100K) | 396 / 408 / 464 ns/row |
| Wazero scalar WebAssembly call | 55.2 ns/call |
| Wasmtime-go scalar WebAssembly call | 1,993 ns/call |
| Wasmer-go scalar WebAssembly call | crashed in its cgo call on this host |

The original LuaJIT vector result (~3 ns/row) excluded marshaling: the input
table was created before timing. Once actual input and result copying is timed,
LuaJIT is over twice as slow as the implemented Go evaluator for this actual
bridge, and it requires a platform C dependency. Batch size does not amortize
the copying away, so it would be misleading to make LuaJIT the default SQL UDF
engine.

Wazero is the selected WebAssembly runtime if a future `LANGUAGE WASM` binary
ABI is added: it is much faster than the two cgo alternatives and has no cgo
dependency. WebAssembly itself is not JavaScript; a `LANGUAGE JS` feature would
also need a JavaScript-to-Wasm toolchain/runtime (for example QuickJS/Javy), a
stable vector ABI, and separate diagnostics. It is deliberately not advertised
as JavaScript support yet.

`go-joker` was also evaluated as inspiration, not embedded. Its typed IR and
native/Wasm fast paths are useful design references, but it is a full
Clojure-like interpreter with a large language/runtime surface. Its own README
notes that its CLBG benchmarks target interpreter bottlenecks rather than
representative applications. Embedding it would add substantially more
semantics and attack surface than a bounded SQL scalar UDF requires.

The implementation therefore uses the native restricted `GO` expression path
now. Fixed SQL built-ins remain ordinary native Go functions. Future work can
add a separately benchmarked, explicitly binary `WASM` ABI without changing
the safety or performance characteristics of `GO` UDFs.

## References

- [Go-Joker architecture and benchmark caveat](https://github.com/rcarmo/go-joker)
- [Wazero compiler/runtime](https://github.com/wazero/wazero)
- [Wasmtime Go binding](https://github.com/bytecodealliance/wasmtime-go)
- [Wasmer Go binding](https://github.com/wasmerio/wasmer-go)
