# CH-004 Native `FINAL` Pushdown

`SQLFinalSourceResolver` lets a `CACHE` or `KEYS` source reconcile replacing or
collapsing rows in its native representation before SQL materialization. This
is an optional physical optimization for queries that already use explicit
`FINAL`; ordinary queries and sources without the interface keep the existing
path.

```go
type SQLFinalSourceResolver interface {
	ResolveSQLFinalSource(
		name string,
		key string,
		options SQLFinalOptions,
	) ([]Row, bool, error)
}
```

The resolver receives the validated `SQLFinalOptions` contract. Returning
`available=true` promises that the returned rows are already reconciled under
that contract. The engine still validates source field types and keeps a
separate query-local cache for reconciled rows, so repeated `FINAL` references
do not reconcile twice and a raw reference to the same source is not polluted.
Returning `available=false` falls back to the existing materialize-and-merge
implementation. Resolver errors are returned to the caller.

The interface is deliberately opt-in because the source owns the physical
index or merge state. A backend must only return `available=true` when its
snapshot, key selection, version ordering, and sign cancellation match the
callbacks supplied by the query. No new background merge, persistence, or
schema inference is enabled by this interface.

## Measurement

The focused benchmark uses 1,024 input rows, 512 logical keys, and five
samples on the repository's AMD Ryzen 9 5950X Linux/amd64 host. It compares a
resolver that materializes duplicate rows with a resolver that returns an
already-reconciled native result.

| Path | Median ns/op | B/op | Allocs/op | Latency vs fallback | Bytes vs fallback | Allocs vs fallback |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before hook, method ignored | 602,165 | 873,501 | 4,125 | 1.00x | 1.00x | 1.00x |
| After, materialized fallback | 604,168 | 873,551 | 4,126 | 1.00x | 1.00x | 1.00x |
| After, native FINAL pushdown | 262,764 | 447,714 | 2,069 | 2.30x faster | 1.95x lower | 1.99x fewer |

The small fallback difference is benchmark noise and the unavoidable optional
interface check on an explicit `FINAL` source. There is no map allocation for
normal queries or for sources that do not use native pushdown.

Raw benchmark target:

```text
make benchmark-ch004-final
```
