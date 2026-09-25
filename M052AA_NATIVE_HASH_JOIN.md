# M052aa: Automatic Native Equality Hash Join

This feature adopts a narrow dataflow-runtime idea from ClickHouse and
Materialize: lower a common equality join into a typed batch hash index before
the general SQL executor needs to materialize and re-evaluate the full plan.

## Scope

The automatic path is enabled only for all of these conditions:

- one `INNER JOIN` with a binary field equality such as `l.k = r.k`;
- `CACHE` or `KEYS` sources on both sides;
- ordinary row resolvers, with no columnar, streaming, lookup, index, or
  ordered resolver contract;
- no typed source declarations, subqueries, CTEs, unions, ordering, grouping,
  windows, limits, `HAVING`, `PREWHERE`, `QUALIFY`, `FINAL`, or lateral source;
- scalar `WHERE` and projection expressions only.

The right side uses the existing typed `sqlJoinHashIndex`, which keeps numeric,
string, and boolean keys in separate representations and skips `NULL` keys.
Matched rows retain left input order and right insertion order. The existing
executor remains authoritative for every shape outside this boundary.

`SQLQueryOptions.DisableNativeDataflow` disables the path. Index, spill,
frontier, memory-tracker, runtime-filter, and advanced planner options also
force the established executor so their existing accounting and semantics are
unchanged.

## Safety And Budgets

The path checks the context while building and probing, applies `MaxRows` and
`MaxIntermediateRows` to both sources and joined rows, applies `MaxJoinWork`,
and keeps the existing `MaxResultBytes` check. It does not alter input rows.

## Benchmark

Command:

```text
make benchmark-m052aa-native-join
```

Linux/amd64, AMD Ryzen 9 5950X, five samples per benchmark. The fixture has
4,096 left rows, 256 right rows, 128 integer keys, and 8,192 matching output
rows. The fallback and native paths were measured in the same process run.

### Raw Samples

```text
fallback:
8818422 ns/op, 14338236 B/op, 70711 allocs/op
8330547 ns/op, 14338191 B/op, 70711 allocs/op
8149555 ns/op, 14338191 B/op, 70711 allocs/op
8265485 ns/op, 14338194 B/op, 70711 allocs/op
8550700 ns/op, 14338191 B/op, 70711 allocs/op

native:
5477993 ns/op, 7870949 B/op, 57618 allocs/op
5475030 ns/op, 7870950 B/op, 57618 allocs/op
5512240 ns/op, 7870954 B/op, 57618 allocs/op
5445242 ns/op, 7870949 B/op, 57618 allocs/op
5829956 ns/op, 7870951 B/op, 57618 allocs/op
```

### Median

| Path | ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing hash-join fallback | 8,330,547 | 14,338,191 | 70,711 | 1.00x |
| Automatic native equality join | 5,477,993 | 7,870,950 | 57,618 | **1.52x faster; 1.82x lower bytes; 1.23x fewer allocations** |

The native path reduces time by about 34.2%, allocation bytes by about 45.1%,
and allocation count by about 18.5% for this workload. It still materializes
the result and is intentionally not used for spillable or specialized source
paths; those are the explicit tradeoffs for preserving existing behavior.

## Verification

```text
make format-m052aa-native-join
make test-m052aa-native-join
make benchmark-m052aa-native-join
```
