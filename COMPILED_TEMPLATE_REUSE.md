# Immutable Compiled SQL Template Reuse

`CompileSQLQuery` parses a SQL statement once and returns a handle that can be
executed repeatedly. For the common case of a static statement with no
parameters and default options, the handle now reuses its immutable rewritten
AST instead of deep-cloning and rewriting it on every execution.

## Eligibility

The read-only path is selected automatically when all of these conditions hold:

- the compiled SQL contains no parameters, including nested queries, joins,
  CTEs, grouping, ordering, windows, and `LIMIT BY` expressions;
- the call has no execution parameters;
- collation and optimizer options are unset; and
- no index hint is supplied.

Parameterized statements and calls with execution-local options retain the
existing clone, bind, rewrite, and validation path. This keeps parameter
binding, custom collation, optimizer rules, and index hints isolated per call.

`ExecuteRows` uses the same selection. The immutable template is safe for
concurrent readers because compilation finishes all rewrites before publishing
the handle, and the direct path does not mutate the AST.

## Benchmark

Run the repeatable benchmark with:

```text
make benchmark-m071-compiled-template-reuse
```

The workload executes a static compiled query over a 16-row `VALUES` source.
`clone_control` uses the same compiled template but forces the old per-call
clone path; `read_only_handle` uses the public compiled-handle API with default
options. Five samples were collected with `-benchmem -count=5` on Linux
amd64/AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Clone control | 10,585 | 17,552 | 111 |
| Read-only compiled handle | 8,111 | 13,696 | 85 |
| Improvement | 1.31x faster | 1.28x lower | 1.31x lower |

Raw samples:

```text
clone_control: 10739, 10585, 10530, 10498, 10750 ns/op; 17552 B/op; 111 allocs/op
read_only_handle: 8121, 8113, 8111, 8084, 8020 ns/op; 13696 B/op; 85 allocs/op
```

The benchmark measures steady-state execution and excludes the one-time
compile. Compilation now performs the immutable rewrite/collation preparation
once; dynamic or parameterized calls do not receive this shortcut and retain
the prior behavior.
