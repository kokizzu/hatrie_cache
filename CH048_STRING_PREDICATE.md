# CH-048 String Predicate Kernel

## Scope

Plain columnar batches now use a direct binary string comparison kernel for
`=`, `!=`, `<>`, `<`, `<=`, `>`, and `>=` when the predicate is a field against
a string literal. Literal-left comparisons are normalized before execution.

The kernel validates the physical column before use. It is skipped for packed,
dictionary, nested, map, mixed-type, prepared-offset, cached, or non-binary
collation layouts, so those paths retain their existing semantics. SQL NULLs
remain non-matches for ordinary comparisons.

## Measurement

Command:

```text
make benchmark-ch048
```

The benchmark evaluates a 4,096-row plain string column with a binary
`name >= 'm'` predicate. Each benchmark iteration scans all 4,096 rows. Five
runs were collected before and after the change on an AMD Ryzen 9 5950X.

| Version | Median ns/op per 4,096 rows | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Before direct kernel | 485,616 | 0 | 0 | 1.00x |
| Direct kernel | 31,013 | 0 | 0 | 15.66x faster |

Raw baseline runs:

```text
508461 ns/op   0 B/op   0 allocs/op
479855 ns/op   0 B/op   0 allocs/op
485616 ns/op   0 B/op   0 allocs/op
483132 ns/op   0 B/op   0 allocs/op
491463 ns/op   0 B/op   0 allocs/op
```

Raw direct-kernel runs:

```text
32239 ns/op    0 B/op   0 allocs/op
31650 ns/op    0 B/op   0 allocs/op
31013 ns/op    0 B/op   0 allocs/op
30689 ns/op    0 B/op   0 allocs/op
30492 ns/op    0 B/op   0 allocs/op
```

The change improves CPU time without changing allocation behavior or adding a
retained column representation. The one-time validation scan is included in
matcher construction, outside the repeated row predicate loop; unsafe layouts
fall back without changing the public query result.

## Verification

```text
make test-ch048
make test-ch048-package
make race-ch048
make vet-ch048
make format-ch048
```

Coverage includes literal-left normalization, all-string ordering and NULL
behavior, numeric literal rejection, non-binary collation rejection, and the
full `hatSql` package.
