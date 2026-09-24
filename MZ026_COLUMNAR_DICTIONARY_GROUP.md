# MZ-026 Columnar Dictionary Group Keys

The columnar vector `GROUP BY` path now uses dictionary codes as its grouping
key when the source already provides a trusted dictionary for the grouped
field. Both the normal single-level loop and the existing opt-in two-level
worker loop resolve the dictionary string once when a group is first seen,
then update aggregate state through a `map[uint32]int` instead of decoding
and hashing a string for every matched row.

The fast path is admitted only when all of these conditions hold:

- the group field is dictionary-backed and its row count matches the batch;
- the dictionary was produced by a constructor that validated its codes;
- dictionary values and offsets are valid; and
- the query uses binary collation.

Unicode case-insensitive grouping, untrusted or malformed dictionaries, and
unsupported query shapes retain the existing string-key executor. The
optimization does not create a dictionary, change wire/storage formats, or
change defaults; it consumes an encoding that the source has already chosen.
Packed dictionary values and packed 1/2/4-byte codes use the same path.

## Benchmark

Command:

```text
make benchmark-ch042-columnar-dictionary-group
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture has 20,000 rows,
64 repeated dictionary values, `COUNT`, `SUM`, and `AVG`; the before path is
the string-key executor on the same dictionary batch.

| Rows | Path | Median ns/op | B/op | Allocs/op | Improvement |
| ---: | --- | ---: | ---: | ---: | --- |
| 1,024 | Before | 407,946 | 208,434 | 3,053 | 1.00x |
| 1,024 | Code-keyed | 308,160 | 174,116 | 1,067 | 1.32x faster; 1.20x lower B/op; 2.86x fewer allocations |
| 20,000 | Before | 4,542,597 | 817,213 | 41,070 | 1.00x |
| 20,000 | Code-keyed | 2,570,328 | 175,372 | 1,131 | 1.77x faster; 4.66x lower B/op; 36.31x fewer allocations |

Raw samples:

```text
before/rows_1024: 441336 412191 407946 398499 407613 ns/op; 208454/208435/208435/208433/208434 B/op; 3053 allocs/op
code-keyed/rows_1024: 301332 312527 308327 307310 308160 ns/op; 174127/174117/174115/174117/174114 B/op; 1067 allocs/op
before/rows_20000: 4542597 4440232 4418893 4574415 4822346 ns/op; 817250/817201/817213/817207/817219 B/op; 41070 allocs/op
code-keyed/rows_20000: 2557406 2570328 2458889 2576688 2573631 ns/op; 175389/175382/175372/175368/175359 B/op; 1131 allocs/op
```

The code-keyed state grows only as matching groups are discovered; it does not
preallocate for the full dictionary cardinality. The benchmark excludes source
dictionary construction, so it measures query execution rather than the cost
of choosing or building the low-cardinality representation.
