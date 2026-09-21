# TT-025 Online Uniqueness Validation

`MaterializedSource.BuildUniqueIndex(field)` builds an equality index in the
same online, generation-checked style as `BuildSecondaryIndex`, but validates
the snapshot before publishing it. A duplicate non-`NULL` key returns
`ErrMaterializedSourceUniqueIndexViolation`; the failed build does not install
an index or change the source rows.

After publication, `Insert` checks the maintained posting map while holding
the source write lock. A duplicate is rejected before the row, postings, or
generation are advanced. Multiple `NULL` values are allowed, matching normal
SQL unique-index behavior. The feature is opt-in; ordinary indexed columns and
all non-indexed sources retain their existing write path.

## Measurement

Command: `make benchmark-tt025-online-uniqueness`.

The build fixture contains 10,000 rows with 10,000 distinct string keys. The
insert fixture seeds 1,024 rows, measures successful inserts, and resets the
fixture outside the timed region every 1,024 operations. Results are five
samples on Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Ordinary index build, unique-key fixture | 2,923,835 | 1,181,935 | 20,036 | baseline |
| Unique index build, same fixture | 2,566,790 | 1,181,925 | 20,036 | 1.14x lower measured CPU; within run variance, not claimed as a speedup |
| Ordinary indexed insert | 1,723 | 956 | 10 | baseline |
| Unique indexed insert | 1,914 | 979 | 11 | 1.11x slower; +23 B and +1 allocation |

The retained unique-index map is the same posting structure used by an
ordinary equality index. The write overhead is therefore accepted only when a
caller explicitly requests uniqueness; it is not enabled by default.
