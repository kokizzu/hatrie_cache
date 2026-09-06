# Deterministic Sampling

`hatSql.SampleSQLRows` selects a stable fraction of rows from a logical key,
seed, and fraction. It hashes the key with xxhash and a splitmix-style mixing
step, so the same key is selected consistently even when rows are reordered or
processed in separate partitions.

```go
sample, err := hatSql.SampleSQLRows(rows,
    func(row hatSql.SQLRow) string { return row["customer_id"].(string) },
    0.10,
    42,
)
```

The key callback is the sampling identity. Rows with the same key, fraction,
and seed are selected together. Returned rows retain the order of the input
batch, but selection itself does not depend on that order. Use the same seed
and key encoding on every partition when combining distributed samples.

`fraction` must be finite and in `[0, 1]`. Zero returns no rows and one copies
the input slice. A nil key callback or invalid fraction returns an error. The
function does not promise an exact row count; it provides a deterministic
probability threshold, so small inputs can deviate from the requested fraction.

The returned slice reuses the input row maps. Treat rows as read-only or clone
maps before mutation. This utility does not provide weighted sampling,
reservoir sampling, or a global exact-size guarantee.

Focused coverage is in `hat/hatSql/deterministic_sample_test.go`, including
reordered input, partitioned input, boundary fractions, invalid values, key
validation, and an allocation-reporting benchmark.
