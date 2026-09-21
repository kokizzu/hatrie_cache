# CH-042 Storage-Aware Sampling

SQL `TABLESAMPLE` remains compatible with the existing materialized fallback,
but a source can now implement `hatSql.SampledSourceResolver` to select rows in
storage before the executor materializes the full source.

## Contract

Implement `ResolveSQLSampledSource` and return:

- sampled rows in their source order;
- `inputRows`, the exact full-source cardinality before sampling;
- `available=true` only when the storage result is complete and correct for the
  requested `SQLSampleRequest`.

`SQLSampleRequest.Mode` is `BERNOULLI` or `RESERVOIR`, `Value` is the parsed
percentage or row count, and `Seed` is the repeatable seed. Returning
`available=false` uses the existing full-source materialization and sampling
path. The SQL executor still applies `WHERE`, projection, limits, joins, and
other SQL semantics after the sampled source is returned.

The full cardinality is required so `MaxRows` continues to protect the source
before sampling. Sampled rows are not inserted into the complete-source cache.

```go
type sampledSource struct { /* storage-owned state */ }

func (s *sampledSource) ResolveSQLSampledSource(
	name, key string,
	sample hatSql.SQLSampleRequest,
) ([]hatSql.Row, int, bool, error) {
	rows, total, err := s.readSample(name, key, sample)
	if err != nil {
		return nil, 0, false, err
	}
	return rows, total, true, nil
}
```

This is intentionally an optional resolver capability. It does not invent a
sampling key or physical index for a storage engine; the adapter owns stable
row identity, partition selection, and the exact repeatable semantics.

## Measurement

Five samples were run on Linux/amd64, AMD Ryzen 9 5950X, with 10,000 source
rows and `TABLESAMPLE BERNOULLI (10) REPEATABLE (7)`. The baseline was run in
a detached worktree at the parent commit with the legacy full-materialization
path. The final benchmark used the same query and compared the legacy resolver
with a storage-aware resolver returning the deterministic sample and full
cardinality.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative to parent legacy |
| --- | --- | ---: | ---: | ---: | ---: |
| Parent legacy baseline | 2,679,974; 2,583,561; 2,617,150; 2,633,838; 2,577,381 | 2,617,150 | 3,986,637 | 22,034 | 1.00x |
| Current legacy control | 2,638,259; 2,593,018; 2,428,171; 2,692,181; 2,729,824 | 2,638,259 | 3,986,609 | 22,033 | 1.01x |
| Storage-aware sample | 304,133; 306,241; 300,647; 318,661; 314,770 | 306,241 | 521,437 | 2,024 | 8.54x faster |

The final row's allocation column is `2,024`; the relative resource results are
8.61x faster, 7.64x lower B/op, and 10.89x fewer allocations. The benchmark
commands are:

```text
make benchmark-ch042-storage-sample-before
make benchmark-ch042-storage-sample
```
