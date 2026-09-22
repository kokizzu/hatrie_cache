# C237 Explain Projection Selection

C237 adds explain metadata for simple single-source columnar scans. The plan
identifies the fields selected from storage, separates predicate fields from
output fields, and reports an estimated byte cost for the selected columnar
batch. This follows the ClickHouse-style goal of making column pruning and
read cost visible to operators.

Example `EXPLAIN` or `EXPLAIN ANALYZE` plan fragment:

```json
{
  "node": "SCAN",
  "projection": {
    "kind": "columnar",
    "fields": ["id", "payload"],
    "predicate_fields": ["id"],
    "output_fields": ["payload"],
    "estimated_read_bytes": 320,
    "estimated_read_bytes_per_row": 54
  }
}
```

`fields` is the union requested from the resolver. `predicate_fields` are
needed to evaluate the filter; `output_fields` are returned by the SELECT.
`estimated_read_bytes` is calculated from the physical representation returned
by the columnar resolver, so it includes compact numeric, dictionary, packed,
and plain-column storage available in that batch. It is an estimate of the
selected batch, not a claim about remote transport, filesystem compression,
or a full-table scan that was not requested.

The metadata is emitted for the ordinary simple columnar scan path. Unsupported
query shapes and generic row sources omit it rather than presenting an
unverified estimate. `EXPLAIN ANALYZE` attaches it to the executed
`COLUMNAR SCAN`; plain `EXPLAIN` attaches it to the planned `SCAN` when the
resolver can provide the batch metadata.

## Cost

The feature is explain-only. Normal queries do not build projection metadata.
The measured EXPLAIN workload added approximately 6% bytes and 5% allocations
to carry the nested metadata in the result, while median CPU remained within
benchmark noise. This cost is bounded by the number of selected fields and is
paid only when requesting EXPLAIN output.

See the raw comparison in [BENCHMARK.md](BENCHMARK.md#c237-explain-projection-selection-and-io-estimate).

## Verification

```text
make test-c237-projection
make race-c237-projection
make vet-c237-projection
make benchmark-c237-before-after
```
