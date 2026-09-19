# M-U12: Arrangement Metadata in Explain Output

M-U12 adds an opt-in enriched explain document for plans that have arrangement
reuse advice. It exposes, per operator:

- the stable arrangement key;
- the reuse action (`create`, `reuse`, or `hydrate_then_reuse`);
- reference/shared state and stale status;
- the arrangement advisor's incremental memory estimate; and
- the existing `ExplainStep` cardinality estimate.

The default `ExplainStep`, `ExplainDocument`, and SQL `EXPLAIN` output remain
unchanged. This preserves existing JSON consumers while allowing a planner or
diagnostic endpoint to add metadata when it has an arrangement catalog.

## Usage

First produce the normal explain plan, then enrich it using annotations whose
`StepIndex` refers to the exact zero-based position in that plan:

```go
result, err := hatSql.ExecuteSQLQuery(
	"EXPLAIN FROM CACHE('orders') SELECT region, count(*) GROUP BY region",
	resolver,
)
if err != nil {
	return err
}

annotations := []hatSql.ExplainArrangementAnnotation{
	{
		StepIndex:      0,
		Key:            "orders_by_region",
		Action:         hatSql.TypedTableArrangementAdvisorReuse,
		References:     2,
		Shared:         true,
		EstimatedBytes: 4096,
	},
}
plan, err := hatSql.BuildExplainArrangementPlan(result.Plan, annotations)
if err != nil {
	return err
}

payload, err := hatSql.MarshalExplainArrangementJSON(result.Plan, annotations)
```

`BuildExplainArrangementPlan` validates indices, rejects duplicate annotations,
rejects unknown actions and negative reference counts, bounds the annotation
count at `MaxExplainArrangementAnnotations`, and deep-copies the original
steps. It never executes SQL or reads a resolver. Annotation order does not
change the result.

`ExplainArrangementDOT` renders the same enriched plan for Graphviz. All plan
and arrangement text is emitted as escaped labels.

## JSON shape

The output format is `hatrie-cache-explain-arrangements/v1`:

```json
{
  "format": "hatrie-cache-explain-arrangements/v1",
  "steps": [
    {
      "step": {
        "node": "SCAN",
        "detail": "orders",
        "estimated_rows": 1000
      },
      "arrangement": {
        "key": "orders_by_region",
        "action": "reuse",
        "references": 2,
        "shared": true,
        "estimated_bytes": 4096
      }
    }
  ]
}
```

The caller owns mapping advisor recommendations to plan positions. This is
intentional: the generic SQL parser cannot infer a typed-table arrangement key
from arbitrary SQL text without a catalog and schema context.

## Benchmark and tradeoff

Five-run medians on the AMD Ryzen 9 5950X for a four-step plan:

| Case | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing `MarshalExplainJSON` baseline | 1,227 before; 1,147 same-run comparison | 288 | 2 |
| Build enriched plan, two annotations | 577.8 | 1,032 | 4 |
| Marshal enriched plan, two annotations | 2,348 | 1,594 | 6 |

The enriched format intentionally costs more because it deep-copies the plan
and emits a second metadata object per annotated operator. It is appropriate for
EXPLAIN/diagnostic responses, not a per-row execution path. Existing explain
calls pay no cost because they do not call the enrichment functions.

## Verification

```text
make test-mu12
make benchmark-mu12-baseline
make benchmark-mu12
make verify-mu12
```
