# M240 Raw Dataflow Explain With Exchange Topology

The existing `BuildExplainDataflowGraph` API now preserves operator structure
and exposes stage-boundary transport separately from structural edges.

## Output

`ExplainDataflowGraph` retains the existing fields:

- `Nodes` contains independent copies of the source `ExplainStep` values. The
  existing `Stage`, `Worker`, and `Workers` fields identify execution placement
  when the caller provides them.
- `Edges` continues to contain `pipeline` and `subplan` relationships. Their
  shape and size are unchanged for plans without stage metadata.
- `Exchanges` contains `ExplainDataflowExchangeEdge` values only when adjacent
  structural operators cross a stage boundary. Each exchange records source
  and destination stage, worker, and worker-group counts.

The structural relationship is retained and the exchange relationship is
additional. A consumer can therefore draw both operator flow and the
cross-stage transport without inferring exchange boundaries from indentation.
`MarshalExplainDataflowJSON` includes the optional `exchanges` array, and
`ExplainDataflowDOT` renders those relationships with an `exchange` label.
Plans with no stage change omit `exchanges`, preserving the existing JSON
shape. The feature is read-only and does not alter query execution.

Example exchange entry:

```json
{
  "from": "op1",
  "to": "op2",
  "kind": "exchange",
  "exchange": {
    "from_stage": 0,
    "to_stage": 1,
    "from_worker": 1,
    "to_worker": 0,
    "from_workers": 2,
    "to_workers": 1
  }
}
```

## Design And Cost Check

The first implementation put an `Exchange` pointer on every structural edge.
That was rejected after measurement because a graph with no exchanges grew from
`1,370` to `1,403 B/op`. The final design keeps exchange records in a separate
optional slice, so the legacy graph path stays at `1,370 B/op` and `7 allocs/op`.

Exchange-enabled graph construction adds one allocation and about `96 B/op` in
the measured four-operator fixture. This cost is proportional to emitted
exchange records and is paid only when stage metadata creates an exchange.

## Benchmark

Five `-benchmem` samples were collected per path with 1-second samples on Linux
amd64, AMD Ryzen 9 5950X. The baseline is the M239 parent commit
(`17eea223`), with the benchmark fixture copied into that worktree for a
comparable workload.

| Path | M239 median ns/op | M240 median ns/op | M240 B/op | M240 allocs/op | M240 wire bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Legacy graph without stage changes | 1,604 | 1,542 | 1,370 | 7 | n/a |
| Stage-aware graph with one exchange | 1,299 | 1,559 | 1,455 | 7 | n/a |
| JSON with one exchange | 5,005 | 5,275 | 2,329 | 9 | 722 |
| DOT with one exchange fixture | 4,948 | 4,893 | 2,726 | 33 | n/a |

Raw samples:

| Benchmark | M239 samples | M240 samples |
| --- | --- | --- |
| Stage-aware graph | 1,299; 1,206; 1,136; 1,324; 1,326 | 1,376; 1,476; 1,572; 1,589; 1,559 |
| JSON with one exchange | 5,051; 5,166; 5,005; 4,888; 4,772 | 5,275; 5,465; 5,103; 5,148; 5,366 |
| Legacy graph | 1,263; 1,716; 1,611; 1,507; 1,604 | 1,387; 1,542; 1,482; 1,650; 1,597 |
| DOT with one exchange fixture | 4,947; 4,363; 4,948; 5,265; 5,215 | 5,116; 4,686; 5,166; 4,893; 4,722 |

The extra JSON wire payload is the intended exchange topology, not hidden
execution state. Existing plans without stage boundaries retain the prior
allocation and memory profile.

## Verification

```text
make test-m240-dataflow
make race-m240-dataflow
make vet-m240-dataflow
make benchmark-m240-dataflow
```
