# MZ-42 Dataflow Dependency Graph

`hat/hatPipeline` exposes an opt-in `DataflowGraph` for runtime operator
metadata. It complements SQL `EXPLAIN` graphs: `ExplainDataflowGraph` describes
one SQL plan, while this graph can represent long-lived sources, operators,
sinks, and their invalidation relationships across pipeline runs.

## Example

```go
graph, err := hatPipeline.NewDataflowGraph(hatPipeline.DataflowGraphOptions{
	MaxNodes: 1024,
	MaxEdges: 4096,
})
if err != nil {
	return err
}
for _, node := range []hatPipeline.DataflowNode{
	{ID: "orders", Kind: "source"},
	{ID: "daily-total", Kind: "aggregate"},
	{ID: "warehouse", Kind: "sink"},
} {
	if err := graph.AddNode(node); err != nil {
		return err
	}
}
if err := graph.AddEdge(hatPipeline.DataflowEdge{From: "orders", To: "daily-total"}); err != nil {
	return err
}
if err := graph.AddEdge(hatPipeline.DataflowEdge{From: "daily-total", To: "warehouse"}); err != nil {
	return err
}

affected, err := graph.Impact("orders")
// affected is ["daily-total", "warehouse"].
```

`Snapshot()` returns sorted, detached nodes and edges for metrics or an API.
`Dependencies` and `Dependents` return direct sorted neighbors. `Impact` gives
the transitive downstream closure, and `TopologicalOrder` gives a deterministic
execution/invalidation order. `RemoveNode` also removes all incident edges.

Node IDs are trimmed and capped at 256 bytes; kinds are capped at 128 bytes.
Empty kinds become `operator`. The default limits are 4,096 nodes and 16,384
edges. Both limits are configurable, but remain finite and cannot exceed the
hard safety bounds.

## Pipeline Adapter

Existing linear pipelines can call `Pipeline.Describe()`. It returns stable
`stage-1`, `stage-2`, ... IDs, stage names, worker and queue settings, and the
stage edges without exposing process functions or changing `Pipeline.Run`.

## Tradeoffs

The graph is not installed in existing pipelines automatically, so current
execution has no new per-record work or retained graph state. Graph updates use
a write lock and cycle-check the proposed edge; the cycle check is O(nodes plus
edges) in the worst case. Snapshots, topological order, and impact queries
allocate output proportional to the requested graph or closure. Bounds prevent
untrusted metadata from growing those allocations without limit.

## Verification

```text
make test-mz42-dataflow-graph
make race-mz42-dataflow-graph
make benchmark-mz42-dataflow-graph
```

The focused tests cover deterministic snapshots, detached results, cycle and
capacity rejection, removal, nil safety, input limits, pipeline descriptions,
and concurrent graph access.
