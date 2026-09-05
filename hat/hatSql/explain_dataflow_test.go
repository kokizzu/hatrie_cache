package hatSql_test

import (
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestBuildExplainDataflowGraphPreservesPlanStructure(t *testing.T) {
	estimatedRows := 7
	steps := []hatSql.ExplainStep{
		{Node: "SCAN", Detail: "CACHE('orders')"},
		{Node: "  SCAN", Detail: "CACHE('customers')", EstimatedRows: &estimatedRows, Lineage: []hatSql.ColumnLineage{{Output: "id", SourceFields: []string{"customer_id"}}}},
		{Node: "  FILTER", Detail: "active = true"},
		{Node: "PROJECT", Detail: "id"},
	}

	graph := hatSql.BuildExplainDataflowGraph(steps)
	if graph.Format != "hatrie-cache-explain-dataflow/v1" {
		t.Fatalf("graph format = %q", graph.Format)
	}
	if len(graph.Nodes) != 4 {
		t.Fatalf("node count = %d, want 4", len(graph.Nodes))
	}
	if graph.Nodes[1].ID != "op1" || graph.Nodes[1].Depth != 1 || graph.Nodes[1].Step.Node != "SCAN" {
		t.Fatalf("nested node = %#v", graph.Nodes[1])
	}
	if len(graph.Edges) != 3 {
		t.Fatalf("edge count = %d, want 3", len(graph.Edges))
	}
	if got := graph.Edges[0]; got.From != "op0" || got.To != "op1" || got.Kind != "subplan" {
		t.Fatalf("subplan edge = %#v", got)
	}
	if got := graph.Edges[1]; got.From != "op1" || got.To != "op2" || got.Kind != "pipeline" {
		t.Fatalf("nested pipeline edge = %#v", got)
	}
	if got := graph.Edges[2]; got.From != "op0" || got.To != "op3" || got.Kind != "pipeline" {
		t.Fatalf("outer pipeline edge = %#v", got)
	}

	graph.Nodes[1].Step.Lineage[0].SourceFields[0] = "mutated"
	*graph.Nodes[1].Step.EstimatedRows = 99
	if steps[1].Lineage[0].SourceFields[0] != "customer_id" || *steps[1].EstimatedRows != 7 {
		t.Fatal("graph shares mutable plan-step state with input")
	}

	encoded, err := hatSql.MarshalExplainDataflowJSON(steps)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"format":"hatrie-cache-explain-dataflow/v1"`) || !strings.Contains(string(encoded), `"kind":"subplan"`) {
		t.Fatalf("dataflow JSON = %s", encoded)
	}
	dot := hatSql.ExplainDataflowDOT(steps)
	if !strings.Contains(dot, `op0 -> op1`) || !strings.Contains(dot, `label="subplan"`) || !strings.Contains(dot, `op1 -> op2`) {
		t.Fatalf("dataflow DOT = %s", dot)
	}
}

func TestBuildExplainDataflowGraphHandlesEmptyInput(t *testing.T) {
	graph := hatSql.BuildExplainDataflowGraph(nil)
	if graph.Nodes == nil || graph.Edges == nil || len(graph.Nodes) != 0 || len(graph.Edges) != 0 {
		t.Fatalf("empty graph = %#v", graph)
	}
}

func BenchmarkBuildExplainDataflowGraph(b *testing.B) {
	steps := benchmarkExplainDataflowSteps()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		_ = hatSql.BuildExplainDataflowGraph(steps)
	}
}

func BenchmarkExplainDataflowDOT(b *testing.B) {
	steps := benchmarkExplainDataflowSteps()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		_ = hatSql.ExplainDataflowDOT(steps)
	}
}

func BenchmarkExplainDOTLinear(b *testing.B) {
	steps := benchmarkExplainDataflowSteps()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		_ = hatSql.ExplainDOT(steps)
	}
}

func benchmarkExplainDataflowSteps() []hatSql.ExplainStep {
	estimatedRows := 7
	return []hatSql.ExplainStep{
		{Node: "SCAN", Detail: "CACHE('orders')"},
		{Node: "  SCAN", Detail: "CACHE('customers')", EstimatedRows: &estimatedRows},
		{Node: "  FILTER", Detail: "active = true"},
		{Node: "PROJECT", Detail: "id"},
	}
}
