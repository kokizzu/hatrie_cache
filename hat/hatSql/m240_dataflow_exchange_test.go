package hatSql

import (
	"strings"
	"testing"
)

func TestM240ExplainDataflowGraphReportsExchangeTopology(t *testing.T) {
	steps := []ExplainStep{
		{Node: "SCAN", Detail: "CACHE('orders')", Stage: 0, Worker: 0, Workers: 2},
		{Node: "FILTER", Detail: "region = 'apac'", Stage: 0, Worker: 1, Workers: 2},
		{Node: "AGGREGATE", Detail: "count(*)", Stage: 1, Worker: 0, Workers: 1},
		{Node: "PROJECT", Detail: "count", Stage: 1, Worker: 0, Workers: 1},
	}

	graph := BuildExplainDataflowGraph(steps)
	var exchange *ExplainDataflowExchangeEdge
	for index := range graph.Exchanges {
		if graph.Exchanges[index].Kind == "exchange" {
			exchange = &graph.Exchanges[index]
			break
		}
	}
	if exchange == nil || exchange.From != "op1" || exchange.To != "op2" {
		t.Fatalf("exchange edge = %#v, want op1 -> op2 with metadata", exchange)
	}
	metadata := exchange.Exchange
	if metadata.FromStage != 0 || metadata.ToStage != 1 || metadata.FromWorker != 1 || metadata.ToWorker != 0 || metadata.FromWorkers != 2 || metadata.ToWorkers != 1 {
		t.Fatalf("exchange metadata = %#v", metadata)
	}

	encoded, err := MarshalExplainDataflowJSON(steps)
	if err != nil {
		t.Fatalf("MarshalExplainDataflowJSON() error = %v", err)
	}
	if !strings.Contains(string(encoded), `"kind":"exchange"`) || !strings.Contains(string(encoded), `"from_stage":0`) || !strings.Contains(string(encoded), `"to_stage":1`) {
		t.Fatalf("dataflow JSON = %s", encoded)
	}
	dot := ExplainDataflowDOT(steps)
	if !strings.Contains(dot, `label="exchange"`) {
		t.Fatalf("dataflow DOT = %q, want exchange edge", dot)
	}
}

func TestM240ExplainDataflowGraphKeepsLegacyEdgesWithoutStages(t *testing.T) {
	graph := BuildExplainDataflowGraph([]ExplainStep{
		{Node: "SCAN", Detail: "VALUES"},
		{Node: "FILTER", Detail: "id > 1"},
	})
	if len(graph.Edges) != 1 || graph.Edges[0].Kind != "pipeline" || len(graph.Exchanges) != 0 {
		t.Fatalf("legacy graph = %#v, want one pipeline edge without exchange", graph)
	}
}
