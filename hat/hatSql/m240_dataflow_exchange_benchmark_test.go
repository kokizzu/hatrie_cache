package hatSql

import "testing"

var (
	m240DataflowExchangeBenchmarkSink ExplainDataflowGraph
	m240DataflowExchangeBenchmarkWire []byte
)

func m240DataflowExchangeBenchmarkSteps() []ExplainStep {
	return []ExplainStep{
		{Node: "SCAN", Detail: "CACHE('orders')", Stage: 0, Worker: 0, Workers: 2},
		{Node: "FILTER", Detail: "region = 'apac'", Stage: 0, Worker: 1, Workers: 2},
		{Node: "AGGREGATE", Detail: "count(*)", Stage: 1, Worker: 0, Workers: 1},
		{Node: "PROJECT", Detail: "count", Stage: 1, Worker: 0, Workers: 1},
	}
}

func BenchmarkM240ExplainDataflowGraphExchange(b *testing.B) {
	steps := m240DataflowExchangeBenchmarkSteps()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		m240DataflowExchangeBenchmarkSink = BuildExplainDataflowGraph(steps)
	}
}

func BenchmarkM240MarshalExplainDataflowJSONExchange(b *testing.B) {
	steps := m240DataflowExchangeBenchmarkSteps()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		encoded, err := MarshalExplainDataflowJSON(steps)
		if err != nil {
			b.Fatal(err)
		}
		m240DataflowExchangeBenchmarkWire = encoded
	}
	b.ReportMetric(float64(len(m240DataflowExchangeBenchmarkWire)), "wire_bytes")
}
