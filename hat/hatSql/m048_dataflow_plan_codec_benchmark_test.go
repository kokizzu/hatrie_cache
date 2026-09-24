package hatSql

import (
	"encoding/json"
	"testing"
)

var m048DataflowPlanBenchmarkSink []byte
var m048DataflowPlanBenchmarkDecoded SQLDataflowPlan

func m048DataflowPlanBenchmarkPlan() SQLDataflowPlan {
	plan := SQLDataflowPlan{
		Format:    sqlDataflowPlanFormat,
		Source:    "FROM CACHE('events') AS event WHERE event.tenant = ? SELECT event.id, event.created_at ORDER BY event.created_at DESC LIMIT 100",
		Root:      31,
		Fragments: make([]SQLDataflowFragment, 32),
	}
	for index := range plan.Fragments {
		plan.Fragments[index] = SQLDataflowFragment{
			ID:     index,
			Kind:   []string{"SCAN", "FILTER", "PROJECT", "SORT"}[index%4],
			Detail: "tenant=customer-eu-west-1;created_at desc;stable projection",
		}
		if index > 0 {
			plan.Fragments[index].Inputs = []int{index - 1}
		}
	}
	return plan
}

func BenchmarkM048DataflowPlanJSONMarshal(b *testing.B) {
	plan := m048DataflowPlanBenchmarkPlan()
	b.ReportAllocs()
	for range b.N {
		encoded, err := json.Marshal(plan)
		if err != nil {
			b.Fatal(err)
		}
		m048DataflowPlanBenchmarkSink = encoded
	}
}

func BenchmarkM048DataflowPlanJSONUnmarshal(b *testing.B) {
	encoded, err := json.Marshal(m048DataflowPlanBenchmarkPlan())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "json_bytes")
	b.ReportAllocs()
	for range b.N {
		var plan SQLDataflowPlan
		if err := json.Unmarshal(encoded, &plan); err != nil {
			b.Fatal(err)
		}
		m048DataflowPlanBenchmarkDecoded = plan
	}
}

func BenchmarkM048DataflowPlanBinaryMarshal(b *testing.B) {
	plan := m048DataflowPlanBenchmarkPlan()
	b.ReportAllocs()
	for range b.N {
		encoded, err := EncodeSQLDataflowPlan(plan)
		if err != nil {
			b.Fatal(err)
		}
		m048DataflowPlanBenchmarkSink = encoded
	}
}

func BenchmarkM048DataflowPlanBinaryUnmarshal(b *testing.B) {
	encoded, err := EncodeSQLDataflowPlan(m048DataflowPlanBenchmarkPlan())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "binary_bytes")
	b.ReportAllocs()
	for range b.N {
		plan, err := DecodeSQLDataflowPlan(encoded)
		if err != nil {
			b.Fatal(err)
		}
		m048DataflowPlanBenchmarkDecoded = plan
	}
}
