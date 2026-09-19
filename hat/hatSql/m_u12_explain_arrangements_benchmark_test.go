//go:build mu12 || mu12baseline

package hatSql

import "testing"

func benchmarkMU12ExplainSteps() []ExplainStep {
	estimatedRows := 1000
	return []ExplainStep{
		{Node: "SCAN", Detail: "orders", EstimatedRows: &estimatedRows},
		{Node: "FILTER", Detail: "region = 'apac'"},
		{Node: "AGGREGATE", Detail: "region"},
		{Node: "PROJECT", Detail: "region,count"},
	}
}

func BenchmarkMU12ExplainJSONBaseline(b *testing.B) {
	steps := benchmarkMU12ExplainSteps()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := MarshalExplainJSON(steps); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU12BuildExplainArrangementPlan(b *testing.B) {
	steps := benchmarkMU12ExplainSteps()
	annotations := []ExplainArrangementAnnotation{
		{StepIndex: 0, Key: "orders_scan", Action: TypedTableArrangementAdvisorReuse, References: 2, Shared: true, EstimatedBytes: 4096},
		{StepIndex: 2, Key: "orders_by_region", Action: TypedTableArrangementAdvisorHydrateThenReuse, Stale: true, EstimatedBytes: 8192},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := BuildExplainArrangementPlan(steps, annotations); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU12MarshalExplainArrangementJSON(b *testing.B) {
	steps := benchmarkMU12ExplainSteps()
	annotations := []ExplainArrangementAnnotation{
		{StepIndex: 0, Key: "orders_scan", Action: TypedTableArrangementAdvisorReuse, References: 2, Shared: true, EstimatedBytes: 4096},
		{StepIndex: 2, Key: "orders_by_region", Action: TypedTableArrangementAdvisorHydrateThenReuse, Stale: true, EstimatedBytes: 8192},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := MarshalExplainArrangementJSON(steps, annotations); err != nil {
			b.Fatal(err)
		}
	}
}
