package main

import "testing"

func TestCompareRunsAllowsRowsInsideRegressionBudget(t *testing.T) {
	baseline := benchmarkRun{Results: []benchmarkResult{{
		Section:  "Command feature",
		Name:     "BenchmarkCommandFeature/StringGet-32",
		NsOp:     100,
		BOp:      20,
		AllocsOp: 2,
	}}}
	current := benchmarkRun{Results: []benchmarkResult{{
		Section:  "Command feature",
		Name:     "BenchmarkCommandFeature/StringGet-32",
		NsOp:     120,
		BOp:      20,
		AllocsOp: 2,
	}}}

	findings, compared := compareRuns(current, baseline, compareOptions{MaxRegressionPct: 20})
	if compared != 1 {
		t.Fatalf("compared = %d, want 1", compared)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none", findings)
	}
}

func TestCompareRunsReportsCPURegression(t *testing.T) {
	baseline := benchmarkRun{Results: []benchmarkResult{{
		Section: "Serialization",
		Name:    "BenchmarkCommandWireJSON-32",
		NsOp:    100,
	}}}
	current := benchmarkRun{Results: []benchmarkResult{{
		Section: "Serialization",
		Name:    "BenchmarkCommandWireJSON-32",
		NsOp:    121,
	}}}

	findings, compared := compareRuns(current, baseline, compareOptions{MaxRegressionPct: 20})
	if compared != 1 {
		t.Fatalf("compared = %d, want 1", compared)
	}
	if len(findings) != 1 || findings[0].Metric != "ns/op" {
		t.Fatalf("findings = %#v, want one ns/op regression", findings)
	}
}

func TestCompareRunsOptionallyReportsMemoryRegression(t *testing.T) {
	baseline := benchmarkRun{Results: []benchmarkResult{{
		Section:  "Transport feature",
		Name:     "BenchmarkCommandTransportFeature/InProcess/StringGet-32",
		NsOp:     100,
		BOp:      100,
		AllocsOp: 1,
	}}}
	current := benchmarkRun{Results: []benchmarkResult{{
		Section:  "Transport feature",
		Name:     "BenchmarkCommandTransportFeature/InProcess/StringGet-32",
		NsOp:     100,
		BOp:      151,
		AllocsOp: 3,
	}}}

	findings, compared := compareRuns(current, baseline, compareOptions{MaxRegressionPct: 20})
	if compared != 1 {
		t.Fatalf("compared = %d, want 1", compared)
	}
	if len(findings) != 0 {
		t.Fatalf("findings without memory comparison = %#v, want none", findings)
	}
	findings, compared = compareRuns(current, baseline, compareOptions{MaxRegressionPct: 20, CompareMemory: true})
	if compared != 1 {
		t.Fatalf("compared with memory = %d, want 1", compared)
	}
	if len(findings) != 2 {
		t.Fatalf("findings with memory = %#v, want B/op and allocs/op regressions", findings)
	}
}
