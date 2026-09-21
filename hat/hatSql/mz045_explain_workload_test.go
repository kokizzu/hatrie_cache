package hatSql

import "testing"

const mz045ExplainWorkloadQuery = `
FROM VALUES (1), (2), (3) AS users(id)
JOIN VALUES (1), (2), (3) AS regions(id) ON users.id = regions.id
JOIN VALUES (1), (2), (3) AS accounts(id) ON users.id = accounts.id
JOIN VALUES (1), (2), (3) AS events(id) ON users.id = events.id
SELECT users.id
`

func TestMZ045ExplainWorkloadPreservesMultiJoinPlan(t *testing.T) {
	query, err := parseSQLQueryTemplate(mz045ExplainWorkloadQuery)
	if err != nil {
		t.Fatal(err)
	}
	steps := sqlExplainSteps(query)
	if len(steps) < 4 {
		t.Fatalf("explain steps = %d, want at least scan plus three joins", len(steps))
	}
	if steps[0].Node != "SCAN" {
		t.Fatalf("first explain node = %q, want SCAN", steps[0].Node)
	}
	joinSteps := 0
	for _, step := range steps {
		if step.Node == "EQUALITY JOIN" || step.Node == "JOIN" {
			joinSteps++
		}
	}
	if joinSteps != 3 {
		t.Fatalf("join explain steps = %d, want 3", joinSteps)
	}
}

var mz045ExplainWorkloadBenchmarkSink []SQLExplainStep

func BenchmarkMZ045ExplainWorkload(b *testing.B) {
	query, err := parseSQLQueryTemplate(mz045ExplainWorkloadQuery)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mz045ExplainWorkloadBenchmarkSink = sqlExplainSteps(query)
	}
}
