package hatSql

import "testing"

type m049BaselineStep struct {
	id        string
	object    string
	action    string
	dependsOn int
}

func BenchmarkM049Baseline(b *testing.B) {
	steps := [...]m049BaselineStep{
		{id: "create-audit", object: "audit", action: "create"},
		{id: "create-table", object: "orders", action: "create"},
		{id: "create-view", object: "orders_view", action: "create", dependsOn: 1},
		{id: "grant-read", object: "orders_view", action: "grant", dependsOn: 2},
	}
	b.ReportAllocs()
	checksum := 0
	for i := 0; i < b.N; i++ {
		for _, step := range steps {
			checksum += len(step.id) + len(step.object) + len(step.action) + step.dependsOn
		}
	}
	if checksum == 0 {
		b.Fatal("unexpected zero checksum")
	}
}
