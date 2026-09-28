package hatSql

import (
	"context"
	"testing"
)

func TestM052ReusableDataflowExecutorSharesMemoizedPlan(t *testing.T) {
	query, err := CompileSQLQuery("FROM VALUES (1) AS src(id) SELECT src.id")
	if err != nil {
		t.Fatal(err)
	}
	first, err := query.CompileReusableDataflow(m052DataflowExecutorBenchmarkRunner)
	if err != nil {
		t.Fatal(err)
	}
	second, err := query.CompileReusableDataflow(m052DataflowExecutorBenchmarkRunner)
	if err != nil {
		t.Fatal(err)
	}
	if query.dataflowPlan == nil || len(query.dataflowPlan.Fragments) == 0 {
		t.Fatal("reusable dataflow did not initialize the memoized plan")
	}
	if &first.plan.Fragments[0] != &second.plan.Fragments[0] || &first.plan.Fragments[0] != &query.dataflowPlan.Fragments[0] {
		t.Fatal("reusable dataflow executors did not share the memoized fragments")
	}
	for name, executor := range map[string]*SQLDataflowExecutor{"first": first, "second": second} {
		rows, err := executor.Execute(context.Background(), []SQLRow{{"id": int64(1)}})
		if err != nil {
			t.Fatalf("%s Execute() error = %v", name, err)
		}
		if len(rows) != 1 || rows[0]["id"] != int64(1) {
			t.Fatalf("%s Execute() rows = %#v, want one id row", name, rows)
		}
	}
	copyPlan := first.Plan()
	copyPlan.Fragments[0].Kind = "MUTATED"
	if first.plan.Fragments[0].Kind == "MUTATED" {
		t.Fatal("Plan() exposed the shared memoized fragment storage")
	}
}
