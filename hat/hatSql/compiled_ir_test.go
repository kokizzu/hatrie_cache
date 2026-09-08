package hatSql

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

func TestCompiledSQLQueryDataflowIRIsStableAndNonMutating(t *testing.T) {
	const source = "SELECT name FROM CACHE('users') WHERE score >= $1 ORDER BY name LIMIT 2"
	compiled, err := CompileSQLQuery(source)
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	wantKinds := []string{"SCAN", "FILTER", "PROJECT", "SORT", "LIMIT"}
	first := compiled.Dataflow()
	if first.Source != source || first.Root != len(first.Nodes)-1 || len(first.Nodes) != len(wantKinds) {
		t.Fatalf("Dataflow() = %#v, want source/root/nodes %q", first, wantKinds)
	}
	for index, wantKind := range wantKinds {
		if first.Nodes[index].ID != index || first.Nodes[index].Kind != wantKind {
			t.Fatalf("Dataflow node %d = %#v, want id=%d kind=%q", index, first.Nodes[index], index, wantKind)
		}
		if index == 0 && len(first.Nodes[index].Inputs) != 0 {
			t.Fatalf("scan inputs = %v, want empty", first.Nodes[index].Inputs)
		}
		if index > 0 && !reflect.DeepEqual(first.Nodes[index].Inputs, []int{index - 1}) {
			t.Fatalf("node %d inputs = %v, want [%d]", index, first.Nodes[index].Inputs, index-1)
		}
	}
	first.Nodes[0].Inputs = append(first.Nodes[0].Inputs, 99)
	second := compiled.Dataflow()
	if !reflect.DeepEqual(second, compiled.Dataflow()) || len(second.Nodes[0].Inputs) != 0 {
		t.Fatalf("Dataflow() was mutated through returned value: first=%#v second=%#v", first, second)
	}

	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"name": "Ada", "score": int64(7)}, {"name": "Lin", "score": int64(3)}}, nil
	})
	result, err := compiled.Execute(context.Background(), resolver, []interface{}{int64(5)}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("compiled.Execute() error = %v", err)
	}
	if want := []Row{{"name": "Ada"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("compiled.Execute() rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCompiledSQLQueryLowerDataflowReturnsReusableImmutablePlan(t *testing.T) {
	compiled, err := CompileSQLQuery("SELECT name FROM CACHE('users') WHERE score >= $1 ORDER BY name LIMIT 2")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	plan := compiled.LowerDataflow()
	if plan.Format != "hatrie-cache-sql-dataflow/v1" {
		t.Fatalf("plan format = %q, want versioned SQL dataflow format", plan.Format)
	}
	if plan.Source != compiled.Source() {
		t.Fatalf("plan source = %q, want %q", plan.Source, compiled.Source())
	}
	wantKinds := []string{"SCAN", "FILTER", "PROJECT", "SORT", "LIMIT"}
	if len(plan.Fragments) != len(wantKinds) {
		t.Fatalf("fragment count = %d, want %d", len(plan.Fragments), len(wantKinds))
	}
	for index, wantKind := range wantKinds {
		fragment := plan.Fragments[index]
		if fragment.ID != index || fragment.Kind != wantKind {
			t.Fatalf("fragment[%d] = %#v, want id=%d kind=%q", index, fragment, index, wantKind)
		}
		if index == 0 {
			if len(fragment.Inputs) != 0 {
				t.Fatalf("root fragment inputs = %#v, want none", fragment.Inputs)
			}
		} else if len(fragment.Inputs) != 1 || fragment.Inputs[0] != index-1 {
			t.Fatalf("fragment[%d] inputs = %#v, want [%d]", index, fragment.Inputs, index-1)
		}
	}
	if plan.Root != len(wantKinds)-1 {
		t.Fatalf("plan root = %d, want %d", plan.Root, len(wantKinds)-1)
	}
	plan.Fragments[0].Kind = "MUTATED"
	plan.Fragments[1].Inputs[0] = 99
	second := compiled.LowerDataflow()
	if second.Fragments[0].Kind != "SCAN" || second.Fragments[1].Inputs[0] != 0 {
		t.Fatalf("lowered plan was not defensively copied: %#v", second.Fragments[:2])
	}
	view := compiled.Dataflow()
	if len(view.Nodes) != len(second.Fragments) || view.Root != second.Root {
		t.Fatalf("Dataflow() shape = %#v, want lowered plan shape %#v", view, second)
	}
	for index, fragment := range second.Fragments {
		node := view.Nodes[index]
		if node.ID != fragment.ID || node.Kind != fragment.Kind || node.Detail != fragment.Detail {
			t.Fatalf("Dataflow() node[%d] = %#v, want fragment %#v", index, node, fragment)
		}
	}
}

func TestCompiledSQLQueryLowerDataflowIsSafeForConcurrentFirstUse(t *testing.T) {
	compiled, err := CompileSQLQuery("SELECT name FROM CACHE('users') WHERE score >= $1")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	const workers = 8
	plans := make(chan SQLDataflowPlan, workers)
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for range workers {
		go func() {
			defer waitGroup.Done()
			plans <- compiled.LowerDataflow()
		}()
	}
	waitGroup.Wait()
	close(plans)
	var first SQLDataflowPlan
	for plan := range plans {
		if first.Fragments == nil {
			first = plan
			continue
		}
		if len(plan.Fragments) != len(first.Fragments) || plan.Root != first.Root || plan.Format != first.Format {
			t.Fatalf("concurrent lowering shape = %#v, want %#v", plan, first)
		}
		for index := range first.Fragments {
			if plan.Fragments[index].ID != first.Fragments[index].ID || plan.Fragments[index].Kind != first.Fragments[index].Kind || plan.Fragments[index].Detail != first.Fragments[index].Detail {
				t.Fatalf("concurrent lowering fragment[%d] = %#v, want %#v", index, plan.Fragments[index], first.Fragments[index])
			}
		}
	}
}

var compiledSQLDataflowBenchmarkSink SQLDataflowIR
var compiledSQLDataflowPlanBenchmarkSink SQLDataflowPlan
var compiledSQLCompileBenchmarkSink *CompiledSQLQuery

func BenchmarkCompiledSQLQueryDataflow(b *testing.B) {
	compiled, err := CompileSQLQuery("SELECT name FROM CACHE('users') WHERE score >= $1 ORDER BY name LIMIT 2")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		compiledSQLDataflowBenchmarkSink = compiled.Dataflow()
	}
}

func BenchmarkCompiledSQLQueryLowerDataflow(b *testing.B) {
	compiled, err := CompileSQLQuery("SELECT name FROM CACHE('users') WHERE score >= $1 ORDER BY name LIMIT 2")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		compiledSQLDataflowPlanBenchmarkSink = compiled.LowerDataflow()
	}
}

func BenchmarkCompileSQLQueryWithDataflowPlan(b *testing.B) {
	const source = "SELECT name FROM CACHE('users') WHERE score >= $1 ORDER BY name LIMIT 2"
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		compiled, err := CompileSQLQuery(source)
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLDataflowPlanBenchmarkSink = compiled.LowerDataflow()
	}
}

func BenchmarkCompileSQLQueryWithoutDataflowPlan(b *testing.B) {
	const source = "SELECT name FROM CACHE('users') WHERE score >= $1 ORDER BY name LIMIT 2"
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		parsed, err := CompileSQLShortcut(source)
		if err != nil {
			b.Fatal(err)
		}
		template, err := parseSQLQueryTemplate(parsed)
		if err != nil {
			b.Fatal(err)
		}
		compiledSQLCompileBenchmarkSink = &CompiledSQLQuery{source: source, template: template}
	}
}
