package hatSql

import (
	"context"
	"reflect"
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

var compiledSQLDataflowBenchmarkSink SQLDataflowIR

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
