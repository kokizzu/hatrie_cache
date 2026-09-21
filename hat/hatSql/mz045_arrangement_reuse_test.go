package hatSql

import (
	"reflect"
	"testing"
)

func TestMZ045CompiledTemplateReusesArrangementWorkload(t *testing.T) {
	compiled, err := CompileSQLQuery(mz045ExplainWorkloadQuery)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.template.arrangementWorkload == nil {
		t.Fatal("compiled template did not prepare its arrangement workload")
	}
	prepared := *compiled.template.arrangementWorkload
	if len(prepared.JoinFields) == 0 {
		t.Fatalf("compiled arrangement workload = %#v, want join fields", prepared)
	}
	if got := sqlArrangementWorkloadForQuery(compiled.template); !reflect.DeepEqual(got, prepared) {
		t.Fatalf("cached arrangement workload = %#v, want %#v", got, prepared)
	}
	if got := sqlArrangementWorkloadForQuery(compiled.template); !reflect.DeepEqual(got, prepared) {
		t.Fatalf("reused arrangement workload = %#v, want %#v", got, prepared)
	}

	parameterized, err := CompileSQLQuery("FROM VALUES (1), (2) AS values(id) WHERE id > $1 SELECT id")
	if err != nil {
		t.Fatal(err)
	}
	bound, err := bindSQLQueryParameters(parameterized.template, []interface{}{0})
	if err != nil {
		t.Fatal(err)
	}
	if bound.arrangementWorkload != nil {
		t.Fatal("parameter-bound clone retained immutable-template arrangement workload")
	}
}

func BenchmarkMZ045ExplainWorkloadCompiledReuse(b *testing.B) {
	compiled, err := CompileSQLQuery(mz045ExplainWorkloadQuery)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		mz045ExplainWorkloadBenchmarkSink = sqlExplainSteps(compiled.template)
	}
}
