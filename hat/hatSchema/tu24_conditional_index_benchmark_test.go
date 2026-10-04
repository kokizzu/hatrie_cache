package hatSchema

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var tu24BenchmarkResult hatSql.SQLQueryResult

func BenchmarkTU24ConditionalQueryBaseline(b *testing.B) {
	adapter := tu24BenchmarkAdapter(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQuery("FROM CACHE('jobs') WHERE id = 9000 AND status = 'active' SELECT id, status", adapter)
		if err != nil {
			b.Fatal(err)
		}
		tu24BenchmarkResult = result
	}
}

func BenchmarkTU24ConditionalQueryIndexed(b *testing.B) {
	adapter := tu24BenchmarkAdapter(b)
	definition := IndexDefinition{
		Name:    "active_id",
		Kind:    IndexKindConditional,
		Columns: []string{"id"},
		Condition: &IndexCondition{
			Field: "status",
			Value: "active",
		},
	}
	if _, err := adapter.Sources["jobs"].BuildConditionalIndex(definition); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteSQLQuery("FROM CACHE('jobs') WHERE id = 9000 AND status = 'active' SELECT id, status", adapter)
		if err != nil {
			b.Fatal(err)
		}
		tu24BenchmarkResult = result
	}
}

func BenchmarkTU24ConditionalIndexBuild(b *testing.B) {
	for index := 0; index < b.N; index++ {
		adapter := tu24BenchmarkAdapter(b)
		if _, err := adapter.Sources["jobs"].BuildConditionalIndex(IndexDefinition{
			Name:    "active_id",
			Kind:    IndexKindConditional,
			Columns: []string{"id"},
			Condition: &IndexCondition{
				Field: "status",
				Value: "active",
			},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func tu24BenchmarkAdapter(b *testing.B) SQLResolverAdapter {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "status"}})
	for index := 0; index < 10_000; index++ {
		status := "inactive"
		if index%10 == 0 {
			status = "active"
		}
		if _, err := source.Insert(Row{"id": int64(index), "status": status, "payload": fmt.Sprintf("payload-%d", index)}); err != nil {
			b.Fatal(err)
		}
	}
	return SQLResolverAdapter{Sources: map[string]*MaterializedSource{"jobs": source}}
}
