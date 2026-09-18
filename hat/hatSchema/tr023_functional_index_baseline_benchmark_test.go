package hatSchema

import (
	"context"
	"fmt"
	"hatrie_cache/hat/hatSql"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkTR023BeforeFunctionalIndexSQLScan(b *testing.B) {
	source := benchmarkTR023MaterializedSource(b)
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	const query = "FROM CACHE('people') AS person WHERE LOWER(person.name) = 'name-042' SELECT person.id"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, adapter, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("scan rows = %d, want 100", len(result.Rows))
		}
	}
}

func BenchmarkTR023BeforeFunctionalIndexEquivalentScan(b *testing.B) {
	source := benchmarkTR023MaterializedSource(b)
	rows := source.Rows()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		matches := 0
		for _, row := range rows {
			name, ok := row["name"].(string)
			if ok && strings.ToLower(name) == "name-042" {
				matches++
			}
		}
		if matches != 100 {
			b.Fatalf("scan matches = %d, want 100", matches)
		}
	}
}

func BenchmarkTR023BeforeFunctionalIndexBuildEquivalent(b *testing.B) {
	source := benchmarkTR023MaterializedSource(b)
	rows := source.Rows()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		index := make(map[string][]int, len(rows))
		for position, row := range rows {
			name, ok := row["name"].(string)
			if !ok {
				b.Fatal("name is not text")
			}
			key := strings.ToLower(name)
			index[key] = append(index[key], position)
		}
		if len(index) != 100 {
			b.Fatalf("built keys = %d, want 100", len(index))
		}
	}
}

func benchmarkTR023MaterializedSource(b *testing.B) *MaterializedSource {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "name"}, {Name: "payload"}})
	for index := 0; index < 10_000; index++ {
		if _, err := source.Insert(Row{
			"id":      int64(index),
			"name":    "Name-" + fmt.Sprintf("%03d", index%100),
			"payload": "payload-" + strconv.Itoa(index),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return source
}
