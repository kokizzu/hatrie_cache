package hatSchema

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTR024BaselineIndexedProjection(b *testing.B) {
	source := newTR024BenchmarkSource(b)
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"items": source}}
	const query = "FROM CACHE('items') AS i WHERE i.region = 'r-07' SELECT i.region, i.name"
	b.ReportAllocs()
	for b.Loop() {
		result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 313 {
			b.Fatalf("rows = %d, want 313", len(result.Rows))
		}
	}
}

func newTR024BenchmarkSource(b testing.TB) *MaterializedSource {
	columns := []DerivedColumn{
		{Name: "id", Identity: true},
		{Name: "region", Indexed: true},
		{Name: "name"},
	}
	for index := 0; index < 16; index++ {
		columns = append(columns, DerivedColumn{Name: fmt.Sprintf("payload_%02d", index)})
	}
	source := NewMaterializedSource(columns)
	for index := 0; index < 20_000; index++ {
		row := Row{
			"region": fmt.Sprintf("r-%02d", index%64),
			"name":   fmt.Sprintf("name-%d", index),
		}
		for payload := 0; payload < 16; payload++ {
			row[fmt.Sprintf("payload_%02d", payload)] = fmt.Sprintf("value-%d-%d", index, payload)
		}
		if _, err := source.Insert(row); err != nil {
			b.Fatalf("insert %d: %v", index, err)
		}
	}
	return source
}
