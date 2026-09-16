package hatSql_test

import (
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func BenchmarkCH031AutomaticJSONSubcolumn(b *testing.B) {
	documents := make([]interface{}, 4096)
	rows := make([]hatSql.Row, len(documents))
	for row := range documents {
		document := `{"id":` + strconv.Itoa(row) + `}`
		documents[row] = document
		rows[row] = hatSql.Row{"doc": document}
	}
	key := hatSql.JSONSubcolumnAutoKey{SourceName: "CACHE", SourceKey: "items", Field: "doc", Path: "$.id", Generation: 1}

	b.Run("materialize_each_time", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(documents)))
		for range b.N {
			column, err := hatSql.MaterializeJSONSubcolumn("$.id", documents)
			if err != nil || column.Rows != len(documents) {
				b.Fatalf("materialize rows = %d, err = %v", column.Rows, err)
			}
		}
	})

	b.Run("observe_cached", func(b *testing.B) {
		materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{MinObservations: 1})
		if err != nil {
			b.Fatal(err)
		}
		if _, ready, err := materializer.Observe(key, documents); err != nil || !ready {
			b.Fatalf("warm Observe() = ready %v, err = %v", ready, err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(documents)))
		for range b.N {
			column, ready, err := materializer.Observe(key, documents)
			if err != nil || !ready || column.Rows != len(documents) {
				b.Fatalf("cached Observe() = rows %d, ready %v, err = %v", column.Rows, ready, err)
			}
		}
	})

	b.Run("lookup_cached", func(b *testing.B) {
		materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{MinObservations: 1})
		if err != nil {
			b.Fatal(err)
		}
		if _, ready, err := materializer.Observe(key, documents); err != nil || !ready {
			b.Fatalf("warm Observe() = ready %v, err = %v", ready, err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(documents)))
		for range b.N {
			column, found, err := materializer.Lookup(key)
			if err != nil || !found || column.Rows != len(documents) {
				b.Fatalf("cached Lookup() = rows %d, found %v, err = %v", column.Rows, found, err)
			}
		}
	})

	b.Run("resolve_batch_cached", func(b *testing.B) {
		materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{MinObservations: 1})
		if err != nil {
			b.Fatal(err)
		}
		source := hatSql.JSONSubcolumnAutoSource{SourceName: key.SourceName, SourceKey: key.SourceKey, Generation: key.Generation}
		paths := []hatSql.ColumnarJSONSubcolumnRequest{{Field: key.Field, Path: key.Path}}
		if _, ready, err := materializer.ResolveBatch(source, nil, paths, rows); err != nil || !ready {
			b.Fatalf("warm ResolveBatch() = ready %v, err = %v", ready, err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(documents)))
		for range b.N {
			batch, ready, err := materializer.ResolveBatch(source, nil, paths, rows)
			if err != nil || !ready || batch.Rows != len(rows) || len(batch.JSONSubcolumns) != 1 {
				b.Fatalf("cached ResolveBatch() = rows %d, columns %d, ready %v, err = %v", batch.Rows, len(batch.JSONSubcolumns), ready, err)
			}
		}
	})
}
