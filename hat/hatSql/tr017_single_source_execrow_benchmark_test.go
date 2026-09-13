package hatSql

import (
	"context"
	"testing"
)

var tr017IndexedOrderStreamSink SQLQueryResult

func BenchmarkTR017IndexedOrderStream(b *testing.B) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.id AS id, src.value AS value ORDER BY src.value DESC LIMIT 32 OFFSET 8192")
	if err != nil {
		b.Fatal(err)
	}
	rows := make([]SQLRow, 16384)
	for index := range rows {
		value := int64(len(rows) - index)
		rows[index] = SQLRow{"id": int64(index), "value": value}
	}
	resolver := tr017OrderedSourceResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		tr017IndexedOrderStreamSink = result
	}
}
