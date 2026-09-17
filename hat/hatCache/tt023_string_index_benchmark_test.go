package hatCache

import "testing"

var tt023StringIndexBenchmarkSink []SQLRow

func BenchmarkSQLJSONFieldIndexStringLookupLegacy(b *testing.B) {
	rows := []SQLRow{{"id": int64(1), "name": "needle"}}
	index := &sqlJSONFieldIndex{rows: map[string][]SQLRow{"s:needle": rows}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		valueKey, ok := sqlIndexValueKey("needle")
		if !ok {
			b.Fatal("legacy key encoding rejected string")
		}
		tt023StringIndexBenchmarkSink = index.rows[valueKey]
	}
}

func BenchmarkSQLJSONFieldIndexStringLookupFast(b *testing.B) {
	rows := []SQLRow{{"id": int64(1), "name": "needle"}}
	index := &sqlJSONFieldIndex{
		stringOnly: true,
		rows:       map[string][]SQLRow{"needle": rows},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ok bool
		tt023StringIndexBenchmarkSink, ok = sqlJSONFieldIndexLookupRows(index, "needle")
		if !ok {
			b.Fatal("fast string lookup unavailable")
		}
	}
}
