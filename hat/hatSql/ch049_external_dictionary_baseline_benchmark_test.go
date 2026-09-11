package hatSql

import "testing"

var ch049ExternalDictionaryBenchmarkSink interface{}

func BenchmarkCH049BaselineMapLookup(b *testing.B) {
	values := map[string]interface{}{"sg": "asia", "us": "america", "jp": "japan"}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ch049ExternalDictionaryBenchmarkSink, _ = values["sg"]
	}
}
