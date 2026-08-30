package hatSql

import "testing"

var sqlSingleSourceEnvelopeBenchmarkResult []sqlExecRow

func BenchmarkSQLMaterializedSingleSourceEnvelope(b *testing.B) {
	rows := make([]Row, 20_000)
	for index := range rows {
		rows[index] = Row{"id": int64(index), "team": "core", "score": int64(index % 64)}
	}
	source := sqlSource{alias: "event"}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		sqlSingleSourceEnvelopeBenchmarkResult = wrapSQLSource(source, rows)
	}
}
