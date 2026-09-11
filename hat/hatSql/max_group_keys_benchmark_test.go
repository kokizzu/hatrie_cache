package hatSql

import "testing"

var maxGroupKeysBenchmarkSink [][]sqlExecRow

func BenchmarkSQLGroupRowsDefault(b *testing.B) {
	rows, by, query := maxGroupKeysBenchmarkInput()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		groups, err := groupSQLRows(rows, by, query)
		if err != nil {
			b.Fatal(err)
		}
		maxGroupKeysBenchmarkSink = groups
	}
}

func maxGroupKeysBenchmarkInput() ([]sqlExecRow, []sqlExpr, *sqlQuery) {
	rows := make([]sqlExecRow, 4_096)
	for index := range rows {
		rows[index] = sqlExecRow{sources: map[string]SQLRow{
			"src": {"region": int64(index % 256)},
		}, order: []string{"src"}}
	}
	by := []sqlExpr{{kind: "field", qualifier: "src", name: "region"}}
	return rows, by, &sqlQuery{}
}
