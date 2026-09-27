package hatSql

import "testing"

const tt024CrossFieldBenchmarkQuery = `
FROM CACHE('docs') AS doc
WHERE (CONTAINS_PHRASE(doc.title, 'alpha beta') AND doc.kind = 'a')
   OR (CONTAINS_PROXIMITY(doc.body, 'gamma delta', 1) AND doc.kind = 'b')
SELECT doc.id`

func tt024CrossFieldBenchmarkInput(rows int) ([]Row, []Row) {
	allRows := make([]Row, rows)
	indexedRows := make([]Row, 0, rows/100)
	for index := range allRows {
		kind := "other"
		title := "unrelated filler title"
		body := "unrelated filler body"
		if index%200 == 0 {
			kind = "a"
			title = "alpha beta"
		} else if index%201 == 0 {
			kind = "b"
			body = "gamma delta"
		}
		row := Row{"id": int64(index), "kind": kind, "title": title, "body": body}
		allRows[index] = row
		if kind == "a" || kind == "b" {
			indexedRows = append(indexedRows, row)
		}
	}
	return allRows, indexedRows
}

var tt024CrossFieldBenchmarkSink SQLQueryResult

func BenchmarkTT024CrossFieldMixedBooleanFullScan(b *testing.B) {
	rows, _ := tt024CrossFieldBenchmarkInput(50000)
	resolver := tt024MixedBooleanScanResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(tt024CrossFieldBenchmarkQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		tt024CrossFieldBenchmarkSink = result
	}
}

func BenchmarkTT024CrossFieldMixedBooleanIndexedUnion(b *testing.B) {
	rows, indexedRows := tt024CrossFieldBenchmarkInput(50000)
	resolver := &tt024CrossFieldUnionResolver{rows: rows, indexedRows: indexedRows}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(tt024CrossFieldBenchmarkQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		tt024CrossFieldBenchmarkSink = result
	}
}
