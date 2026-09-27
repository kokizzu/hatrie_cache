package hatSql

import "testing"

type tt024MixedBooleanScanResolver struct {
	rows []Row
}

func (resolver tt024MixedBooleanScanResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

type tt024MixedBooleanIndexedResolver struct {
	tt024MixedBooleanScanResolver
	indexedRows []Row
}

func (resolver tt024MixedBooleanIndexedResolver) ResolveSQLTextProximityUnionSource(string, string, string, []SQLTextProximityQuery) ([]Row, bool, error) {
	return resolver.indexedRows, true, nil
}

func tt024MixedBooleanBenchmarkInput(rows int) ([]Row, []Row) {
	allRows := make([]Row, rows)
	indexedRows := make([]Row, 0, rows/100)
	for index := range allRows {
		kind := "other"
		text := "unrelated filler text"
		if index%200 == 0 {
			kind = "a"
			text = "alpha beta"
		} else if index%201 == 0 {
			kind = "b"
			text = "gamma delta"
		}
		row := Row{"id": int64(index), "kind": kind, "text": text}
		allRows[index] = row
		if kind == "a" || kind == "b" {
			indexedRows = append(indexedRows, row)
		}
	}
	return allRows, indexedRows
}

const tt024MixedBooleanBenchmarkQuery = `
FROM CACHE('docs') AS doc
WHERE (CONTAINS_PHRASE(doc.text, 'alpha beta') AND doc.kind = 'a')
   OR (CONTAINS_PROXIMITY(doc.text, 'gamma delta', 1) AND doc.kind = 'b')
SELECT doc.id`

const tt024MixedBooleanFallbackQuery = `
FROM CACHE('docs') AS doc
WHERE CONTAINS_PHRASE(doc.text, 'alpha beta') OR doc.kind = 'missing'
SELECT doc.id`

var tt024MixedBooleanBenchmarkSink SQLQueryResult

func BenchmarkTT024MixedBooleanFullScan(b *testing.B) {
	rows, _ := tt024MixedBooleanBenchmarkInput(50000)
	resolver := tt024MixedBooleanScanResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(tt024MixedBooleanBenchmarkQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		tt024MixedBooleanBenchmarkSink = result
	}
}

func BenchmarkTT024MixedBooleanIndexedUnion(b *testing.B) {
	rows, indexedRows := tt024MixedBooleanBenchmarkInput(50000)
	resolver := tt024MixedBooleanIndexedResolver{
		tt024MixedBooleanScanResolver: tt024MixedBooleanScanResolver{rows: rows},
		indexedRows:                   indexedRows,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(tt024MixedBooleanBenchmarkQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		tt024MixedBooleanBenchmarkSink = result
	}
}

func BenchmarkTT024MixedBooleanFullScanFallback(b *testing.B) {
	rows, _ := tt024MixedBooleanBenchmarkInput(50000)
	resolver := tt024MixedBooleanScanResolver{rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(tt024MixedBooleanFallbackQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		tt024MixedBooleanBenchmarkSink = result
	}
}

func BenchmarkTT024MixedBooleanIndexedFallback(b *testing.B) {
	rows, indexedRows := tt024MixedBooleanBenchmarkInput(50000)
	resolver := tt024MixedBooleanIndexedResolver{
		tt024MixedBooleanScanResolver: tt024MixedBooleanScanResolver{rows: rows},
		indexedRows:                   indexedRows,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(tt024MixedBooleanFallbackQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		tt024MixedBooleanBenchmarkSink = result
	}
}
