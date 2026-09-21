package hatSql

import "testing"

var ch037ArrayJoinBenchmarkSink int

func benchmarkCH037ArrayJoinRows(count int, withEmpty bool) ([]Row, int) {
	rows := make([]Row, count)
	expected := 0
	for index := range rows {
		tags := []interface{}{"a", "b", "c", "d"}
		if withEmpty && index%8 == 0 {
			tags = []interface{}{}
			expected++
		} else {
			expected += len(tags)
		}
		rows[index] = Row{"id": int64(index), "tags": tags}
	}
	return rows, expected
}

func benchmarkCH037ArrayJoin(b *testing.B, query string, rows []Row, expected int) {
	b.Helper()
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != expected {
			b.Fatalf("rows = %d, want %d", len(result.Rows), expected)
		}
		ch037ArrayJoinBenchmarkSink = len(result.Rows)
	}
}

func BenchmarkCH037ArrayJoinInner(b *testing.B) {
	rows, expected := benchmarkCH037ArrayJoinRows(2048, false)
	benchmarkCH037ArrayJoin(b, `FROM CACHE('items') ARRAY JOIN tags AS tag SELECT id, tag`, rows, expected)
}

func BenchmarkCH037LeftArrayJoin(b *testing.B) {
	rows, expected := benchmarkCH037ArrayJoinRows(2048, true)
	benchmarkCH037ArrayJoin(b, `FROM CACHE('items') LEFT ARRAY JOIN tags AS tag SELECT id, tag`, rows, expected)
}

func benchmarkCH037LegacyLeftRows(rows []Row) []Row {
	normalized := make([]Row, len(rows))
	for index, source := range rows {
		row := make(Row, len(source))
		for key, value := range source {
			row[key] = value
		}
		if tags, ok := source["tags"].([]interface{}); ok && len(tags) == 0 {
			row["tags"] = []interface{}{nil}
		}
		normalized[index] = row
	}
	return normalized
}

func BenchmarkCH037LeftArrayJoinLegacy(b *testing.B) {
	rows, expected := benchmarkCH037ArrayJoinRows(2048, true)
	benchmarkCH037ArrayJoin(b, `FROM CACHE('items') ARRAY JOIN tags AS tag SELECT id, tag`, benchmarkCH037LegacyLeftRows(rows), expected)
}
