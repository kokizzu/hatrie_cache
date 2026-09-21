package hatSql

import (
	"strconv"
	"strings"
	"testing"
)

type ch030BenchmarkMapResolver struct {
	rows  []Row
	batch ColumnarBatch
}

func (resolver ch030BenchmarkMapResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func (resolver ch030BenchmarkMapResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver ch030BenchmarkMapResolver) ResolveSQLColumnarMapSubcolumns(string, string, []string, []ColumnarMapSubcolumn) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	return resolver.batch, nil, true, nil
}

func newCH030BenchmarkInput(rowCount int) ([]Row, ch030BenchmarkMapResolver) {
	rows := make([]Row, rowCount)
	mapRows := make([]map[string]interface{}, rowCount)
	large := strings.Repeat("x", 4096)
	ids := make([]interface{}, rowCount)
	for index := range rows {
		country := "US"
		if index%10 == 0 {
			country = "SG"
		}
		doc := `{"country":"` + country + `","large":"` + large + `"}`
		rows[index] = Row{"id": int64(index), "doc": doc}
		mapRows[index] = map[string]interface{}{"country": country}
		ids[index] = int64(index)
	}
	mapColumn, err := NewColumnarMapColumn(mapRows)
	if err != nil {
		panic(err)
	}
	return rows, ch030BenchmarkMapResolver{
		rows: rows,
		batch: ColumnarBatch{
			Columns:    map[string][]interface{}{"id": ids},
			MapColumns: map[string]ColumnarMapColumn{"doc": mapColumn},
			Rows:       rowCount,
		},
	}
}

func BenchmarkCH030MapSubcolumn(b *testing.B) {
	query := "FROM CACHE('docs') AS src WHERE JSON_VALUE(src.doc, '$.country') = 'SG' SELECT id, JSON_VALUE(src.doc, '$.country') AS country"
	for _, rowCount := range []int{1024, 10000} {
		rows, optimizedResolver := newCH030BenchmarkInput(rowCount)
		wantRows := (rowCount + 9) / 10
		b.Run("rows="+strconv.Itoa(rowCount)+"/full-row", func(b *testing.B) {
			resolver := ch030FullMapResolver{rows: rows}
			b.ReportAllocs()
			for range b.N {
				result, err := ExecuteSQLQuery(query, resolver)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != wantRows {
					b.Fatalf("result rows = %d, want %d", len(result.Rows), wantRows)
				}
			}
		})
		b.Run("rows="+strconv.Itoa(rowCount)+"/map-subcolumn", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				result, err := ExecuteSQLQuery(query, optimizedResolver)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != wantRows {
					b.Fatalf("result rows = %d, want %d", len(result.Rows), wantRows)
				}
			}
		})
	}
}

func newCH045NestedBenchmarkInput(rowCount int) ([]Row, ch030BenchmarkMapResolver) {
	rows := make([]Row, rowCount)
	mapRows := make([]map[string]interface{}, rowCount)
	large := strings.Repeat("x", 4096)
	ids := make([]interface{}, rowCount)
	for index := range rows {
		country := "US"
		if index%10 == 0 {
			country = "SG"
		}
		doc := `{"profile":{"country":"` + country + `","large":"` + large + `"}}`
		rows[index] = Row{"id": int64(index), "doc": doc}
		mapRows[index] = map[string]interface{}{"profile": map[string]interface{}{"country": country}}
		ids[index] = int64(index)
	}
	mapColumn, err := NewColumnarMapColumn(mapRows)
	if err != nil {
		panic(err)
	}
	return rows, ch030BenchmarkMapResolver{
		rows: rows,
		batch: ColumnarBatch{
			Columns:    map[string][]interface{}{"id": ids},
			MapColumns: map[string]ColumnarMapColumn{"doc": mapColumn},
			Rows:       rowCount,
		},
	}
}

func BenchmarkCH045NestedMapSubcolumn(b *testing.B) {
	query := "FROM CACHE('docs') AS src WHERE JSON_VALUE(src.doc, '$.profile.country') = 'SG' SELECT id, JSON_VALUE(src.doc, '$.profile.country') AS country"
	for _, rowCount := range []int{1024, 10000} {
		rows, optimizedResolver := newCH045NestedBenchmarkInput(rowCount)
		wantRows := (rowCount + 9) / 10
		b.Run("rows="+strconv.Itoa(rowCount)+"/full-row", func(b *testing.B) {
			resolver := ch030FullMapResolver{rows: rows}
			b.ReportAllocs()
			for range b.N {
				result, err := ExecuteSQLQuery(query, resolver)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != wantRows {
					b.Fatalf("result rows = %d, want %d", len(result.Rows), wantRows)
				}
			}
		})
		b.Run("rows="+strconv.Itoa(rowCount)+"/map-subcolumn", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				result, err := ExecuteSQLQuery(query, optimizedResolver)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != wantRows {
					b.Fatalf("result rows = %d, want %d", len(result.Rows), wantRows)
				}
			}
		})
	}
}
