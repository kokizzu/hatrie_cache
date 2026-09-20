package hatSql

import (
	"encoding/json"
	"fmt"
	"testing"
)

var chu20BenchmarkSink interface{}

func newCHU20BenchmarkFixture(rows int) ([]interface{}, ColumnarJSONSubcolumn) {
	documents := newCHU20BenchmarkDocuments(rows)
	column, err := MaterializeJSONSubcolumn("$.items", documents)
	if err != nil {
		panic(err)
	}
	return documents, column
}

func newCHU20BenchmarkDocuments(rows int) []interface{} {
	documents := make([]interface{}, rows)
	for row := range documents {
		documents[row] = fmt.Sprintf(`{"items":[{"sku":"sku-%d","quantity":%d},{"sku":"backup-%d"}],"unused":{"payload":"%s"}}`, row, row+1, row, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	}
	return documents
}

func BenchmarkCHU20JSONSubcolumnMaterialize(b *testing.B) {
	documents := newCHU20BenchmarkDocuments(256)
	b.ReportAllocs()
	b.SetBytes(int64(totalCHU20DocumentBytes(documents)))
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		column, err := MaterializeJSONSubcolumn("$.items", documents)
		if err != nil {
			b.Fatal(err)
		}
		chu20BenchmarkSink = column
	}
}

func BenchmarkCHU20JSONQueryRowFallback(b *testing.B) {
	documents, _ := newCHU20BenchmarkFixture(256)
	program := compileSQLJSONPath("$.items")
	b.ReportAllocs()
	b.SetBytes(int64(len(documents)))
	b.ResetTimer()
	b.ReportMetric(float64(totalCHU20DocumentBytes(documents)), "source_bytes/op")
	for iteration := 0; iteration < b.N; iteration++ {
		for _, document := range documents {
			value, present, err := sqlJSONPathValue(document, program.segments)
			if err != nil || !present {
				b.Fatalf("row fallback = %#v, %v, %v", value, present, err)
			}
			chu20BenchmarkSink = value
		}
	}
}

func BenchmarkCHU20JSONQueryColumnarLateMaterialized(b *testing.B) {
	_, column := newCHU20BenchmarkFixture(256)
	b.ReportAllocs()
	b.SetBytes(int64(column.Rows))
	b.ResetTimer()
	b.ReportMetric(float64(columnarJSONSubcolumnRetainedBytes(column)), "column_bytes/op")
	for iteration := 0; iteration < b.N; iteration++ {
		for row := 0; row < column.Rows; row++ {
			value, present := column.Value(row)
			if !present {
				b.Fatalf("columnar row %d is missing", row)
			}
			chu20BenchmarkSink = sqlJSONMaterialize(value)
		}
	}
}

func BenchmarkCHU20JSONExistsRowFallback(b *testing.B) {
	documents, _ := newCHU20BenchmarkFixture(256)
	program := compileSQLJSONPath("$.items")
	b.ReportAllocs()
	b.SetBytes(int64(len(documents)))
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, document := range documents {
			_, present, err := sqlJSONPathValue(document, program.segments)
			if err != nil {
				b.Fatal(err)
			}
			chu20BenchmarkSink = present
		}
	}
}

func BenchmarkCHU20JSONExistsColumnar(b *testing.B) {
	_, column := newCHU20BenchmarkFixture(256)
	b.ReportAllocs()
	b.SetBytes(int64(column.Rows))
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for row := 0; row < column.Rows; row++ {
			chu20BenchmarkSink = column.Exists(row)
		}
	}
}

func totalCHU20DocumentBytes(documents []interface{}) int {
	total := 0
	for _, document := range documents {
		encoded, err := json.Marshal(document)
		if err == nil {
			total += len(encoded)
		}
	}
	return total
}
