package hatSql

import "testing"

func TestCH048StringComparisonRecognizerNormalizesLiteralLeft(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE 'm' <= src.name SELECT src.name`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	field, operator, value, ok := sqlColumnarStringComparison(query.where, query.from.alias)
	if !ok || field != "name" || operator != ">=" || value != "m" {
		t.Fatalf("sqlColumnarStringComparison() = %q, %q, %q, %t, want name, >=, m, true", field, operator, value, ok)
	}
}

func TestCH048StringComparisonMatcherPreservesBinaryOrderingAndNulls(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.name >= 'm' SELECT src.name`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"name": {"a", "m", "z", nil}},
		Rows:    4,
	}
	matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
	want := []bool{false, true, true, false}
	for row, expected := range want {
		matched, err := matcher(row)
		if err != nil {
			t.Fatalf("matcher(%d) error = %v", row, err)
		}
		if matched != expected {
			t.Fatalf("matcher(%d) = %t, want %t", row, matched, expected)
		}
	}
}

func TestCH048StringComparisonRecognizerRejectsUnsafeShapes(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.name >= 1 SELECT src.name`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters(numeric) error = %v", err)
	}
	if _, _, _, ok := sqlColumnarStringComparison(query.where, query.from.alias); ok {
		t.Fatal("sqlColumnarStringComparison(numeric) unexpectedly accepted unsafe shape")
	}

	query, err = parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.name >= 'm' SELECT src.name`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters(collation) error = %v", err)
	}
	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	if _, _, _, ok := sqlColumnarStringComparison(query.where, query.from.alias); ok {
		t.Fatal("sqlColumnarStringComparison(collation) unexpectedly accepted unsafe shape")
	}

}

func BenchmarkCH048StringComparison(b *testing.B) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.name >= 'm' SELECT src.name`, nil)
	if err != nil {
		b.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	values := make([]interface{}, 4096)
	for index := range values {
		if index&1 == 0 {
			values[index] = "a"
		} else {
			values[index] = "z"
		}
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"name": values}, Rows: len(values)}
	matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matched := 0
		for row := 0; row < batch.Rows; row++ {
			ok, err := matcher(row)
			if err != nil {
				b.Fatal(err)
			}
			if ok {
				matched++
			}
		}
		if matched != batch.Rows/2 {
			b.Fatalf("matched = %d, want %d", matched, batch.Rows/2)
		}
	}
}
