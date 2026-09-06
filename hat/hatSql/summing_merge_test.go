package hatSql

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestSQLSummingMergeSumsSelectedColumnsInStableKeyOrder(t *testing.T) {
	rows := []SQLRow{
		{"id": "a", "count": int64(2), "amount": 1.5, "label": "first"},
		{"id": "b", "count": int64(4), "amount": 2.0, "label": "b"},
		{"id": "a", "count": int64(3), "amount": 2.5, "label": "second"},
		{"id": "b", "count": int64(1), "amount": 4.0, "label": "stale-label"},
	}

	merged, err := SumSQLRows(rows, func(row SQLRow) string { return row["id"].(string) }, []string{"count", "amount"})
	if err != nil {
		t.Fatalf("SumSQLRows() error = %v", err)
	}
	want := []SQLRow{
		{"id": "a", "count": int64(5), "amount": 4.0, "label": "first"},
		{"id": "b", "count": int64(5), "amount": 6.0, "label": "b"},
	}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("merged = %#v, want %#v", merged, want)
	}
	merged[0]["label"] = "changed-output"
	if rows[0]["label"] != "first" {
		t.Fatal("merged row aliases input row")
	}
}

func TestSQLSummingMergePreservesNumericTypesAndHandlesMissingValues(t *testing.T) {
	rows := []SQLRow{
		{"id": "int8", "value": int8(2)},
		{"id": "int8", "value": int8(3)},
		{"id": "uint32", "value": uint32(4)},
		{"id": "uint32", "value": nil},
		{"id": "float32", "value": float32(1.25)},
		{"id": "float32", "value": float32(2.75)},
	}
	merged, err := SumSQLRows(rows, func(row SQLRow) string { return row["id"].(string) }, []string{"value"})
	if err != nil {
		t.Fatalf("SumSQLRows() error = %v", err)
	}
	want := []SQLRow{
		{"id": "int8", "value": int8(5)},
		{"id": "uint32", "value": uint32(4)},
		{"id": "float32", "value": float32(4)},
	}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("merged = %#v, want %#v", merged, want)
	}
}

func TestSQLSummingMergeSupportsEveryBuiltinNumericKind(t *testing.T) {
	tests := []struct {
		name  string
		first interface{}
		last  interface{}
		want  interface{}
	}{
		{name: "int", first: int(1), last: int(2), want: int(3)},
		{name: "int8", first: int8(1), last: int8(2), want: int8(3)},
		{name: "int16", first: int16(1), last: int16(2), want: int16(3)},
		{name: "int32", first: int32(1), last: int32(2), want: int32(3)},
		{name: "int64", first: int64(1), last: int64(2), want: int64(3)},
		{name: "uint", first: uint(1), last: uint(2), want: uint(3)},
		{name: "uint8", first: uint8(1), last: uint8(2), want: uint8(3)},
		{name: "uint16", first: uint16(1), last: uint16(2), want: uint16(3)},
		{name: "uint32", first: uint32(1), last: uint32(2), want: uint32(3)},
		{name: "uint64", first: uint64(1), last: uint64(2), want: uint64(3)},
		{name: "float32", first: float32(1), last: float32(2), want: float32(3)},
		{name: "float64", first: float64(1), last: float64(2), want: float64(3)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := []SQLRow{{"id": test.name, "value": test.first}, {"id": test.name, "value": test.last}}
			merged, err := SumSQLRows(rows, func(row SQLRow) string { return row["id"].(string) }, []string{"value"})
			if err != nil {
				t.Fatalf("SumSQLRows() error = %v", err)
			}
			if got := merged[0]["value"]; !reflect.DeepEqual(got, test.want) {
				t.Fatalf("value = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestSQLSummingMergeRejectsOverflowAndTypeErrors(t *testing.T) {
	key := func(row SQLRow) string { return row["id"].(string) }
	for name, rows := range map[string][]SQLRow{
		"signed overflow":   {{"id": "a", "value": int64(math.MaxInt64)}, {"id": "a", "value": int64(1)}},
		"unsigned overflow": {{"id": "a", "value": uint64(math.MaxUint64)}, {"id": "a", "value": uint64(1)}},
		"type mismatch":     {{"id": "a", "value": int64(1)}, {"id": "a", "value": uint64(1)}},
		"nonnumeric":        {{"id": "a", "value": "one"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := SumSQLRows(rows, key, []string{"value"}); err == nil {
				t.Fatal("SumSQLRows() error = nil")
			}
		})
	}
}

func TestSQLSummingMergeRejectsInvalidArguments(t *testing.T) {
	rows := []SQLRow{{"id": "a", "value": int64(1)}}
	if _, err := SumSQLRows(rows, nil, []string{"value"}); !errors.Is(err, ErrSQLSummingMergeKeyRequired) {
		t.Fatalf("nil key error = %v, want ErrSQLSummingMergeKeyRequired", err)
	}
	for _, columns := range [][]string{nil, {}, {"value", "value"}, {""}} {
		if _, err := SumSQLRows(rows, func(SQLRow) string { return "a" }, columns); err == nil {
			t.Errorf("sum columns %#v error = nil", columns)
		}
	}
}

func TestSQLSummingMergeHandlesEmptyInput(t *testing.T) {
	merged, err := SumSQLRows(nil, func(SQLRow) string { return "" }, []string{"value"})
	if err != nil {
		t.Fatalf("SumSQLRows() error = %v", err)
	}
	if merged != nil {
		t.Fatalf("merged = %#v, want nil", merged)
	}
}

func BenchmarkSQLSummingMerge(b *testing.B) {
	rows := make([]SQLRow, 1024)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    index % 256,
			"count": int64(index),
		}
	}
	key := func(row SQLRow) string { return string(rune(row["id"].(int))) }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := SumSQLRows(rows, key, []string{"count"}); err != nil {
			b.Fatal(err)
		}
	}
}
