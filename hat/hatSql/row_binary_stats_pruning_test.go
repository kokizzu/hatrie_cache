package hatSql_test

import (
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCanSkipSQLRowBinaryStatsUsesMinMaxAndNullSemantics(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "id", Type: hatSql.SQLRowBinaryInt64},
		{Name: "optional", Type: hatSql.SQLRowBinaryString, Nullable: true},
	}
	rows := []hatSql.SQLRow{
		{"id": int64(10), "optional": nil},
		{"id": int64(20), "optional": "ready"},
	}
	stats, err := hatSql.BuildSQLRowBinaryColumnStats(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		query hatSql.SQLRowBinaryStatsPredicate
		want  bool
	}{
		{name: "equal below min", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsEqual, Value: int64(5)}, want: true},
		{name: "equal in range", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsEqual, Value: int64(15)}, want: false},
		{name: "less above max", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsLess, Value: int64(5)}, want: true},
		{name: "less in range", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsLess, Value: int64(15)}, want: false},
		{name: "less equal at min", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsLessEqual, Value: int64(10)}, want: false},
		{name: "greater at max", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsGreater, Value: int64(20)}, want: true},
		{name: "greater equal at max", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsGreaterEqual, Value: int64(20)}, want: false},
		{name: "between outside", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsBetween, Value: int64(30), UpperValue: int64(40)}, want: true},
		{name: "between overlap", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsBetween, Value: int64(15), UpperValue: int64(25)}, want: false},
		{name: "not equal range", query: hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsNotEqual, Value: int64(10)}, want: false},
		{name: "is null", query: hatSql.SQLRowBinaryStatsPredicate{Column: "optional", Operator: hatSql.SQLRowBinaryStatsIsNull}, want: false},
		{name: "is not null", query: hatSql.SQLRowBinaryStatsPredicate{Column: "optional", Operator: hatSql.SQLRowBinaryStatsIsNotNull}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := hatSql.CanSkipSQLRowBinaryStats(columns, stats, test.query)
			if err != nil {
				t.Fatalf("CanSkipSQLRowBinaryStats() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("CanSkipSQLRowBinaryStats() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCanSkipSQLRowBinaryStatsHandlesAllNullAndInvalidPredicates(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "optional", Type: hatSql.SQLRowBinaryInt64, Nullable: true},
		{Name: "metadata", Type: hatSql.SQLRowBinaryJSON},
	}
	rows := []hatSql.SQLRow{{"optional": nil, "metadata": json.RawMessage(`{}`)}, {"optional": nil, "metadata": json.RawMessage(`{}`)}}
	stats, err := hatSql.BuildSQLRowBinaryColumnStats(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	for _, operator := range []hatSql.SQLRowBinaryStatsPredicateOperator{hatSql.SQLRowBinaryStatsEqual, hatSql.SQLRowBinaryStatsNotEqual, hatSql.SQLRowBinaryStatsLess, hatSql.SQLRowBinaryStatsLessEqual, hatSql.SQLRowBinaryStatsGreater, hatSql.SQLRowBinaryStatsGreaterEqual, hatSql.SQLRowBinaryStatsBetween} {
		got, err := hatSql.CanSkipSQLRowBinaryStats(columns, stats, hatSql.SQLRowBinaryStatsPredicate{Column: "optional", Operator: operator, Value: int64(1), UpperValue: int64(2)})
		if err != nil || !got {
			t.Fatalf("all-null operator %d = %v, %v, want true", operator, got, err)
		}
	}
	if got, err := hatSql.CanSkipSQLRowBinaryStats(columns, stats, hatSql.SQLRowBinaryStatsPredicate{Column: "optional", Operator: hatSql.SQLRowBinaryStatsIsNull}); err != nil || got {
		t.Fatalf("all-null IS NULL = %v, %v, want false", got, err)
	}
	if got, err := hatSql.CanSkipSQLRowBinaryStats(columns, stats, hatSql.SQLRowBinaryStatsPredicate{Column: "optional", Operator: hatSql.SQLRowBinaryStatsIsNotNull}); err != nil || !got {
		t.Fatalf("all-null IS NOT NULL = %v, %v, want true", got, err)
	}
	invalid := []hatSql.SQLRowBinaryStatsPredicate{
		{Column: "missing", Operator: hatSql.SQLRowBinaryStatsEqual, Value: int64(1)},
		{Column: "metadata", Operator: hatSql.SQLRowBinaryStatsEqual, Value: int64(1)},
		{Column: "optional", Operator: hatSql.SQLRowBinaryStatsEqual, Value: "wrong type"},
		{Column: "optional", Operator: hatSql.SQLRowBinaryStatsBetween, Value: int64(3), UpperValue: int64(2)},
	}
	for index, predicate := range invalid {
		if _, err := hatSql.CanSkipSQLRowBinaryStats(columns, stats, predicate); err == nil {
			t.Fatalf("invalid predicate %d returned nil error", index)
		}
	}
}

func BenchmarkCanSkipSQLRowBinaryStats(b *testing.B) {
	columns := []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64}}
	rows := make([]hatSql.SQLRow, 1024)
	for index := range rows {
		rows[index] = hatSql.SQLRow{"id": int64(index)}
	}
	stats, err := hatSql.BuildSQLRowBinaryColumnStats(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	predicate := hatSql.SQLRowBinaryStatsPredicate{Column: "id", Operator: hatSql.SQLRowBinaryStatsEqual, Value: int64(1 << 20)}
	b.ReportAllocs()
	for range b.N {
		if skipped, err := hatSql.CanSkipSQLRowBinaryStats(columns, stats, predicate); err != nil || !skipped {
			b.Fatalf("skip = %v, %v", skipped, err)
		}
	}
}
