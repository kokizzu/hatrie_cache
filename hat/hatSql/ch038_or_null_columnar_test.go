package hatSql

import (
	"reflect"
	"testing"
)

const ch038OrNullColumnarGroupQuery = `
FROM CACHE('items') AS events
SELECT events.region,
       COUNT_OR_NULL(*) AS rows_present,
       COUNT_OR_NULL(events.amount) AS counted,
       SUM_OR_NULL(events.amount) AS summed,
       AVG_OR_NULL(events.amount) AS averaged,
       MIN_OR_NULL(events.amount) AS minimum,
       MAX_OR_NULL(events.amount) AS maximum
GROUP BY events.region`

func TestCH038OrNullColumnarGroupUsesVectorPlanAndPreservesNulls(t *testing.T) {
	query, err := parseSQLQuery(ch038OrNullColumnarGroupQuery)
	if err != nil {
		t.Fatalf("parse OrNull columnar query: %v", err)
	}
	_, projections, fields, ok := sqlColumnarVectorGroupAggregatePlan(query, nil)
	if !ok {
		t.Fatal("OrNull grouped query was not admitted to the columnar vector plan")
	}
	if want := []string{"region", "amount"}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("columnar fields = %#v, want %#v", fields, want)
	}
	if len(projections) != 7 {
		t.Fatalf("projection count = %d, want 7", len(projections))
	}

	resolver := &sqlColumnarQueryRowsResolver{batch: ch038OrNullColumnarGroupBatch()}
	result, err := ExecuteSQLQuery(ch038OrNullColumnarGroupQuery, resolver)
	if err != nil {
		t.Fatalf("execute OrNull columnar query: %v", err)
	}
	if resolver.columnarCalls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.columnarCalls)
	}

	got := make(map[string]SQLRow, len(result.Rows))
	for _, row := range result.Rows {
		region, ok := row["region"].(string)
		if !ok {
			t.Fatalf("region row value = %#v, want string", row["region"])
		}
		got[region] = row
	}
	want := map[string]SQLRow{
		"a": {
			"region":       "a",
			"rows_present": int64(2),
			"counted":      int64(1),
			"summed":       float64(10),
			"averaged":     float64(10),
			"minimum":      float64(10),
			"maximum":      float64(10),
		},
		"b": {
			"region":       "b",
			"rows_present": int64(2),
			"counted":      int64(1),
			"summed":       float64(4),
			"averaged":     float64(4),
			"minimum":      float64(4),
			"maximum":      float64(4),
		},
		"c": {
			"region":       "c",
			"rows_present": int64(2),
			"counted":      nil,
			"summed":       nil,
			"averaged":     nil,
			"minimum":      nil,
			"maximum":      nil,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OrNull grouped rows = %#v, want %#v", got, want)
	}
}

func ch038OrNullColumnarGroupBatch() ColumnarBatch {
	return ColumnarBatch{
		Columns: map[string][]interface{}{
			"region": {"a", "a", "b", "b", "c", "c"},
			"amount": {int64(10), nil, nil, int64(4), nil, nil},
		},
		Rows: 6,
	}
}

func BenchmarkCH038OrNullColumnarGroupBaseline(b *testing.B) {
	query := ch038OrNullColumnarGroupBenchmarkQuery()
	resolver := &ch038OrNullBenchmarkResolver{batch: ch038OrNullColumnarGroupBenchmarkBatch(), columnar: true}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ExecuteSQLQuery(query, resolver); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH038OrNullColumnarGroupFallback(b *testing.B) {
	query := ch038OrNullColumnarGroupBenchmarkQuery()
	resolver := &ch038OrNullBenchmarkResolver{batch: ch038OrNullColumnarGroupBenchmarkBatch()}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ExecuteSQLQuery(query, resolver); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH038OrNullColumnarGroupFastPath(b *testing.B) {
	query := ch038OrNullColumnarGroupBenchmarkQuery()
	resolver := &ch038OrNullBenchmarkResolver{batch: ch038OrNullColumnarGroupBenchmarkBatch(), columnar: true}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ExecuteSQLQuery(query, resolver); err != nil {
			b.Fatal(err)
		}
	}
}

func ch038OrNullColumnarGroupBenchmarkQuery() string {
	return `
FROM CACHE('items') AS events
SELECT events.region,
       COUNT_OR_NULL(events.amount) AS counted,
       SUM_OR_NULL(events.amount) AS summed,
       AVG_OR_NULL(events.amount) AS averaged,
       MIN_OR_NULL(events.amount) AS minimum,
       MAX_OR_NULL(events.amount) AS maximum
GROUP BY events.region`
}

func ch038OrNullColumnarGroupBenchmarkBatch() ColumnarBatch {
	const rows = 4096
	regions := make([]interface{}, rows)
	amounts := make([]interface{}, rows)
	for row := 0; row < rows; row++ {
		regions[row] = "region-" + string(rune('a'+row%32))
		if row%4 != 0 {
			amounts[row] = int64(row % 100)
		}
	}
	return ColumnarBatch{
		Columns: map[string][]interface{}{"region": regions, "amount": amounts},
		Rows:    rows,
	}
}

type ch038OrNullBenchmarkResolver struct {
	batch    ColumnarBatch
	rows     []Row
	columnar bool
}

func (resolver *ch038OrNullBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	if resolver.rows == nil {
		resolver.rows = make([]Row, resolver.batch.Rows)
		regions := resolver.batch.Columns["region"]
		amounts := resolver.batch.Columns["amount"]
		for row := 0; row < resolver.batch.Rows; row++ {
			resolver.rows[row] = Row{"region": regions[row], "amount": amounts[row]}
		}
	}
	return resolver.rows, nil
}

func (resolver *ch038OrNullBenchmarkResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, resolver.columnar, nil
}
