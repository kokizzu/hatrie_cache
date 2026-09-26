package hatSql

import (
	"fmt"
	"reflect"
	"testing"
)

const ch037ColumnarArrayJoinQuery = `
FROM CACHE('items') AS events
ARRAY JOIN events.tags AS tag
SELECT events.id AS id, tag AS tag`

const ch037ColumnarLeftArrayJoinQuery = `
FROM CACHE('items') AS events
LEFT ARRAY JOIN events.tags AS tag
SELECT events.id AS id, tag AS tag`

func TestCH037ColumnarArrayJoinUsesBatchPath(t *testing.T) {
	query, err := parseSQLQuery(ch037ColumnarArrayJoinQuery)
	if err != nil {
		t.Fatalf("parse columnar ARRAY JOIN: %v", err)
	}
	plan, planned := sqlColumnarArrayJoinPlanFor(query, nil)
	if !planned {
		t.Fatalf("columnar ARRAY JOIN plan rejected: from=%#v join=%#v where=%#v prewhere=%#v group=%d order=%d limit=%d offset=%d", query.from, query.joins[0], query.where, query.prewhere, len(query.groupBy), len(query.orderBy), query.limit, query.offset)
	}
	if want := []string{"id", "tags"}; !reflect.DeepEqual(plan.fields, want) {
		t.Fatalf("columnar ARRAY JOIN plan fields = %#v, want %#v", plan.fields, want)
	}
	resolver := &ch037ColumnarArrayJoinResolver{batch: ch037ColumnarArrayJoinBatch(), columnar: true}
	result, err := ExecuteSQLQuery(ch037ColumnarArrayJoinQuery, resolver)
	if err != nil {
		t.Fatalf("execute columnar ARRAY JOIN: %v", err)
	}
	if resolver.columnarCalls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.columnarCalls)
	}
	if want := []string{"id", "tags"}; !reflect.DeepEqual(resolver.requestedFields, want) {
		t.Fatalf("requested fields = %#v, want %#v", resolver.requestedFields, want)
	}
	want := []SQLRow{
		{"id": int64(1), "tag": "a"},
		{"id": int64(1), "tag": "b"},
		{"id": int64(3), "tag": "c"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("columnar ARRAY JOIN rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH037ColumnarLeftArrayJoinPreservesEmptyAndNullArrays(t *testing.T) {
	resolver := &ch037ColumnarArrayJoinResolver{batch: ch037ColumnarArrayJoinBatch(), columnar: true}
	result, err := ExecuteSQLQuery(ch037ColumnarLeftArrayJoinQuery, resolver)
	if err != nil {
		t.Fatalf("execute columnar LEFT ARRAY JOIN: %v", err)
	}
	want := []SQLRow{
		{"id": int64(1), "tag": "a"},
		{"id": int64(1), "tag": "b"},
		{"id": int64(2), "tag": nil},
		{"id": int64(3), "tag": "c"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("columnar LEFT ARRAY JOIN rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.columnarCalls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.columnarCalls)
	}
}

func TestCH037ColumnarArrayJoinUnsupportedShapeFallsBack(t *testing.T) {
	resolver := &ch037ColumnarArrayJoinResolver{batch: ch037ColumnarArrayJoinBatch(), columnar: true}
	result, err := ExecuteSQLQuery(`
FROM CACHE('items') AS events
ARRAY JOIN events.tags AS tag
SELECT events.id AS id, tag AS tag
WHERE events.id >= 3`, resolver)
	if err != nil {
		t.Fatalf("execute fallback ARRAY JOIN: %v", err)
	}
	want := []SQLRow{{"id": int64(3), "tag": "c"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback ARRAY JOIN rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.columnarCalls != 0 {
		t.Fatalf("columnar calls for unsupported shape = %d, want 0", resolver.columnarCalls)
	}
}

func ch037ColumnarArrayJoinBatch() ColumnarBatch {
	return ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":   {int64(1), int64(2), int64(3)},
			"tags": {[]interface{}{"a", "b"}, []interface{}{}, []interface{}{"c"}},
		},
		Rows: 3,
	}
}

type ch037ColumnarArrayJoinResolver struct {
	batch           ColumnarBatch
	rows            []Row
	columnar        bool
	columnarCalls   int
	requestedFields []string
}

func (resolver *ch037ColumnarArrayJoinResolver) ResolveSQLSource(string, string) ([]Row, error) {
	if resolver.rows == nil {
		resolver.rows = make([]Row, resolver.batch.Rows)
		ids := resolver.batch.Columns["id"]
		tags := resolver.batch.Columns["tags"]
		for index := 0; index < resolver.batch.Rows; index++ {
			resolver.rows[index] = Row{"id": ids[index], "tags": tags[index]}
		}
	}
	return resolver.rows, nil
}

func (resolver *ch037ColumnarArrayJoinResolver) ResolveSQLColumnarSource(_ string, _ string, fields []string) (ColumnarBatch, bool, error) {
	resolver.columnarCalls++
	resolver.requestedFields = append([]string(nil), fields...)
	return resolver.batch, resolver.columnar, nil
}

func BenchmarkCH037ColumnarArrayJoinBaseline(b *testing.B) {
	benchmarkCH037ColumnarArrayJoin(b, true)
}

func BenchmarkCH037ColumnarArrayJoinFallback(b *testing.B) {
	benchmarkCH037ColumnarArrayJoin(b, false)
}

func BenchmarkCH037ColumnarArrayJoinFastPath(b *testing.B) {
	benchmarkCH037ColumnarArrayJoin(b, true)
}

func benchmarkCH037ColumnarArrayJoin(b *testing.B, columnar bool) {
	resolver := &ch037ColumnarArrayJoinResolver{batch: ch037ColumnarArrayJoinBenchmarkBatch(), columnar: columnar}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(ch037ColumnarArrayJoinQuery, resolver)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 8192 {
			b.Fatalf("output rows = %d, want 8192", len(result.Rows))
		}
	}
}

func ch037ColumnarArrayJoinBenchmarkBatch() ColumnarBatch {
	const rows = 2048
	ids := make([]interface{}, rows)
	tags := make([]interface{}, rows)
	for index := 0; index < rows; index++ {
		ids[index] = int64(index)
		tags[index] = []interface{}{
			fmt.Sprintf("tag-%d-a", index),
			fmt.Sprintf("tag-%d-b", index),
			fmt.Sprintf("tag-%d-c", index),
			fmt.Sprintf("tag-%d-d", index),
		}
	}
	return ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "tags": tags}, Rows: rows}
}
