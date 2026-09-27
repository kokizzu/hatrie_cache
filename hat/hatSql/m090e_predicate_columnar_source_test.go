package hatSql

import (
	"context"
	"reflect"
	"testing"
)

type m090ePredicateColumnarResolver struct {
	rows           []Row
	filteredCalls  int
	columnarCalls  int
	filteredFields [][]string
	filteredPreds  [][]SQLPartitionPredicate
}

func (resolver *m090ePredicateColumnarResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return CloneRows(resolver.rows), nil
}

func (resolver *m090ePredicateColumnarResolver) ResolveSQLColumnarSource(_ string, _ string, fields []string) (ColumnarBatch, bool, error) {
	resolver.columnarCalls++
	return m090eColumnarBatch(resolver.rows, fields), true, nil
}

func (resolver *m090ePredicateColumnarResolver) ResolveSQLColumnarSourceWithPredicates(_ string, _ string, fields []string, predicates []SQLPartitionPredicate) (ColumnarBatch, bool, error) {
	resolver.filteredCalls++
	resolver.filteredFields = append(resolver.filteredFields, append([]string(nil), fields...))
	cloned := make([]SQLPartitionPredicate, len(predicates))
	for index, predicate := range predicates {
		cloned[index] = predicate
		cloned[index].Values = append([]interface{}(nil), predicate.Values...)
	}
	resolver.filteredPreds = append(resolver.filteredPreds, cloned)
	filtered := make([]Row, 0, len(resolver.rows))
	for _, row := range resolver.rows {
		if row["region"] == "eu" {
			filtered = append(filtered, row)
		}
	}
	return m090eColumnarBatch(filtered, fields), true, nil
}

func m090eColumnarBatch(rows []Row, fields []string) ColumnarBatch {
	columns := make(map[string][]interface{}, len(fields))
	for _, field := range fields {
		values := make([]interface{}, len(rows))
		for index, row := range rows {
			values[index] = row[field]
		}
		columns[field] = values
	}
	return ColumnarBatch{Columns: columns, Rows: len(rows)}
}

func TestM090ePredicateColumnarSourcePrunesBeforeTransfer(t *testing.T) {
	resolver := &m090ePredicateColumnarResolver{
		rows: []Row{
			{"id": int64(1), "region": "eu", "payload": "large-a"},
			{"id": int64(2), "region": "us", "payload": "large-b"},
			{"id": int64(3), "region": "eu", "payload": "large-c"},
		},
	}
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("filtered columnar query error = %v", err)
	}
	if want := []Row{{"id": int64(1)}, {"id": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered columnar rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.filteredCalls != 1 || resolver.columnarCalls != 0 {
		t.Fatalf("resolver calls filtered=%d columnar=%d, want filtered=1 columnar=0", resolver.filteredCalls, resolver.columnarCalls)
	}
	if want := [][]string{{"region", "id"}}; !reflect.DeepEqual(resolver.filteredFields, want) {
		t.Fatalf("filtered fields = %#v, want %#v", resolver.filteredFields, want)
	}
	if len(resolver.filteredPreds) != 1 || len(resolver.filteredPreds[0]) != 1 {
		t.Fatalf("filtered predicates = %#v, want one predicate", resolver.filteredPreds)
	}
	predicate := resolver.filteredPreds[0][0]
	if predicate.Field != "region" || predicate.Operator != "=" || !reflect.DeepEqual(predicate.Values, []interface{}{"eu"}) {
		t.Fatalf("filtered predicate = %#v, want region = eu", predicate)
	}
}

type m090eDecliningPredicateColumnarResolver struct {
	*m090ePredicateColumnarResolver
	declinedCalls int
}

func (resolver *m090eDecliningPredicateColumnarResolver) ResolveSQLColumnarSourceWithPredicates(string, string, []string, []SQLPartitionPredicate) (ColumnarBatch, bool, error) {
	resolver.declinedCalls++
	return ColumnarBatch{}, false, nil
}

func TestM090ePredicateColumnarSourceFallsBackWhenUnavailable(t *testing.T) {
	resolver := &m090eDecliningPredicateColumnarResolver{
		m090ePredicateColumnarResolver: &m090ePredicateColumnarResolver{
			rows: []Row{{"id": int64(1), "region": "eu"}, {"id": int64(2), "region": "us"}},
		},
	}
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("declining columnar query error = %v", err)
	}
	if want := []Row{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("declining columnar rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.declinedCalls != 1 || resolver.columnarCalls != 1 {
		t.Fatalf("resolver calls declined=%d columnar=%d, want declined=1 columnar=1", resolver.declinedCalls, resolver.columnarCalls)
	}
}

func TestM090ePredicateColumnarSourceForwardsThroughSession(t *testing.T) {
	resolver := &m090ePredicateColumnarResolver{
		rows: []Row{{"id": int64(1), "region": "eu"}, {"id": int64(2), "region": "us"}},
	}
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		NewSQLSession(resolver), SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("session columnar query error = %v", err)
	}
	if want := []Row{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("session columnar rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.filteredCalls != 1 || resolver.columnarCalls != 0 {
		t.Fatalf("session resolver calls filtered=%d columnar=%d, want filtered=1 columnar=0", resolver.filteredCalls, resolver.columnarCalls)
	}
}

func BenchmarkM090ePredicateColumnarSource(b *testing.B) {
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{
			"id":       int64(index),
			"region":   []string{"eu", "us"}[index%2],
			"payload":  "payload-with-extra-data-that-stays-out-of-the-query",
			"metadata": "metadata-with-extra-data-that-stays-out-of-the-query",
		}
	}
	query := "FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'"
	b.Run("legacy-full-columnar", func(b *testing.B) {
		resolver := &m090eLegacyColumnarResolver{rows: rows}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
			if err != nil || len(result.Rows) != len(rows)/2 {
				b.Fatalf("legacy result = %d/%v", len(result.Rows), err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(m090eColumnarBytes(rows, false)), "source-bytes/op")
		b.ReportMetric(float64(len(rows)), "source-rows/op")
	})
	b.Run("predicate-columnar", func(b *testing.B) {
		resolver := &m090ePredicateColumnarResolver{rows: rows}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
			if err != nil || len(result.Rows) != len(rows)/2 {
				b.Fatalf("filtered result = %d/%v", len(result.Rows), err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(m090eColumnarBytes(rows, true)), "source-bytes/op")
		b.ReportMetric(float64(len(rows)/2), "source-rows/op")
	})
}

type m090eLegacyColumnarResolver struct{ rows []Row }

func (resolver *m090eLegacyColumnarResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return CloneRows(resolver.rows), nil
}

func (resolver *m090eLegacyColumnarResolver) ResolveSQLColumnarSource(_ string, _ string, fields []string) (ColumnarBatch, bool, error) {
	return m090eColumnarBatch(resolver.rows, fields), true, nil
}

func m090eColumnarBytes(rows []Row, filtered bool) int {
	bytes := 0
	for _, row := range rows {
		if filtered && row["region"] != "eu" {
			continue
		}
		bytes += 8 + len("id")
		bytes += len("region")
		if region, ok := row["region"].(string); ok {
			bytes += len(region)
		}
	}
	return bytes
}
