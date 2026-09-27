package hatSql

import (
	"context"
	"reflect"
	"testing"
)

type m090dFilteredProjectedResolver struct {
	rows            []Row
	filteredCalls   int
	contextCalls    int
	fullCalls       int
	projectedFields [][]string
	predicates      [][]SQLPartitionPredicate
}

func (resolver *m090dFilteredProjectedResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.fullCalls++
	return CloneRows(resolver.rows), nil
}

func (resolver *m090dFilteredProjectedResolver) ResolveSQLProjectedSourceWithPredicates(name, key string, fields []string, predicates []SQLPartitionPredicate) ([]Row, bool, error) {
	resolver.filteredCalls++
	return resolver.resolveFiltered(name, key, fields, predicates), true, nil
}

func (resolver *m090dFilteredProjectedResolver) ResolveSQLProjectedSourceContextWithPredicates(ctx context.Context, name, key string, fields []string, predicates []SQLPartitionPredicate) ([]Row, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	resolver.contextCalls++
	return resolver.resolveFiltered(name, key, fields, predicates), true, nil
}

func (resolver *m090dFilteredProjectedResolver) resolveFiltered(_ string, _ string, fields []string, predicates []SQLPartitionPredicate) []Row {
	resolver.projectedFields = append(resolver.projectedFields, append([]string(nil), fields...))
	clonedPredicates := make([]SQLPartitionPredicate, len(predicates))
	for index, predicate := range predicates {
		clonedPredicates[index] = predicate
		clonedPredicates[index].Values = append([]interface{}(nil), predicate.Values...)
	}
	resolver.predicates = append(resolver.predicates, clonedPredicates)
	result := make([]Row, 0, len(resolver.rows))
	for _, sourceRow := range resolver.rows {
		if sourceRow["region"] != "eu" {
			continue
		}
		row := make(Row, len(fields))
		for _, field := range fields {
			if value, ok := sourceRow[field]; ok {
				row[field] = value
			}
		}
		result = append(result, row)
	}
	return result
}

func TestM090dPredicateProjectedSourceResolverPrunesBeforeTransfer(t *testing.T) {
	resolver := &m090dFilteredProjectedResolver{
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
		t.Fatalf("filtered projected query error = %v", err)
	}
	if want := []Row{{"id": int64(1)}, {"id": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered projected rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.contextCalls != 1 || resolver.filteredCalls != 0 || resolver.fullCalls != 0 {
		t.Fatalf("resolver calls context=%d filtered=%d full=%d, want context=1 filtered=0 full=0", resolver.contextCalls, resolver.filteredCalls, resolver.fullCalls)
	}
	if want := [][]string{{"id", "region"}}; !reflect.DeepEqual(resolver.projectedFields, want) {
		t.Fatalf("projected fields = %#v, want %#v", resolver.projectedFields, want)
	}
	if len(resolver.predicates) != 1 || len(resolver.predicates[0]) != 1 {
		t.Fatalf("predicates = %#v, want one predicate", resolver.predicates)
	}
	predicate := resolver.predicates[0][0]
	if predicate.Field != "region" || predicate.Operator != "=" || !reflect.DeepEqual(predicate.Values, []interface{}{"eu"}) {
		t.Fatalf("predicate = %#v, want region = eu", predicate)
	}
}

func TestM090dPredicateProjectedSourceResolverHonorsCancellation(t *testing.T) {
	resolver := &m090dFilteredProjectedResolver{rows: []Row{{"id": int64(1), "region": "eu"}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ExecuteSQLQueryContext(ctx,
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err == nil {
		t.Fatal("canceled filtered projected query error = nil")
	}
	if resolver.contextCalls != 0 || resolver.filteredCalls != 0 || resolver.fullCalls != 0 {
		t.Fatalf("canceled resolver calls context=%d filtered=%d full=%d, want no source call", resolver.contextCalls, resolver.filteredCalls, resolver.fullCalls)
	}
}

type m090dDecliningFilteredResolver struct {
	rows           []Row
	filteredCalls  int
	projectedCalls int
}

func (resolver *m090dDecliningFilteredResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return CloneRows(resolver.rows), nil
}

func (resolver *m090dDecliningFilteredResolver) ResolveSQLProjectedSource(name, key string, fields []string) ([]Row, bool, error) {
	resolver.projectedCalls++
	rows := make([]Row, len(resolver.rows))
	for index, sourceRow := range resolver.rows {
		rows[index] = make(Row, len(fields))
		for _, field := range fields {
			rows[index][field] = sourceRow[field]
		}
	}
	return rows, true, nil
}

func (resolver *m090dDecliningFilteredResolver) ResolveSQLProjectedSourceWithPredicates(string, string, []string, []SQLPartitionPredicate) ([]Row, bool, error) {
	resolver.filteredCalls++
	return nil, false, nil
}

func TestM090dPredicateProjectedSourceResolverFallsBackWhenUnavailable(t *testing.T) {
	resolver := &m090dDecliningFilteredResolver{
		rows: []Row{
			{"id": int64(1), "region": "eu"},
			{"id": int64(2), "region": "us"},
		},
	}
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("declining filtered query error = %v", err)
	}
	if want := []Row{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("declining filtered rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.filteredCalls != 1 || resolver.projectedCalls != 1 {
		t.Fatalf("resolver calls filtered=%d projected=%d, want filtered=1 projected=1", resolver.filteredCalls, resolver.projectedCalls)
	}
}

func TestM090dPredicateProjectedSourceResolverForwardsThroughSession(t *testing.T) {
	resolver := &m090dFilteredProjectedResolver{
		rows: []Row{
			{"id": int64(1), "region": "eu"},
			{"id": int64(2), "region": "us"},
		},
	}
	session := NewSQLSession(resolver)
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		session, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("session filtered query error = %v", err)
	}
	if want := []Row{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("session filtered rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.contextCalls != 1 || resolver.fullCalls != 0 {
		t.Fatalf("session resolver calls context=%d full=%d, want context=1 full=0", resolver.contextCalls, resolver.fullCalls)
	}
}

func BenchmarkM090dFilteredProjectedSource(b *testing.B) {
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{
			"id":       int64(index),
			"region":   []string{"eu", "us"}[index%2],
			"payload":  "payload-with-extra-data-that-should-not-cross-the-boundary",
			"metadata": "metadata-with-extra-data-that-should-not-cross-the-boundary",
		}
	}
	query := "FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'"
	b.Run("legacy-full-row", func(b *testing.B) {
		resolver := &m090dLegacyResolver{rows: rows}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
			if err != nil || len(result.Rows) != len(rows)/2 {
				b.Fatalf("legacy result = %d/%v", len(result.Rows), err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(m090dRowsBytes(rows, false)), "source-bytes/op")
	})
	b.Run("filtered-projected", func(b *testing.B) {
		resolver := &m090dFilteredProjectedResolver{rows: rows}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
			if err != nil || len(result.Rows) != len(rows)/2 {
				b.Fatalf("filtered result = %d/%v", len(result.Rows), err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(m090dRowsBytes(rows, true)), "source-bytes/op")
	})
}

type m090dLegacyResolver struct{ rows []Row }

func (resolver *m090dLegacyResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return CloneRows(resolver.rows), nil
}

func m090dRowsBytes(rows []Row, projected bool) int {
	bytes := 0
	for _, row := range rows {
		if projected {
			if row["region"] != "eu" {
				continue
			}
			bytes += len("id") + len("region")
			if value, ok := row["region"].(string); ok {
				bytes += len(value)
			}
			continue
		}
		bytes += len(row)
		for key, value := range row {
			bytes += len(key)
			if text, ok := value.(string); ok {
				bytes += len(text)
			}
		}
	}
	return bytes
}
