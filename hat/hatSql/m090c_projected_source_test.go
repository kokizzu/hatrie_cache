package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

type m090cProjectedSourceResolver struct {
	rows             []Row
	projectedCalls   int
	projectedFields  [][]string
	fullCalls        int
	projectedEnabled bool
}

type m090cLegacySourceResolver struct {
	rows []Row
}

func (resolver *m090cLegacySourceResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return CloneRows(resolver.rows), nil
}

func (resolver *m090cProjectedSourceResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.fullCalls++
	return CloneRows(resolver.rows), nil
}

func (resolver *m090cProjectedSourceResolver) ResolveSQLProjectedSource(_, _ string, fields []string) ([]Row, bool, error) {
	resolver.projectedCalls++
	resolver.projectedFields = append(resolver.projectedFields, append([]string(nil), fields...))
	if !resolver.projectedEnabled {
		return nil, false, nil
	}
	projected := make([]Row, len(resolver.rows))
	for index, sourceRow := range resolver.rows {
		row := make(Row, len(fields))
		for _, field := range fields {
			if value, ok := sourceRow[field]; ok {
				row[field] = value
			}
		}
		projected[index] = row
	}
	return projected, true, nil
}

func TestM090cProjectedSourceResolverReceivesOnlyRequiredFields(t *testing.T) {
	resolver := &m090cProjectedSourceResolver{
		rows: []Row{
			{"id": int64(1), "region": "eu", "payload": "large-a"},
			{"id": int64(2), "region": "us", "payload": "large-b"},
			{"id": int64(3), "region": "eu", "payload": "large-c"},
		},
		projectedEnabled: true,
	}
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("projected query error = %v", err)
	}
	wantRows := []Row{{"id": int64(1)}, {"id": int64(3)}}
	if !reflect.DeepEqual(result.Rows, wantRows) {
		t.Fatalf("projected rows = %#v, want %#v", result.Rows, wantRows)
	}
	if resolver.projectedCalls != 1 || resolver.fullCalls != 0 {
		t.Fatalf("resolver calls projected=%d full=%d, want projected=1 full=0", resolver.projectedCalls, resolver.fullCalls)
	}
	if want := [][]string{{"id", "region"}}; !reflect.DeepEqual(resolver.projectedFields, want) {
		t.Fatalf("projected fields = %#v, want %#v", resolver.projectedFields, want)
	}
}

func TestM090cProjectedSourceResolverFeedsAutomaticNativeDataflow(t *testing.T) {
	resolver := &m090cProjectedSourceResolver{
		rows: []Row{
			{"id": int64(1), "region": "eu", "payload": "large-a"},
			{"id": int64(2), "region": "us", "payload": "large-b"},
		},
		projectedEnabled: true,
	}
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("automatic native projected query error = %v", err)
	}
	if want := []Row{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("automatic native rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.projectedCalls != 1 || resolver.fullCalls != 0 {
		t.Fatalf("automatic native resolver calls projected=%d full=%d, want projected=1 full=0", resolver.projectedCalls, resolver.fullCalls)
	}
}

func TestM090cProjectedSourceResolverFallsBackWhenUnavailable(t *testing.T) {
	resolver := &m090cProjectedSourceResolver{
		rows: []Row{{"id": int64(1), "region": "eu", "payload": "large-a"}},
	}
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'",
		resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback query error = %v", err)
	}
	if want := []Row{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.projectedCalls != 1 || resolver.fullCalls != 1 {
		t.Fatalf("resolver calls projected=%d full=%d, want projected=1 full=1", resolver.projectedCalls, resolver.fullCalls)
	}
}

func TestM090cProjectedSourceResolverSkipsUnsupportedProjectionShapes(t *testing.T) {
	resolver := &m090cProjectedSourceResolver{
		rows:             []Row{{"id": int64(1), "region": "eu"}},
		projectedEnabled: true,
	}
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS event SELECT event.id + 1 AS next_id WHERE event.region = 'eu'",
		resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("unsupported-shape query error = %v", err)
	}
	if want := []Row{{"next_id": int64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("unsupported-shape rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.projectedCalls != 0 || resolver.fullCalls != 1 {
		t.Fatalf("resolver calls projected=%d full=%d, want projected=0 full=1", resolver.projectedCalls, resolver.fullCalls)
	}
}

func BenchmarkM090cProjectedSource(b *testing.B) {
	rows := m090cBenchmarkRows()
	query := "FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'"
	for _, variant := range []struct {
		name   string
		fields []string
		make   func() SQLSourceResolver
	}{
		{name: "legacy-full-row", fields: []string{"id", "region", "payload", "metadata", "unused"}, make: func() SQLSourceResolver {
			return &m090cLegacySourceResolver{rows: rows}
		}},
		{name: "projected-columns", fields: []string{"id", "region"}, make: func() SQLSourceResolver {
			return &m090cProjectedSourceResolver{rows: rows, projectedEnabled: true}
		}},
	} {
		b.Run(variant.name, func(b *testing.B) {
			resolver := variant.make()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != len(rows)/2 {
					b.Fatalf("rows = %d, want %d", len(result.Rows), len(rows)/2)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(m090cSourceBytes(rows, variant.fields)), "source-bytes/op")
		})
	}
}

func BenchmarkM090cProjectedSourceAutomaticNative(b *testing.B) {
	rows := m090cBenchmarkRows()
	query := "FROM CACHE('events') AS event SELECT event.id WHERE event.region = 'eu'"
	for _, variant := range []struct {
		name   string
		fields []string
		make   func() SQLSourceResolver
	}{
		{name: "legacy-full-row", fields: []string{"id", "region", "payload", "metadata", "unused"}, make: func() SQLSourceResolver {
			return &m090cLegacySourceResolver{rows: rows}
		}},
		{name: "projected-columns", fields: []string{"id", "region"}, make: func() SQLSourceResolver {
			return &m090cProjectedSourceResolver{rows: rows, projectedEnabled: true}
		}},
	} {
		b.Run(variant.name, func(b *testing.B) {
			resolver := variant.make()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != len(rows)/2 {
					b.Fatalf("rows = %d, want %d", len(result.Rows), len(rows)/2)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(m090cSourceBytes(rows, variant.fields)), "source-bytes/op")
		})
	}
}

func m090cBenchmarkRows() []Row {
	rows := make([]Row, 4096)
	for index := range rows {
		rows[index] = Row{
			"id":       int64(index),
			"region":   []string{"eu", "us"}[index%2],
			"payload":  fmt.Sprintf("payload-%08d-with-extra-data", index),
			"metadata": fmt.Sprintf("metadata-%08d-with-extra-data", index),
			"unused":   fmt.Sprintf("unused-%08d-with-extra-data", index),
		}
	}
	return rows
}

func m090cSourceBytes(rows []Row, fields []string) int {
	bytes := 0
	for _, row := range rows {
		for _, field := range fields {
			value, ok := row[field]
			if ok {
				bytes += len(field) + len(fmt.Sprint(value))
			}
		}
	}
	return bytes
}
