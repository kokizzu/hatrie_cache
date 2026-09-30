package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestC193LimitZeroSkipsSingleSourceRead(t *testing.T) {
	sourceCalls := 0
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		sourceCalls++
		return []SQLRow{{"id": int64(1)}}, nil
	})

	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event SELECT event.id AS id LIMIT 0", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"id"}) {
		t.Fatalf("columns = %#v, want [id]", result.Columns)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %#v, want empty", result.Rows)
	}
	if sourceCalls != 0 {
		t.Fatalf("source calls = %d, want 0", sourceCalls)
	}
}

func TestC193LimitZeroRowsSkipsSingleSourceRead(t *testing.T) {
	sourceCalls := 0
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		sourceCalls++
		return []SQLRow{{"id": int64(1)}}, nil
	})
	rows := 0
	err := ExecuteSQLQueryRows(context.Background(), "FROM CACHE('events') AS event SELECT event.id AS id LIMIT 0", resolver, nil, SQLQueryOptions{}, func([]string, SQLRow) error {
		rows++
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if rows != 0 {
		t.Fatalf("visited rows = %d, want 0", rows)
	}
	if sourceCalls != 0 {
		t.Fatalf("source calls = %d, want 0", sourceCalls)
	}
}

func TestC193LimitZeroDoesNotChangeNonZeroLimit(t *testing.T) {
	sourceCalls := 0
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		sourceCalls++
		return []SQLRow{{"id": int64(1)}, {"id": int64(2)}}, nil
	})

	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event SELECT event.id AS id LIMIT 1", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if want := []SQLRow{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	if sourceCalls != 1 {
		t.Fatalf("source calls = %d, want 1", sourceCalls)
	}
}

func TestC193LimitZeroKeepsStarExpansionOnSourcePath(t *testing.T) {
	sourceCalls := 0
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		sourceCalls++
		return []SQLRow{{"id": int64(1)}}, nil
	})

	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event SELECT * LIMIT 0", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("rows = %#v, want empty", result.Rows)
	}
	if sourceCalls != 1 {
		t.Fatalf("source calls = %d, want 1 for star expansion", sourceCalls)
	}
}

var c193LimitZeroBenchmarkResult SQLQueryResult

func BenchmarkC193LimitZero(b *testing.B) {
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		rows := make([]SQLRow, 16384)
		for index := range rows {
			rows[index] = SQLRow{"id": int64(index)}
		}
		return rows, nil
	})
	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "LimitZero", query: "FROM CACHE('events') AS event SELECT event.id AS id LIMIT 0"},
		{name: "LimitOne", query: "FROM CACHE('events') AS event SELECT event.id AS id LIMIT 1"},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryContext(context.Background(), test.query, resolver, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				c193LimitZeroBenchmarkResult = result
			}
		})
	}
}
