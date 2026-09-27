package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func m065FirstLastWindowQuery(rows int) string {
	values := make([]string, rows)
	for index := range values {
		values[index] = strconv.Itoa(index)
	}
	return m065FirstLastWindowQueryValues(values...)
}

func m065FirstLastWindowQueryValues(values ...string) string {
	var query strings.Builder
	query.WriteString("FROM VALUES ")
	for index, value := range values {
		if index != 0 {
			query.WriteString(", ")
		}
		fmt.Fprintf(&query, "(%s)", value)
	}
	query.WriteString(" AS src(value) SELECT src.value AS value, FIRST_VALUE(src.value) OVER () AS first_value, LAST_VALUE(src.value) OVER () AS last_value")
	return query.String()
}

func TestM065FirstLastWindowUsesRunningStream(t *testing.T) {
	query, err := parseSQLQuery(m065FirstLastWindowQuery(4))
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if !sqlRunningWindowStreamable(query, nil) {
		t.Fatal("FIRST_VALUE/LAST_VALUE query is not eligible for the bounded running-window stream")
	}
}

func TestM065FirstLastWindowStreamingMatchesMaterialized(t *testing.T) {
	query := m065FirstLastWindowQuery(8)
	want, err := ExecuteSQLQuery(query, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	var got []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), query, nil, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if !reflect.DeepEqual(got, want.Rows) {
		t.Fatalf("stream rows = %#v, want materialized rows %#v", got, want.Rows)
	}
}

func TestM065FirstLastWindowPreservesNulls(t *testing.T) {
	query := m065FirstLastWindowQueryValues("NULL", "2", "NULL", "4")
	want := []SQLRow{
		{"value": nil, "first_value": nil, "last_value": nil},
		{"value": int64(2), "first_value": nil, "last_value": int64(2)},
		{"value": nil, "first_value": nil, "last_value": nil},
		{"value": int64(4), "first_value": nil, "last_value": int64(4)},
	}
	materialized, err := ExecuteSQLQuery(query, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if !reflect.DeepEqual(materialized.Rows, want) {
		t.Fatalf("materialized rows = %#v, want %#v", materialized.Rows, want)
	}
	var streamed []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), query, nil, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		streamed = append(streamed, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if !reflect.DeepEqual(streamed, want) {
		t.Fatalf("streamed rows = %#v, want %#v", streamed, want)
	}
}

func TestM065FirstLastWindowUsesFilteredRows(t *testing.T) {
	query := m065FirstLastWindowQueryValues("1", "2", "3") + " WHERE src.value >= 2"
	want := []SQLRow{
		{"value": int64(2), "first_value": int64(2), "last_value": int64(2)},
		{"value": int64(3), "first_value": int64(2), "last_value": int64(3)},
	}
	materialized, err := ExecuteSQLQuery(query, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if !reflect.DeepEqual(materialized.Rows, want) {
		t.Fatalf("materialized rows = %#v, want %#v", materialized.Rows, want)
	}
	var streamed []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), query, nil, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		streamed = append(streamed, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if !reflect.DeepEqual(streamed, want) {
		t.Fatalf("streamed rows = %#v, want %#v", streamed, want)
	}
}

func BenchmarkM065FirstLastWindow(b *testing.B) {
	query := m065FirstLastWindowQuery(4096)
	b.Run("materialized", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			result, err := ExecuteSQLQuery(query, nil)
			if err != nil || len(result.Rows) != 4096 {
				b.Fatalf("ExecuteSQLQuery() rows/error = %d/%v", len(result.Rows), err)
			}
		}
	})
	b.Run("streamed", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			rows := 0
			err := ExecuteSQLQueryRows(context.Background(), query, nil, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
				rows++
				return nil
			})
			if err != nil || rows != 4096 {
				b.Fatalf("ExecuteSQLQueryRows() rows/error = %d/%v", rows, err)
			}
		}
	})
}
