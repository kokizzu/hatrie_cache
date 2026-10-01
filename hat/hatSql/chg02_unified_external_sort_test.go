package hatSql_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type chg02StarStreamResolver struct {
	rows              []hatSql.Row
	streamCalls       int
	materializedCalls int
}

func (resolver *chg02StarStreamResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	resolver.materializedCalls++
	return nil, errors.New("CH-G02 star test must not materialize the source")
}

func (resolver *chg02StarStreamResolver) StreamSQLSource(ctx context.Context, _, _ string, visit func(hatSql.Row) error) error {
	resolver.streamCalls++
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func TestCHG02ExternalOrderBySelectStarUsesStreamingSpill(t *testing.T) {
	resolver := &chg02StarStreamResolver{rows: []hatSql.Row{
		{"id": int64(2), "name": "bravo"},
		{"id": int64(1), "name": "alpha"},
		{"id": int64(3), "name": "charlie"},
	}}
	spillDirectory := t.TempDir()
	var columns []string
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM CACHE('events') AS event
SELECT *
ORDER BY event.id DESC`, resolver, nil, hatSql.QueryOptions{
		MaxSortBytes:   1,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1 << 20,
	}, func(gotColumns []string, row hatSql.SQLRow) error {
		columns = append([]string(nil), gotColumns...)
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if resolver.streamCalls != 1 || resolver.materializedCalls != 0 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want stream-only", resolver.streamCalls, resolver.materializedCalls)
	}
	if want := []string{"id", "name"}; !reflect.DeepEqual(columns, want) {
		t.Fatalf("columns = %#v, want %#v", columns, want)
	}
	want := []hatSql.Row{
		{"id": int64(3), "name": "charlie"},
		{"id": int64(2), "name": "bravo"},
		{"id": int64(1), "name": "alpha"},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("rows = %#v, want %#v", result, want)
	}
	entries, err := os.ReadDir(spillDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries = %d, want cleanup", len(entries))
	}
}

func TestCHG02ExternalOrderBySelectStarCleansSpillAfterCancellation(t *testing.T) {
	rows := make([]hatSql.Row, 32)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index), "name": "event"}
	}
	spillDirectory := t.TempDir()
	callbackErr := errors.New("stop CH-G02 star stream")
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM CACHE('events') AS event
SELECT *
ORDER BY event.id DESC`, &chg02StarStreamResolver{rows: rows}, nil, hatSql.QueryOptions{
		MaxSortBytes:   1,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1 << 20,
	}, func([]string, hatSql.SQLRow) error {
		return callbackErr
	})
	if !errors.Is(err, callbackErr) {
		t.Fatalf("ExecuteSQLQueryRows() error = %v, want callback error", err)
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries after callback error = %d, want cleanup", len(entries))
	}
}

func TestCHG02ValuesOrderBySelectStarUsesStreamingSpill(t *testing.T) {
	spillDirectory := t.TempDir()
	var columns []string
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM VALUES (2, 'bravo'), (1, 'alpha'), (3, 'charlie') AS event(id, name)
SELECT *
ORDER BY event.id DESC`, nil, nil, hatSql.QueryOptions{
		MaxSortBytes:   1,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1 << 20,
	}, func(gotColumns []string, row hatSql.SQLRow) error {
		columns = append([]string(nil), gotColumns...)
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if want := []string{"id", "name"}; !reflect.DeepEqual(columns, want) {
		t.Fatalf("columns = %#v, want %#v", columns, want)
	}
	want := []hatSql.Row{
		{"id": int64(3), "name": "charlie"},
		{"id": int64(2), "name": "bravo"},
		{"id": int64(1), "name": "alpha"},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("rows = %#v, want %#v", result, want)
	}
}
