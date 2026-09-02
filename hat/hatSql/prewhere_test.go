package hatSql

import (
	"context"
	"testing"
)

type prewhereStreamResolver struct {
	rows     []SQLRow
	streamed int
	resolved bool
}

func (resolver *prewhereStreamResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	resolver.resolved = true
	return resolver.rows, nil
}

func (resolver *prewhereStreamResolver) StreamSQLSource(ctx context.Context, _ string, _ string, visit func(Row) error) error {
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		resolver.streamed++
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func TestSQLPrewhereStreamsAndLateMaterializesProjection(t *testing.T) {
	resolver := &prewhereStreamResolver{rows: []SQLRow{
		{"id": int64(1), "keep": true, "payload": "first"},
		{"id": int64(2), "keep": false, "payload": "second"},
		{"id": int64(3), "keep": true, "payload": "third"},
		{"id": int64(4), "keep": true, "payload": "fourth"},
	}}
	result, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS e WHERE e.keep = true SELECT e.id, e.payload LIMIT 2", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if resolver.resolved {
		t.Fatal("materialized source resolver was used")
	}
	if resolver.streamed != 4 {
		t.Fatalf("streamed rows = %d, want full source drain of 4", resolver.streamed)
	}
	want := []SQLRow{{"id": int64(1), "payload": "first"}, {"id": int64(3), "payload": "third"}}
	if len(result.Rows) != len(want) || result.Rows[0]["id"] != want[0]["id"] || result.Rows[1]["id"] != want[1]["id"] || result.Rows[1]["payload"] != want[1]["payload"] {
		t.Fatalf("result.Rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLPrewhereHonorsMaxRows(t *testing.T) {
	resolver := &prewhereStreamResolver{rows: []SQLRow{
		{"id": 1, "keep": true},
		{"id": 2, "keep": false},
		{"id": 3, "keep": true},
	}}

	_, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('events') AS e WHERE e.keep = true SELECT e.id LIMIT 1",
		resolver, SQLQueryOptions{MaxRows: 2})
	if err == nil {
		t.Fatal("expected MaxRows error for an oversized streamed source")
	}
}
