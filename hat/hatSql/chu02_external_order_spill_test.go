package hatSql_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type chu02ExternalStreamResolver struct {
	rows              []hatSql.Row
	streamCalls       int
	materializedCalls int
}

func (resolver *chu02ExternalStreamResolver) ResolveSQLExternalSource(string) ([]hatSql.Row, error) {
	resolver.materializedCalls++
	return resolver.rows, nil
}

func (resolver *chu02ExternalStreamResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (resolver *chu02ExternalStreamResolver) StreamSQLExternalSource(ctx context.Context, _ string, visit func(hatSql.Row) error) error {
	resolver.streamCalls++
	for _, row := range resolver.rows {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func TestCHU02ExternalOrderByUsesBoundedStreamingSpill(t *testing.T) {
	rows := make([]hatSql.Row, 128)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":      int64(index),
			"payload": fmt.Sprintf("payload-%03d", index),
		}
	}
	resolver := &chu02ExternalStreamResolver{rows: rows}
	spillDirectory := t.TempDir()
	options := hatSql.QueryOptions{
		MaxSortBytes:   128,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1 << 20,
	}
	var result []hatSql.Row
	if err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id, event.payload
ORDER BY event.id DESC`, resolver, nil, options, func(_ []string, row hatSql.SQLRow) error {
		result = append(result, hatSql.Row(row))
		return nil
	}); err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if resolver.streamCalls != 1 || resolver.materializedCalls != 0 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want stream-only", resolver.streamCalls, resolver.materializedCalls)
	}
	if len(result) != len(rows) || result[0]["id"] != int64(127) || result[len(result)-1]["id"] != int64(0) {
		t.Fatalf("ordered result endpoints = %#v and %#v, want 127 and 0", result[0]["id"], result[len(result)-1]["id"])
	}
	entries, err := os.ReadDir(spillDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries = %d, want cleanup", len(entries))
	}
}

func TestCHU02ExternalOrderByKeepsStableTieOrderAcrossSpillRuns(t *testing.T) {
	const rowCount = 256
	rows := make([]hatSql.Row, rowCount)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":  int64(index % 4),
			"seq": int64(index),
		}
	}
	spillDirectory := t.TempDir()
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id, event.seq
ORDER BY event.id`, &chu02ExternalStreamResolver{rows: rows}, nil, hatSql.QueryOptions{
		MaxSortBytes:   128,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1 << 20,
	}, func(_ []string, row hatSql.SQLRow) error {
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if len(result) != rowCount {
		t.Fatalf("result rows = %d, want %d", len(result), rowCount)
	}
	outputIndex := 0
	for id := int64(0); id < 4; id++ {
		for sourceIndex := int(id); sourceIndex < rowCount; sourceIndex += 4 {
			row := result[outputIndex]
			if row["id"] != id || row["seq"] != int64(sourceIndex) {
				t.Fatalf("result[%d] = %#v, want id=%d seq=%d", outputIndex, row, id, sourceIndex)
			}
			outputIndex++
		}
	}
	entries, err := os.ReadDir(spillDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries after stable merge = %d, want cleanup", len(entries))
	}
}

func TestCHU02ExternalOrderByCleansSpillAfterQuotaFailure(t *testing.T) {
	rows := make([]hatSql.Row, 128)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index), "payload": fmt.Sprintf("payload-%03d", index)}
	}
	spillDirectory := t.TempDir()
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id, event.payload
ORDER BY event.id DESC`, &chu02ExternalStreamResolver{rows: rows}, nil, hatSql.QueryOptions{
		MaxSortBytes:   128,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  256,
	}, func([]string, hatSql.SQLRow) error { return nil })
	if err == nil {
		t.Fatal("ExecuteSQLQueryRows() error = nil, want spill quota failure")
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries after quota failure = %d, want cleanup", len(entries))
	}
}

func TestCHU02ExternalTablesStreamsReplaceableSnapshot(t *testing.T) {
	tables := hatSql.NewExternalTables()
	oldRows := []hatSql.Row{
		{"id": int64(1)},
		{"id": int64(2)},
		{"id": int64(3)},
	}
	if err := tables.Register("events", hatSql.ExternalTable{Columns: []string{"id"}, Rows: oldRows}); err != nil {
		t.Fatal(err)
	}
	var streamed []int64
	err := tables.StreamSQLExternalSource(context.Background(), "events", func(row hatSql.Row) error {
		streamed = append(streamed, row["id"].(int64))
		if len(streamed) == 1 {
			return tables.Register("events", hatSql.ExternalTable{
				Columns: []string{"id"},
				Rows:    []hatSql.Row{{"id": int64(99)}},
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("StreamSQLExternalSource() error = %v", err)
	}
	if fmt.Sprint(streamed) != "[1 2 3]" {
		t.Fatalf("streamed rows = %v, want old snapshot", streamed)
	}
	current, ok := tables.Get("events")
	if !ok || len(current.Rows) != 1 || current.Rows[0]["id"] != int64(99) {
		t.Fatalf("current table = %#v, want replacement", current)
	}
}

func TestCHU02ExternalTablesStreamHonorsCancellation(t *testing.T) {
	tables := hatSql.NewExternalTables()
	if err := tables.Register("events", hatSql.ExternalTable{
		Columns: []string{"id"},
		Rows:    []hatSql.Row{{"id": int64(1)}, {"id": int64(2)}},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	visited := 0
	err := tables.StreamSQLExternalSource(ctx, "events", func(hatSql.Row) error {
		visited++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StreamSQLExternalSource() error = %v, want context.Canceled", err)
	}
	if visited != 1 {
		t.Fatalf("visited rows = %d, want 1", visited)
	}
}

func TestCHU02ExternalOrderByKeepsMaterializedFallback(t *testing.T) {
	rows := []hatSql.Row{{"id": int64(2)}, {"id": int64(1)}}
	resolver := &chu02ExternalStreamResolver{rows: rows}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id
ORDER BY event.id DESC`, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if resolver.streamCalls != 0 || resolver.materializedCalls != 1 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want materialized-only", resolver.streamCalls, resolver.materializedCalls)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(2) || result.Rows[1]["id"] != int64(1) {
		t.Fatalf("ordered result = %#v, want 2 then 1", result.Rows)
	}
}
