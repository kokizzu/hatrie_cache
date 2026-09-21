package hatSql_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestC228ExternalSortPreservesStableOrderAcrossRuns(t *testing.T) {
	const rowCount = 192
	rows := make([]hatSql.Row, rowCount)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":    int64(index),
			"score": int64(index % 3),
			"tag":   fmt.Sprintf("row-%03d", index),
		}
	}
	resolver := &chu02ExternalStreamResolver{rows: rows}
	spillDirectory := t.TempDir()
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id, event.score, event.tag
ORDER BY event.score ASC`, resolver, nil, hatSql.QueryOptions{
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
	if resolver.streamCalls != 1 || resolver.materializedCalls != 0 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want stream-only", resolver.streamCalls, resolver.materializedCalls)
	}
	if len(result) != rowCount {
		t.Fatalf("result rows = %d, want %d", len(result), rowCount)
	}
	position := 0
	for score := int64(0); score < 3; score++ {
		for id := score; id < rowCount; id += 3 {
			if got := result[position]; got["score"] != score || got["id"] != id {
				t.Fatalf("result[%d] = %#v, want id=%d score=%d", position, got, id, score)
			}
			position++
		}
	}
	entries, err := os.ReadDir(spillDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries = %d, want cleanup", len(entries))
	}
}
