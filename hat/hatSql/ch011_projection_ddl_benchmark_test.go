package hatSql

import (
	"context"
	"strconv"
	"testing"
)

const ch011ProjectionQuery = "FROM CACHE('events') SELECT id, score ORDER BY score DESC LIMIT 32"

func BenchmarkCH011ProjectionFullScan(b *testing.B) {
	session := NewSQLSession(newCH011ProjectionSource(4096))
	benchmarkCH011ProjectionQuery(b, session, ch011ProjectionQuery)
}

func BenchmarkCH011ProjectionHit(b *testing.B) {
	session := NewSQLSession(newCH011ProjectionSource(4096))
	if _, err := session.Execute(context.Background(), "CREATE PROJECTION top_events AS "+ch011ProjectionQuery, nil, SQLQueryOptions{}); err != nil {
		b.Fatal(err)
	}
	benchmarkCH011ProjectionQuery(b, session, ch011ProjectionQuery)
}

func benchmarkCH011ProjectionQuery(b *testing.B, session *SQLSession, query string) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := session.Execute(context.Background(), query, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 32 {
			b.Fatalf("result rows = %d, want 32", len(result.Rows))
		}
	}
}

func newCH011ProjectionSource(rowCount int) *ch011ProjectionSource {
	rows := make([]Row, rowCount)
	for index := range rows {
		rows[index] = Row{"id": int64(index), "score": int64(index), "label": "event-" + strconv.Itoa(index)}
	}
	return &ch011ProjectionSource{rows: rows, version: 1}
}
