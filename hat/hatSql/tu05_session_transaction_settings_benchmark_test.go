package hatSql

import (
	"context"
	"testing"
	"time"
)

func BenchmarkTU05SessionExecuteBaseline(b *testing.B) {
	session := NewSQLSession(nil)
	if err := session.CreateTemporaryTable("rows", []Row{{"id": int64(1)}}); err != nil {
		b.Fatal(err)
	}
	query := `FROM CACHE('rows') SELECT id`
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := session.Execute(ctx, query, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU05SessionExecuteWithTimeout(b *testing.B) {
	session, err := NewSQLSessionWithTransactionSettings(nil, SQLSessionTransactionSettings{Timeout: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	if err := session.CreateTemporaryTable("rows", []Row{{"id": int64(1)}}); err != nil {
		b.Fatal(err)
	}
	query := `FROM CACHE('rows') SELECT id`
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := session.Execute(ctx, query, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
