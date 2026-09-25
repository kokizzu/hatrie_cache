//go:build ch030baseline

package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCH030ExecutionStepsDefault(b *testing.B) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(cancel)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := control.check(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH030QueryDefault(b *testing.B) {
	const query = "FROM VALUES ('a'), ('b') AS events(kind) SELECT kind"
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, nil, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
