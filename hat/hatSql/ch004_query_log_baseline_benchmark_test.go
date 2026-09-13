package hatSql

import (
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkCH004BaselineSQLQueryLogAppend(b *testing.B) {
	log, err := OpenSQLQueryLogWithOptions(filepath.Join(b.TempDir(), "query.log"), SQLQueryLogOptions{})
	if err != nil {
		b.Fatal(err)
	}
	status := SQLQueryStatus{
		QueryID:    "benchmark-query",
		State:      SQLQueryStateSucceeded,
		StartedAt:  time.Unix(100, 0).UTC(),
		FinishedAt: time.Unix(100, int64(time.Millisecond)).UTC(),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := log.Append(status); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := log.Close(); err != nil {
		b.Fatal(err)
	}
}
