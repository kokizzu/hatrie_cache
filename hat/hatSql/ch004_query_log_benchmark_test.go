package hatSql

import (
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkCH004RotatingSQLQueryLogAppend(b *testing.B) {
	for _, testCase := range []struct {
		name    string
		options SQLQueryLogOptions
	}{
		{name: "bytes", options: SQLQueryLogOptions{MaxFileBytes: 64 << 10, MaxRetainedFiles: 2}},
		{name: "age-check", options: SQLQueryLogOptions{MaxFileAge: 24 * time.Hour, MaxRetainedFiles: 2}},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			log, err := OpenSQLQueryLogWithOptions(filepath.Join(b.TempDir(), "query.log"), testCase.options)
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
		})
	}
}
