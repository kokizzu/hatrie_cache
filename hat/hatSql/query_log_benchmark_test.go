package hatSql

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkCH031SQLQueryLogAppend(b *testing.B) {
	for _, syncOnAppend := range []bool{false, true} {
		b.Run(fmt.Sprintf("sync-%t", syncOnAppend), func(b *testing.B) {
			log, err := OpenSQLQueryLogWithOptions(filepath.Join(b.TempDir(), "query.log"), SQLQueryLogOptions{SyncOnAppend: syncOnAppend})
			if err != nil {
				b.Fatal(err)
			}
			status := SQLQueryStatus{
				QueryID:    "benchmark-query",
				State:      SQLQueryStateSucceeded,
				StartedAt:  time.Unix(100, 0).UTC(),
				FinishedAt: time.Unix(100, int64(time.Millisecond)),
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
