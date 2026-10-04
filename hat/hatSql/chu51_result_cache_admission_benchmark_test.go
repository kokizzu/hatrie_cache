package hatSql

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func BenchmarkCHU51ResultCacheAdmission(b *testing.B) {
	cache, err := NewSQLResultCacheWithAdmission(1024, ResultCacheAdmissionPolicy{MinExecutionDuration: time.Millisecond})
	if err != nil {
		b.Fatal(err)
	}
	version := func() (string, bool) { return "v1", true }
	result := QueryResult{Rows: []Row{{"id": int64(1)}}}
	execute := func(context.Context) (QueryResult, error) { return result, nil }
	keys := make([]string, 1024)
	for index := range keys {
		keys[index] = "query-" + strconv.Itoa(index)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := cache.ExecuteVersioned(context.Background(), keys[iteration%len(keys)], version, execute); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(cache.Stats().Entries), "entries")
	b.ReportMetric(float64(cache.AdmissionStats().Rejected), "rejected")
}
