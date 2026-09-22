package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func m220BenchmarkIndexedViews(b *testing.B) *hatSql.MaterializedViews {
	b.Helper()
	views := m219BenchmarkViews(b)
	if err := views.CreatePointLookupIndex(m219BenchmarkPointLookupDefinition()); err != nil {
		b.Fatalf("CreatePointLookupIndex() error = %v", err)
	}
	return views
}

func BenchmarkM220PointLookupRetirement(b *testing.B) {
	b.Run("one_shot_lookup", func(b *testing.B) {
		views := m220BenchmarkIndexedViews(b)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, found, err := views.LookupPoint("people_by_id", "9999")
			if err != nil || !found || len(result.Rows) != 1 {
				b.Fatalf("LookupPoint() = %#v, %v, %v", result, found, err)
			}
		}
	})

	b.Run("leased_lookup", func(b *testing.B) {
		views := m220BenchmarkIndexedViews(b)
		reader, err := views.AcquirePointLookupReader("people_by_id")
		if err != nil {
			b.Fatalf("AcquirePointLookupReader() error = %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, found, err := reader.LookupPoint("9999")
			if err != nil || !found || len(result.Rows) != 1 {
				b.Fatalf("reader.LookupPoint() = %#v, %v, %v", result, found, err)
			}
		}
		b.StopTimer()
		reader.Close()
	})

	b.Run("retire_without_readers", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			views := m220BenchmarkIndexedViews(b)
			b.StartTimer()
			retirement, err := views.StartPointLookupIndexRetirement("people_by_id")
			if err == nil {
				_, err = retirement.Wait(context.Background())
			}
			b.StopTimer()
			if err != nil {
				b.Fatalf("retirement error = %v", err)
			}
		}
	})
}
