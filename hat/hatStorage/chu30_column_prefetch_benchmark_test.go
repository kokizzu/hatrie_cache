package hatStorage_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var chu30ColumnPrefetchBenchmarkSink int

func chu30ColumnPrefetchBenchmarkInput(b testing.TB) ([]hatStorage.RemotePartColumnReference, []string, hatStorage.RemotePartCacheLoader) {
	b.Helper()
	columns := make([]hatStorage.RemotePartColumnReference, 16)
	requested := make([]string, 4)
	for index := range columns {
		name := fmt.Sprintf("column-%02d", index)
		reference, err := hatStorage.NewRemotePartReference(
			"s3://bucket/parts/"+name,
			"parts/"+name+".bin",
			"sha256:"+name,
			4096,
		)
		if err != nil {
			b.Fatal(err)
		}
		columns[index] = hatStorage.RemotePartColumnReference{Name: name, Reference: reference}
		if index < len(requested) {
			requested[index] = name
		}
	}
	loader := func(_ context.Context, reference hatStorage.RemotePartReference) ([]byte, error) {
		return make([]byte, int(reference.SizeBytes())), nil
	}
	return columns, requested, loader
}

func BenchmarkCHU30ColumnPrefetch(b *testing.B) {
	columns, requested, loader := chu30ColumnPrefetchBenchmarkInput(b)
	allReferences := make([]hatStorage.RemotePartReference, len(columns))
	for index := range columns {
		allReferences[index] = columns[index].Reference
	}
	b.Run("all_columns", func(b *testing.B) {
		b.SetBytes(int64(len(allReferences) * 4096))
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: uint64(len(allReferences) * 4096), MaxEntries: len(allReferences)})
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			if err := cache.Prefetch(context.Background(), allReferences, hatStorage.RemotePartPrefetchOptions{MaxConcurrent: 2, Priority: 1}, loader); err != nil {
				b.Fatal(err)
			}
			chu30ColumnPrefetchBenchmarkSink = cache.Stats().Entries
		}
		b.StopTimer()
	})
	b.Run("selected_columns", func(b *testing.B) {
		b.SetBytes(int64(len(requested) * 4096))
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{MaxBytes: uint64(len(requested) * 4096), MaxEntries: len(requested)})
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			plan, err := cache.PrefetchColumns(context.Background(), columns, requested, hatStorage.RemotePartColumnPrefetchOptions{MaxBytes: uint64(len(requested) * 4096), MaxConcurrent: 2, Priority: 1}, loader)
			if err != nil {
				b.Fatal(err)
			}
			chu30ColumnPrefetchBenchmarkSink = len(plan.References)
		}
		b.StopTimer()
	})
}
