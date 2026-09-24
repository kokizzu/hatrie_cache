package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLSnapshotReadinessSnapshot(b *testing.B) {
	registry := newM035ReadyRegistry(b, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		snapshot, err := registry.Snapshot("query")
		if err != nil || !snapshot.Ready {
			b.Fatalf("Snapshot() = %#v/%v", snapshot, err)
		}
	}
}

func BenchmarkSQLSnapshotReadinessSnapshotLarge(b *testing.B) {
	registry := newM035ReadyRegistry(b, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		snapshot, err := registry.Snapshot("query")
		if err != nil || !snapshot.Ready {
			b.Fatalf("Snapshot() = %#v/%v", snapshot, err)
		}
	}
}

func BenchmarkSQLSnapshotReadinessWaitReady(b *testing.B) {
	registry := newM035ReadyRegistry(b, 16)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		snapshot, err := registry.Wait(ctx, "query")
		if err != nil || !snapshot.Ready {
			b.Fatalf("Wait() = %#v/%v", snapshot, err)
		}
	}
}

func newM035ReadyRegistry(b *testing.B, objectCount int) *hatSql.SQLSnapshotReadinessRegistry {
	b.Helper()
	registry, err := hatSql.NewSQLSnapshotReadinessRegistry(hatSql.SQLSnapshotReadinessRegistryOptions{
		MaxObjects:                  objectCount,
		MaxDependents:               1,
		MaxDependenciesPerDependent: objectCount,
	})
	if err != nil {
		b.Fatal(err)
	}
	dependencies := make([]string, objectCount)
	for index := 0; index < objectCount; index++ {
		object := "source-" + strconv.Itoa(index)
		dependencies[index] = object
		if err := registry.RegisterObject(object); err != nil {
			b.Fatal(err)
		}
		if err := registry.MarkReady(object, uint64(index+1)); err != nil {
			b.Fatal(err)
		}
	}
	if err := registry.RegisterDependent("query", dependencies); err != nil {
		b.Fatal(err)
	}
	return registry
}
