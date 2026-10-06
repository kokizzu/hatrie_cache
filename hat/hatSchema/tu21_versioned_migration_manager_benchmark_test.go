package hatSchema

import (
	"context"
	"testing"
)

func benchmarkVersionedMigrationPlan(b *testing.B) VersionedMigrationPlan {
	b.Helper()
	plan, err := NewVersionedMigrationPlan("users-v2", versionedMigrationBaseSchema(), []VersionedMigrationStep{
		{Migration: versionedMigrationAddEmail()},
		{Migration: versionedMigrationAddActive()},
	})
	if err != nil {
		b.Fatal(err)
	}
	return plan
}

func BenchmarkTU21VersionedMigrationApplyNext(b *testing.B) {
	plan := benchmarkVersionedMigrationPlan(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		manager, err := NewVersionedMigrationManager(plan)
		if err != nil {
			b.Fatal(err)
		}
		if err := manager.ApplyNext(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU21VersionedMigrationSnapshotMarshal(b *testing.B) {
	manager, err := NewVersionedMigrationManager(benchmarkVersionedMigrationPlan(b))
	if err != nil {
		b.Fatal(err)
	}
	checkpoint := manager.Snapshot()
	encoded, err := MarshalVersionedMigrationCheckpoint(checkpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := MarshalVersionedMigrationCheckpoint(checkpoint); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU21VersionedMigrationSnapshotUnmarshal(b *testing.B) {
	manager, err := NewVersionedMigrationManager(benchmarkVersionedMigrationPlan(b))
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := MarshalVersionedMigrationCheckpoint(manager.Snapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := UnmarshalVersionedMigrationCheckpoint(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU21VersionedMigrationClientAdmission(b *testing.B) {
	plan := benchmarkVersionedMigrationPlan(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		manager, err := NewVersionedMigrationManager(plan)
		if err != nil {
			b.Fatal(err)
		}
		if err := manager.AdmitClient("reader", 0); err != nil {
			b.Fatal(err)
		}
	}
}
