package hatDataStructure

import "testing"

func BenchmarkTU21MigrationStatus(b *testing.B) {
	manager := benchmarkTU21Manager(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.Status("hot"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU21MigrationAdvance(b *testing.B) {
	manager := benchmarkTU21Manager(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := manager.Advance("hot", 1); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU21MigrationMarshal(b *testing.B) {
	manager := benchmarkTU21Manager(b)
	if _, err := manager.MarshalBinary(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU21MigrationUnmarshal(b *testing.B) {
	manager := benchmarkTU21Manager(b)
	payload, err := manager.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		restored := NewVersionedMigrationManager()
		if err := restored.UnmarshalBinary(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkTU21Manager(b *testing.B) *VersionedMigrationManager {
	b.Helper()
	manager := NewVersionedMigrationManager()
	if err := manager.RegisterPlan(MigrationPlan{Name: "hot", FromVersion: 1, ToVersion: 2, TotalUnits: ^uint64(0)}); err != nil {
		b.Fatal(err)
	}
	if err := manager.Start("hot"); err != nil {
		b.Fatal(err)
	}
	return manager
}
