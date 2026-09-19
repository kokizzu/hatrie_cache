//go:build mu44

package hatSql

import "testing"

func BenchmarkSQLDataflowVisibilityPublish4(b *testing.B) {
	coordinator, err := NewSQLDataflowVisibilityCoordinator(SQLDataflowVisibilityOptions{})
	if err != nil {
		b.Fatal(err)
	}
	names := []string{"orders", "customers", "inventory", "payments"}
	for _, name := range names {
		if err := coordinator.Register(name); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := coordinator.Publish(names); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLDataflowVisibilityAcquire4(b *testing.B) {
	coordinator, names := newSQLDataflowVisibilityBenchmarkCoordinator(b)
	if _, err := coordinator.Publish(names); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := coordinator.Acquire(names); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLDataflowVisibilityCheck4(b *testing.B) {
	coordinator, names := newSQLDataflowVisibilityBenchmarkCoordinator(b)
	token, err := coordinator.Publish(names)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := coordinator.Check(token); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLDataflowVisibilitySnapshot4(b *testing.B) {
	coordinator, names := newSQLDataflowVisibilityBenchmarkCoordinator(b)
	if _, err := coordinator.Publish(names); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = coordinator.Snapshot()
	}
}

func newSQLDataflowVisibilityBenchmarkCoordinator(b *testing.B) (*SQLDataflowVisibilityCoordinator, []string) {
	b.Helper()
	coordinator, err := NewSQLDataflowVisibilityCoordinator(SQLDataflowVisibilityOptions{})
	if err != nil {
		b.Fatal(err)
	}
	names := []string{"orders", "customers", "inventory", "payments"}
	for _, name := range names {
		if err := coordinator.Register(name); err != nil {
			b.Fatal(err)
		}
	}
	return coordinator, names
}
