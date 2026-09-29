package hatSql

import "testing"

func BenchmarkM220IndexReaderLifecycle(b *testing.B) {
	index := &struct{ value uint64 }{value: 1}
	registry := NewSQLIndexRetirementRegistry()
	if err := registry.Register("accounts_by_region", index); err != nil {
		b.Fatal(err)
	}

	b.Run("direct_pointer", func(b *testing.B) {
		var sink *struct{ value uint64 }
		for iteration := 0; iteration < b.N; iteration++ {
			sink = index
		}
		if sink.value != 1 {
			b.Fatal("direct pointer was not observed")
		}
	})

	b.Run("acquire_release", func(b *testing.B) {
		var sink *struct{ value uint64 }
		for iteration := 0; iteration < b.N; iteration++ {
			lease, err := registry.Acquire("accounts_by_region")
			if err != nil {
				b.Fatal(err)
			}
			sink = lease.Index().(*struct{ value uint64 })
			if _, released := lease.Release(); !released {
				b.Fatal("lease was not released")
			}
		}
		if sink.value != 1 {
			b.Fatal("leased pointer was not observed")
		}
	})

	b.Run("held_lease", func(b *testing.B) {
		lease, err := registry.Acquire("accounts_by_region")
		if err != nil {
			b.Fatal(err)
		}
		defer lease.Release()
		var sink *struct{ value uint64 }
		for iteration := 0; iteration < b.N; iteration++ {
			sink = lease.Index().(*struct{ value uint64 })
		}
		if sink.value != 1 {
			b.Fatal("held pointer was not observed")
		}
	})
}

func BenchmarkM220RetireIdleIndex(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		registry := NewSQLIndexRetirementRegistry()
		if err := registry.Register("accounts_by_region", &struct{}{}); err != nil {
			b.Fatal(err)
		}
		result, err := registry.Retire("accounts_by_region")
		if err != nil || !result.Removed {
			b.Fatalf("Retire() = %#v, %v", result, err)
		}
	}
}
