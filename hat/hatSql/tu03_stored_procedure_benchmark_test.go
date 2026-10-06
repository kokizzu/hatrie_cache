package hatSql

import (
	"context"
	"testing"
)

func BenchmarkTU03StoredProcedureResolve(b *testing.B) {
	registry := tu03BenchmarkRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok := registry.Resolve("sum"); !ok {
			b.Fatal("procedure was not resolved")
		}
	}
}

func BenchmarkTU03StoredProcedureInvoke(b *testing.B) {
	registry := tu03BenchmarkRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := registry.Invoke(context.Background(), "bench", "sum", nil); err != nil {
			b.Fatal(err)
		}
	}
}

func tu03BenchmarkRegistry(b *testing.B) *StoredProcedureRegistry {
	b.Helper()
	registry, err := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{
		Authorize: func(StoredProcedureAuthorization) bool { return true },
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.Register(StoredProcedure{
		Name:    "sum",
		Version: "v1",
		Execute: func(context.Context, []interface{}) (interface{}, error) { return int64(1), nil },
	}, ""); err != nil {
		b.Fatal(err)
	}
	return registry
}
