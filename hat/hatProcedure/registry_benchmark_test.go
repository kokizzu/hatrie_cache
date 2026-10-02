package hatProcedure

import (
	"context"
	"testing"
)

func BenchmarkRegistryCall(b *testing.B) {
	registry, err := NewRegistry(RegistryOptions{
		Authorize: func(context.Context, string, ProcedureInfo) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(Definition{
		Name:    "echo",
		Version: 1,
		Handler: func(_ context.Context, call Call) ([]byte, error) { return call.Arguments, nil },
	}); err != nil {
		b.Fatal(err)
	}
	arguments := []byte("benchmark-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := registry.Call(context.Background(), "bench", "echo", 1, arguments); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDirectHandler(b *testing.B) {
	handler := func(_ context.Context, call Call) ([]byte, error) { return call.Arguments, nil }
	call := Call{Principal: "bench", Name: "echo", Version: 1, Arguments: []byte("benchmark-payload")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := handler(context.Background(), call); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegistryList(b *testing.B) {
	registry, err := NewRegistry(RegistryOptions{
		Authorize: func(context.Context, string, ProcedureInfo) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"alpha", "beta", "gamma"} {
		if err := registry.Register(Definition{
			Name:    name,
			Version: 1,
			Handler: func(context.Context, Call) ([]byte, error) { return nil, nil },
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = registry.List()
	}
}
