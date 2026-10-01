package hatProcedure

import (
	"context"
	"testing"
)

var procedureBenchmarkSink []byte

func BenchmarkDirectProcedureCall(b *testing.B) {
	payload := []byte("payload")
	handler := func(_ context.Context, input []byte) ([]byte, error) {
		return input, nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := handler(context.Background(), payload)
		if err != nil {
			b.Fatal(err)
		}
		procedureBenchmarkSink = result
	}
}

func BenchmarkRegistryInvoke(b *testing.B) {
	registry, err := NewRegistry(Options{Authorize: AllowAll})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(Definition{
		Name:    "echo",
		Version: 1,
		Handler: func(_ context.Context, input []byte) ([]byte, error) { return input, nil },
	}); err != nil {
		b.Fatal(err)
	}
	call := Call{Name: "echo", Payload: []byte("payload")}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := registry.Invoke(context.Background(), call)
		if err != nil {
			b.Fatal(err)
		}
		procedureBenchmarkSink = result
	}
}
