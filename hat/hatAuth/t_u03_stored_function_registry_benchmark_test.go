package hatAuth

import (
	"context"
	"testing"
)

func BenchmarkTU03StoredFunctionRegistryCall(b *testing.B) {
	registry, err := NewStoredFunctionRegistry(StoredFunctionRegistryOptions{
		Authorize: func(_, operation, _ string) bool {
			return operation == StoredFunctionOperationRegister || operation == StoredFunctionOperationCall
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.Register("admin", StoredFunctionSpec{Name: "echo", Version: 1, Handler: func(_ context.Context, input []byte) ([]byte, error) { return input, nil }}); err != nil {
		b.Fatal(err)
	}
	input := []byte("payload")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := registry.Call(ctx, "caller", "echo", 1, input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU03DirectOwnedFunctionCall(b *testing.B) {
	handler := func(input []byte) []byte { return append([]byte(nil), input...) }
	input := []byte("payload")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = handler(input)
	}
}
