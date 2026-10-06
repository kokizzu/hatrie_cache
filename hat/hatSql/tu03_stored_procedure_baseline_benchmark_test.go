package hatSql

import "testing"

func BenchmarkTU03BaselineVersionedFunctionResolve(b *testing.B) {
	registry := NewFunctionRegistry()
	if err := registry.Register(VersionedFunction{
		Package: "bench",
		Name:    "sum",
		Version: "v1",
		Evaluate: func(values []interface{}) (interface{}, error) {
			return values[0], nil
		},
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok := registry.Resolve("bench", "sum", "v1"); !ok {
			b.Fatal("function was not resolved")
		}
	}
}

func BenchmarkTU03BaselineVersionedFunctionInvoke(b *testing.B) {
	registry := NewFunctionRegistry()
	if err := registry.Register(VersionedFunction{
		Package: "bench",
		Name:    "sum",
		Version: "v1",
		Evaluate: func(values []interface{}) (interface{}, error) {
			return int64(1), nil
		},
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		function, ok := registry.Resolve("bench", "sum", "v1")
		if !ok {
			b.Fatal("function was not resolved")
		}
		if _, err := function.Evaluate(nil); err != nil {
			b.Fatal(err)
		}
	}
}
