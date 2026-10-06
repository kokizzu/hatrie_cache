package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTU03DirectCallback(b *testing.B) {
	evaluate := func(_ context.Context, arguments []interface{}) (interface{}, error) {
		return arguments[0].(int64) + arguments[1].(int64), nil
	}
	arguments := []interface{}{int64(1), int64(2)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, err := evaluate(context.Background(), arguments)
		if err != nil || value.(int64) != 3 {
			b.Fatalf("direct callback returned %v, %v", value, err)
		}
	}
}

func BenchmarkTU03RegistryCall(b *testing.B) {
	registry, err := hatSql.NewStoredProcedureRegistry(hatSql.StoredProcedureRegistryOptions{
		Authorize: func(context.Context, hatSql.StoredProcedureAuthorization) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(hatSql.StoredProcedureDefinition{
		Package: "bench",
		Name:    "sum",
		Version: "v1",
		Evaluate: func(_ context.Context, arguments []interface{}) (interface{}, error) {
			return arguments[0].(int64) + arguments[1].(int64), nil
		},
	}); err != nil {
		b.Fatal(err)
	}
	arguments := []interface{}{int64(1), int64(2)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, err := registry.Call(context.Background(), "bench-user", "BENCH", "SUM", "v1", arguments)
		if err != nil || value.(int64) != 3 {
			b.Fatalf("registry returned %v, %v", value, err)
		}
	}
}
