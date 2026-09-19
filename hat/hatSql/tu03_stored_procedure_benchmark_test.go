package hatSql

import (
	"context"
	"testing"
)

var storedProcedureBenchmarkValue interface{}

func benchmarkStoredProcedureDefinition() StoredProcedureDefinition {
	return StoredProcedureDefinition{
		Name:    "double",
		Version: "v1",
		Execute: func(_ context.Context, request StoredProcedureRequest) (StoredProcedureResponse, error) {
			return StoredProcedureResponse{Values: []interface{}{request.Arguments[0].(int64) * 2}}, nil
		},
	}
}

func BenchmarkStoredProcedureDirect(b *testing.B) {
	execute := benchmarkStoredProcedureDefinition().Execute
	request := StoredProcedureRequest{Name: "double", Version: "v1", Arguments: []interface{}{int64(21)}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response, err := execute(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		storedProcedureBenchmarkValue = response.Values[0]
	}
}

func BenchmarkStoredProcedureRegistryCall(b *testing.B) {
	registry := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{})
	if err := registry.Register(benchmarkStoredProcedureDefinition()); err != nil {
		b.Fatal(err)
	}
	arguments := []interface{}{int64(21)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values, err := registry.Call(context.Background(), "double", "v1", arguments)
		if err != nil {
			b.Fatal(err)
		}
		storedProcedureBenchmarkValue = values[0]
	}
}

func BenchmarkStoredProcedureRegistryAuthorizedCall(b *testing.B) {
	registry := NewStoredProcedureRegistry(StoredProcedureRegistryOptions{
		Authorize: func(context.Context, StoredProcedureRequest) error { return nil },
	})
	if err := registry.Register(benchmarkStoredProcedureDefinition()); err != nil {
		b.Fatal(err)
	}
	arguments := []interface{}{int64(21)}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values, err := registry.Call(context.Background(), "double", "v1", arguments)
		if err != nil {
			b.Fatal(err)
		}
		storedProcedureBenchmarkValue = values[0]
	}
}
