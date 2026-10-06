package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatRuntime"
	"hatrie_cache/hat/hatSql"
)

func BenchmarkTU04SQLRuntimeResolver(b *testing.B) {
	program, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
		{Op: hatRuntime.OpLoadArg, Operand: 0},
		{Op: hatRuntime.OpPushInt, Operand: 2},
		{Op: hatRuntime.OpAddInt},
		{Op: hatRuntime.OpReturn},
	})
	if err != nil {
		b.Fatal(err)
	}
	resolver, err := hatSql.NewRuntimeFunctionResolver(hatSql.RuntimeFunction{
		FunctionDefinition: hatSql.FunctionDefinition{Name: "ADD_TWO"},
		Program:            program,
	})
	if err != nil {
		b.Fatal(err)
	}
	calls := []hatSql.FunctionCall{{Arguments: []interface{}{int64(40)}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, evaluateErr := resolver.EvaluateSQLFunction("ADD_TWO", calls)
		if evaluateErr != nil {
			b.Fatal(evaluateErr)
		}
		benchmarkTU04SQLRuntimeValueSink = values[0]
	}
}

func BenchmarkTU04SQLRuntimeBaseline(b *testing.B) {
	program, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
		{Op: hatRuntime.OpLoadArg, Operand: 0},
		{Op: hatRuntime.OpPushInt, Operand: 2},
		{Op: hatRuntime.OpAddInt},
		{Op: hatRuntime.OpReturn},
	})
	if err != nil {
		b.Fatal(err)
	}
	args := []hatRuntime.Value{hatRuntime.Int64(40)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, executeErr := hatRuntime.Execute(context.Background(), program, args)
		if executeErr != nil {
			b.Fatal(executeErr)
		}
		benchmarkTU04SQLRuntimeValueSink = value
	}
}

func BenchmarkTU04SQLRuntimeResolverBatch(b *testing.B) {
	program, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
		{Op: hatRuntime.OpLoadArg, Operand: 0},
		{Op: hatRuntime.OpPushInt, Operand: 2},
		{Op: hatRuntime.OpAddInt},
		{Op: hatRuntime.OpReturn},
	})
	if err != nil {
		b.Fatal(err)
	}
	resolver, err := hatSql.NewRuntimeFunctionResolver(hatSql.RuntimeFunction{
		FunctionDefinition: hatSql.FunctionDefinition{Name: "ADD_TWO"},
		Program:            program,
	})
	if err != nil {
		b.Fatal(err)
	}
	calls := make([]hatSql.FunctionCall, 256)
	for index := range calls {
		calls[index] = hatSql.FunctionCall{Arguments: []interface{}{int64(index)}}
	}
	b.ReportMetric(float64(len(calls)), "calls/op")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, evaluateErr := resolver.EvaluateSQLFunction("ADD_TWO", calls)
		if evaluateErr != nil {
			b.Fatal(evaluateErr)
		}
		benchmarkTU04SQLRuntimeValueSink = values
	}
}

func BenchmarkTU04SQLRuntimeBaselineBatch(b *testing.B) {
	program, err := hatRuntime.Compile(1, []hatRuntime.Instruction{
		{Op: hatRuntime.OpLoadArg, Operand: 0},
		{Op: hatRuntime.OpPushInt, Operand: 2},
		{Op: hatRuntime.OpAddInt},
		{Op: hatRuntime.OpReturn},
	})
	if err != nil {
		b.Fatal(err)
	}
	arguments := make([][]hatRuntime.Value, 256)
	for index := range arguments {
		arguments[index] = []hatRuntime.Value{hatRuntime.Int64(int64(index))}
	}
	b.ReportMetric(float64(len(arguments)), "calls/op")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, executeErr := hatRuntime.ExecuteBatch(context.Background(), program, arguments)
		if executeErr != nil {
			b.Fatal(executeErr)
		}
		benchmarkTU04SQLRuntimeValueSink = values
	}
}

var benchmarkTU04SQLRuntimeValueSink interface{}
