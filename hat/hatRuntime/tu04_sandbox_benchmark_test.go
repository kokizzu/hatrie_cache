package hatRuntime

import (
	"context"
	"testing"
)

var (
	benchmarkTU04IntSink   int64
	benchmarkTU04ValueSink Value
	benchmarkTU04BatchSink []Value
)

func BenchmarkTU04DirectInt(b *testing.B) {
	var sum int64
	for i := 0; i < b.N; i++ {
		sum += int64(i) + 2
	}
	benchmarkTU04IntSink = sum
}

func BenchmarkTU04ExecuteInt(b *testing.B) {
	program, err := Compile(1, []Instruction{
		{Op: OpLoadArg, Operand: 0},
		{Op: OpPushInt, Operand: 2},
		{Op: OpAddInt},
		{Op: OpReturn},
	})
	if err != nil {
		b.Fatal(err)
	}
	args := []Value{Int64(40)}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, executeErr := Execute(ctx, program, args)
		if executeErr != nil {
			b.Fatal(executeErr)
		}
		benchmarkTU04ValueSink = value
	}
}

func BenchmarkTU04DirectConcat(b *testing.B) {
	left := "row"
	right := "!"

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkTU04ValueSink = Text(left + right)
	}
}

func BenchmarkTU04ExecuteConcat(b *testing.B) {
	program, err := Compile(1, []Instruction{
		{Op: OpLoadArg, Operand: 0},
		{Op: OpPushString, Text: "!"},
		{Op: OpConcat},
		{Op: OpReturn},
	})
	if err != nil {
		b.Fatal(err)
	}
	args := []Value{Text("row")}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, executeErr := Execute(ctx, program, args)
		if executeErr != nil {
			b.Fatal(executeErr)
		}
		benchmarkTU04ValueSink = value
	}
}

func BenchmarkTU04DirectBatchConcat(b *testing.B) {
	rows := make([]string, 256)
	for i := range rows {
		rows[i] = "row"
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		values := make([]Value, len(rows))
		for rowIndex, row := range rows {
			values[rowIndex] = Text(row + "!")
		}
		benchmarkTU04BatchSink = values
	}
}

func BenchmarkTU04ExecuteBatchConcat(b *testing.B) {
	program, err := Compile(1, []Instruction{
		{Op: OpLoadArg, Operand: 0},
		{Op: OpPushString, Text: "!"},
		{Op: OpConcat},
		{Op: OpReturn},
	})
	if err != nil {
		b.Fatal(err)
	}
	rows := make([][]Value, 256)
	for i := range rows {
		rows[i] = []Value{Text("row")}
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, executeErr := ExecuteBatch(ctx, program, rows)
		if executeErr != nil {
			b.Fatal(executeErr)
		}
		benchmarkTU04BatchSink = values
	}
}
