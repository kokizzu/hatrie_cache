package hatRuntime

import (
	"context"
	"errors"
	"testing"
)

func TestTU04CompilesAndExecutesBoundedArithmetic(t *testing.T) {
	program, err := Compile(1, []Instruction{
		{Op: OpLoadArg, Operand: 0},
		{Op: OpPushInt, Operand: 2},
		{Op: OpAddInt},
		{Op: OpReturn},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Execute(context.Background(), program, []Value{Int64(40)})
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := result.Int64(); !ok || value != 42 {
		t.Fatalf("result = %#v, want integer 42", result)
	}
}

func TestTU04StopsUntrustedInfiniteLoops(t *testing.T) {
	program, err := CompileWithLimits(Limits{MaxSteps: 10}, 0, []Instruction{
		{Op: OpPushBool, Operand: 1},
		{Op: OpJumpIfFalse, Operand: 4},
		{Op: OpJump, Operand: 0},
		{Op: OpPushNull},
		{Op: OpReturn},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = Execute(context.Background(), program, nil)
	if !errors.Is(err, ErrStepLimit) {
		t.Fatalf("error = %v, want ErrStepLimit", err)
	}
}

func TestTU04RejectsInvalidPrograms(t *testing.T) {
	tests := []struct {
		name         string
		arguments    int
		instructions []Instruction
	}{
		{name: "bad opcode", instructions: []Instruction{{Op: OpCode(255)}}},
		{name: "bad argument", arguments: 1, instructions: []Instruction{{Op: OpLoadArg, Operand: 1}}},
		{name: "bad jump", instructions: []Instruction{{Op: OpJump, Operand: 99}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Compile(test.arguments, test.instructions); !errors.Is(err, ErrProgramInvalid) {
				t.Fatalf("error = %v, want ErrProgramInvalid", err)
			}
		})
	}
}

func TestTU04StopsStackGrowth(t *testing.T) {
	program, err := CompileWithLimits(Limits{MaxStack: 1}, 0, []Instruction{
		{Op: OpPushInt, Operand: 1},
		{Op: OpPushInt, Operand: 2},
		{Op: OpReturn},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(context.Background(), program, nil); !errors.Is(err, ErrStackLimit) {
		t.Fatalf("error = %v, want ErrStackLimit", err)
	}
}

func TestTU04HonorsCancellationAndStringLimits(t *testing.T) {
	program, err := CompileWithLimits(Limits{MaxStringBytes: 3}, 0, []Instruction{
		{Op: OpPushString, Text: "ab"},
		{Op: OpPushString, Text: "cd"},
		{Op: OpConcat},
		{Op: OpReturn},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Execute(context.Background(), program, nil); !errors.Is(err, ErrValueLimit) {
		t.Fatalf("error = %v, want ErrValueLimit", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Execute(canceled, program, nil); !errors.Is(err, ErrExecutionCanceled) {
		t.Fatalf("canceled error = %v, want ErrExecutionCanceled", err)
	}
}

func TestTU04BatchExecutionIsolatedPerRow(t *testing.T) {
	program, err := Compile(1, []Instruction{
		{Op: OpLoadArg, Operand: 0},
		{Op: OpPushString, Text: "!"},
		{Op: OpConcat},
		{Op: OpReturn},
	})
	if err != nil {
		t.Fatal(err)
	}
	results, err := ExecuteBatch(context.Background(), program, [][]Value{{Text("a")}, {Text("b")}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("result count = %d, want 2", len(results))
	}
	for index, want := range []string{"a!", "b!"} {
		got, ok := results[index].Text()
		if !ok || got != want {
			t.Fatalf("result %d = %#v, want %q", index, results[index], want)
		}
	}
}
