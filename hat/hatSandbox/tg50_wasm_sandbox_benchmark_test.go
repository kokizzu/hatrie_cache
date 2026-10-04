package hatSandbox

import (
	"context"
	"testing"
)

var tg50BenchmarkResult []uint64

func BenchmarkTG50DirectScalarBaseline(b *testing.B) {
	input := uint64(41)
	var result uint64
	b.ReportAllocs()
	for b.Loop() {
		result = input + 1
	}
	if result != input+1 {
		b.Fatalf("unexpected result: %d", result)
	}
}

func BenchmarkTG50SandboxCall(b *testing.B) {
	sandbox, err := New(testContext(), tg50PlusOneModule, DefaultSandboxOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer sandbox.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := sandbox.Call(testContext(), "plus_one", 41)
		if err != nil {
			b.Fatal(err)
		}
		tg50BenchmarkResult = result
	}
}

func BenchmarkTG50SandboxCreateClose(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sandbox, err := New(testContext(), tg50PlusOneModule, DefaultSandboxOptions())
		if err != nil {
			b.Fatal(err)
		}
		if err := sandbox.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func testContext() context.Context {
	return context.Background()
}
