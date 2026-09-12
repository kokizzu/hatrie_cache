package hatCache

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestSQLWASMRegistryUsesMemoryLimitAndKeepsTimeoutOptIn(t *testing.T) {
	registry := NewSQLFunctionRegistry()
	defer registry.Close()

	if registry.options.WASMExecutionTimeout != 0 {
		t.Fatalf("default WASM execution timeout = %s, want disabled fast path", registry.options.WASMExecutionTimeout)
	}
	if registry.options.WASMMemoryLimitPages == 0 {
		t.Fatal("default WASM memory limit must be positive")
	}
}

func TestSQLWASMRegistryRejectsModuleAboveMemoryLimit(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{
		WASMMemoryLimitPages: 1,
	})
	defer registry.Close()

	err := registry.Register(SQLFunctionDefinition{
		Name:          "memory_ok",
		Language:      "WASM",
		Source:        base64.StdEncoding.EncodeToString(wasmModuleWithInitialMemory(2)),
		ArgumentTypes: nil,
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "memory") {
		t.Fatalf("Register() error = %v, want memory-limit error", err)
	}
}

func TestSQLWASMFunctionExecutionTimeoutStopsInfiniteLoop(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{
		WASMExecutionTimeout: 10 * time.Millisecond,
	})
	defer registry.Close()

	if err := registry.Register(SQLFunctionDefinition{
		Name:     "spin",
		Language: "WASM",
		Source:   base64.StdEncoding.EncodeToString(wasmInfiniteLoop()),
	}); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	_, err := registry.EvaluateSQLFunction("spin", []SQLFunctionCall{{}})
	if err == nil {
		t.Fatal("EvaluateSQLFunction() error = nil, want execution timeout")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "deadline") && !strings.Contains(strings.ToLower(err.Error()), "timeout") && !strings.Contains(strings.ToLower(err.Error()), "context") {
		t.Fatalf("EvaluateSQLFunction() error = %v, want timeout diagnostic", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("infinite WASM call took %s, want it to stop promptly", elapsed)
	}
}

func TestSQLWASMFunctionStillRunsWithExplicitMemoryLimit(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{
		WASMMemoryLimitPages: 1,
	})
	defer registry.Close()

	if err := registry.Register(SQLFunctionDefinition{
		Name:          "memory_ok",
		Language:      "WASM",
		Source:        base64.StdEncoding.EncodeToString(wasmModuleWithInitialMemory(1)),
		ArgumentTypes: nil,
	}); err != nil {
		t.Fatal(err)
	}
	values, err := registry.EvaluateSQLFunction("memory_ok", []SQLFunctionCall{{}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := values[0], int64(1); got != want {
		t.Fatalf("result = %#v, want %#v", got, want)
	}
}

func BenchmarkSQLWASMFunctionBatchWithTimeout(b *testing.B) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{
		WASMExecutionTimeout: time.Hour,
	})
	defer registry.Close()
	wasm := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x60, 0x01, 0x7e, 0x01, 0x7e, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0c, 0x01, 0x08, 'p', 'l', 'u', 's', '_', 'o', 'n', 'e', 0x00, 0x00, 0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x42, 0x01, 0x7c, 0x0b}
	if err := registry.Register(SQLFunctionDefinition{Name: "plus_one", Arguments: []string{"value"}, ArgumentTypes: []string{"INTEGER"}, Language: "WASM", Source: base64.StdEncoding.EncodeToString(wasm)}); err != nil {
		b.Fatal(err)
	}
	calls := make([]SQLFunctionCall, 10_000)
	for index := range calls {
		calls[index] = SQLFunctionCall{Arguments: []interface{}{int64(index)}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		values, err := registry.EvaluateSQLFunction("plus_one", calls)
		if err != nil {
			b.Fatal(err)
		}
		sqlFunctionBenchmarkResult = values
	}
}

func wasmModuleWithInitialMemory(pages byte) []byte {
	return []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
		0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7e,
		0x03, 0x02, 0x01, 0x00,
		0x05, 0x03, 0x01, 0x00, pages,
		0x07, 0x0d, 0x01, 0x09, 'm', 'e', 'm', 'o', 'r', 'y', '_', 'o', 'k', 0x00, 0x00,
		0x0a, 0x06, 0x01, 0x04, 0x00, 0x42, 0x01, 0x0b,
	}
}

func wasmInfiniteLoop() []byte {
	return []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
		0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7e,
		0x03, 0x02, 0x01, 0x00,
		0x07, 0x08, 0x01, 0x04, 's', 'p', 'i', 'n', 0x00, 0x00,
		0x0a, 0x0b, 0x01, 0x09, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x42, 0x00, 0x0b,
	}
}
