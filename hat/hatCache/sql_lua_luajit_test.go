//go:build luajit

package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

func TestSQLLuaFunctionVectorizedBatch(t *testing.T) {
	definition, err := CompileSQLFunction("CREATE FUNCTION adjusted(score INTEGER, disabled BOOLEAN) LANGUAGE LUA AS 'return (not disabled) and score * 2 + 1 or 0'")
	if err != nil {
		t.Fatal(err)
	}
	registry := NewSQLFunctionRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	values, err := registry.EvaluateSQLFunction("adjusted", []SQLFunctionCall{{Arguments: []interface{}{int64(4), false}}, {Arguments: []interface{}{int64(7), true}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := values[0], int64(9); got != want {
		t.Fatalf("first result = %#v, want %#v", got, want)
	}
	if got, want := values[1], int64(0); got != want {
		t.Fatalf("second result = %#v, want %#v", got, want)
	}
}

func TestSQLLuaFunctionDoesNotExposeStandardLibraries(t *testing.T) {
	registry := NewSQLFunctionRegistry()
	definition := SQLFunctionDefinition{
		Name:          "sandboxed_globals",
		Arguments:     []string{},
		ArgumentTypes: []string{},
		Language:      "LUA",
		Source:        "return io == nil and os == nil and package == nil and debug == nil and ffi == nil and loadstring == nil",
	}
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	values, err := registry.EvaluateSQLFunction("sandboxed_globals", []SQLFunctionCall{{Arguments: nil}})
	if err != nil || len(values) != 1 || values[0] != true {
		t.Fatalf("sandbox result = %#v, %v; want true", values, err)
	}
}

func BenchmarkSQLLuaFunctionBatch(b *testing.B) {
	registry := NewSQLFunctionRegistry()
	definition := SQLFunctionDefinition{Name: "eligible", Arguments: []string{"age", "score"}, ArgumentTypes: []string{"INTEGER", "INTEGER"}, Language: "LUA", Source: "return age > 10 and score < 9"}
	if err := registry.Register(definition); err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			calls := make([]SQLFunctionCall, size)
			for index := range calls {
				calls[index] = SQLFunctionCall{Arguments: []interface{}{int64(index % 30), int64((index * 7) % 12)}}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				values, err := registry.EvaluateSQLFunction("eligible", calls)
				if err != nil {
					b.Fatal(err)
				}
				sqlFunctionBenchmarkResult = values
			}
		})
	}
}

func TestSQLLuaFunctionReportsUnsupportedValues(t *testing.T) {
	definition := SQLFunctionDefinition{Name: "bad_value", Arguments: []string{"value"}, ArgumentTypes: []string{"ANY"}, Language: "LUA", Source: "return value"}
	registry := NewSQLFunctionRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	_, err := registry.EvaluateSQLFunction("bad_value", []SQLFunctionCall{{Arguments: []interface{}{map[string]interface{}{"a": 1}}}})
	if err == nil || !strings.Contains(err.Error(), "cannot be passed to LuaJIT") {
		t.Fatalf("error = %v, want clear conversion error", err)
	}
}

func TestSQLLuaFunctionEnforcesInstructionLimit(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{LuaExecutionLimit: 1000})
	definition := SQLFunctionDefinition{
		Name:          "bounded_loop",
		Arguments:     []string{"value"},
		ArgumentTypes: []string{"INTEGER"},
		Language:      "LUA",
		Source:        "return (function() local total = 0; for i = 1, 100000 do total = total + i end; return total + value end)()",
	}
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	_, err := registry.EvaluateSQLFunction("bounded_loop", []SQLFunctionCall{{Arguments: []interface{}{int64(1)}}})
	if err == nil || !strings.Contains(err.Error(), "execution quantum exceeded") {
		t.Fatalf("error = %v, want bounded execution error", err)
	}
}

func TestSQLLuaFunctionRecoversAfterInstructionLimit(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{LuaExecutionLimit: 1000})
	definition := SQLFunctionDefinition{
		Name:          "recoverable_loop",
		Arguments:     []string{"value"},
		ArgumentTypes: []string{"INTEGER"},
		Language:      "LUA",
		Source:        "return value < 0 and value or (function() local total = 0; for i = 1, 100000 do total = total + i end; return total end)()",
	}
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	values, err := registry.EvaluateSQLFunction("recoverable_loop", []SQLFunctionCall{{Arguments: []interface{}{int64(-1)}}})
	if err != nil || len(values) != 1 || values[0] != int64(-1) {
		t.Fatalf("safe evaluation = %#v, %v; want -1", values, err)
	}
	if _, err := registry.EvaluateSQLFunction("recoverable_loop", []SQLFunctionCall{{Arguments: []interface{}{int64(1)}}}); err == nil || !strings.Contains(err.Error(), "execution quantum exceeded") {
		t.Fatalf("bounded evaluation error = %v, want execution limit", err)
	}
	values, err = registry.EvaluateSQLFunction("recoverable_loop", []SQLFunctionCall{{Arguments: []interface{}{int64(-2)}}})
	if err != nil || len(values) != 1 || values[0] != int64(-2) {
		t.Fatalf("post-error evaluation = %#v, %v; want -2", values, err)
	}
}

func TestSQLLuaFunctionRejectsOversizedBatch(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{LuaMaxBatchCalls: 1})
	definition := SQLFunctionDefinition{Name: "identity", Arguments: []string{"value"}, ArgumentTypes: []string{"INTEGER"}, Language: "LUA", Source: "return value"}
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	_, err := registry.EvaluateSQLFunction("identity", []SQLFunctionCall{{Arguments: []interface{}{int64(1)}}, {Arguments: []interface{}{int64(2)}}})
	if err == nil || !strings.Contains(err.Error(), "maximum is 1") {
		t.Fatalf("error = %v, want batch limit error", err)
	}
}

func TestSQLLuaFunctionEnforcesMemoryLimit(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{LuaMemoryLimitBytes: 128 << 10})
	definition := SQLFunctionDefinition{
		Name:          "bounded_memory",
		Arguments:     []string{"value"},
		ArgumentTypes: []string{"INTEGER"},
		Language:      "LUA",
		Source:        "return (function() local values = {}; for i = 1, 100000 do values[i] = i end; return value end)()",
	}
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	_, err := registry.EvaluateSQLFunction("bounded_memory", []SQLFunctionCall{{Arguments: []interface{}{int64(1)}}})
	if err == nil || !strings.Contains(err.Error(), "memory limit exceeded") {
		t.Fatalf("error = %v, want bounded memory error", err)
	}
}

func TestSQLLuaFunctionRejectsOversizedSource(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{LuaMaxSourceBytes: 8})
	definition := SQLFunctionDefinition{Name: "large_source", Arguments: []string{"value"}, ArgumentTypes: []string{"INTEGER"}, Language: "LUA", Source: "return value"}
	if err := registry.Register(definition); err == nil || !strings.Contains(err.Error(), "exceeds 8 bytes") {
		t.Fatalf("Register() error = %v, want source-size error", err)
	}
}

func TestSQLLuaFunctionRejectsOversizedStringInput(t *testing.T) {
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{LuaMemoryLimitBytes: 32})
	definition := SQLFunctionDefinition{Name: "large_input", Arguments: []string{"value"}, ArgumentTypes: []string{"TEXT"}, Language: "LUA", Source: "return value"}
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	_, err := registry.EvaluateSQLFunction("large_input", []SQLFunctionCall{{Arguments: []interface{}{strings.Repeat("x", 64)}}})
	if err == nil || !strings.Contains(err.Error(), "input exceeds memory limit") {
		t.Fatalf("error = %v, want input memory error", err)
	}
}
