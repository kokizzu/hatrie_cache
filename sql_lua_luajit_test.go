//go:build luajit

package hatriecache

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
