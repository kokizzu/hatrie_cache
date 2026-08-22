//go:build javy

package hatriecache

import (
	"flag"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

var sqlTestJavyPath = flag.String("sql-js-compiler", "", "path to the Javy compiler for LANGUAGE JS integration tests")

func newJavyTestRegistry(t *testing.T, timeout time.Duration) *SQLFunctionRegistry {
	t.Helper()
	if *sqlTestJavyPath == "" {
		t.Skip("pass -sql-js-compiler=/path/to/javy to run LANGUAGE JS integration tests")
	}
	return NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{
		JavyPath:           *sqlTestJavyPath,
		JSCompileTimeout:   10 * time.Second,
		JSExecutionTimeout: timeout,
	})
}

func TestSQLJavaScriptFunctionVectorizedBatch(t *testing.T) {
	registry := newJavyTestRegistry(t, time.Second)
	definition := SQLFunctionDefinition{
		Name:          "label_score",
		Arguments:     []string{"name", "score", "disabled"},
		ArgumentTypes: []string{"TEXT", "NUMBER", "BOOLEAN"},
		Language:      "JS",
		Source: `if (disabled) {
  return null;
}
return name + ':' + (score * 2);`,
	}
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	calls := []SQLFunctionCall{
		{Arguments: []interface{}{"Ivi", float64(4.5), false}},
		{Arguments: []interface{}{"Lia", float64(9), true}},
	}
	for attempt := 0; attempt < 2; attempt++ {
		got, err := registry.EvaluateSQLFunction("label_score", calls)
		if err != nil {
			t.Fatal(err)
		}
		if want := []interface{}{"Ivi:9", nil}; !reflect.DeepEqual(got, want) {
			t.Fatalf("attempt %d result = %#v, want %#v", attempt+1, got, want)
		}
	}
}

func TestSQLJavaScriptFunctionTimeoutIsReported(t *testing.T) {
	registry := newJavyTestRegistry(t, 50*time.Millisecond)
	definition := SQLFunctionDefinition{Name: "forever", Language: "JS", Source: "while (true) {}\nreturn 0;"}
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	_, err := registry.EvaluateSQLFunction("forever", []SQLFunctionCall{{}})
	if err == nil || !strings.Contains(err.Error(), "execution exceeded 50ms batch limit") {
		t.Fatalf("EvaluateSQLFunction() error = %v, want timeout diagnostic", err)
	}
}

func TestSQLJavaScriptFunctionCompileDiagnosticUsesSourceLocation(t *testing.T) {
	registry := newJavyTestRegistry(t, time.Second)
	definition := SQLFunctionDefinition{Name: "broken", Arguments: []string{"score"}, ArgumentTypes: []string{"NUMBER"}, Language: "JS", Source: "const noop = 1;\nreturn score + ;"}
	err := registry.Register(definition)
	if err == nil {
		t.Fatal("Register() error = nil, want JavaScript compiler error")
	}
	formatted := FormatSQLFunctionDiagnostic(definition, err)
	for _, want := range []string{"JavaScript compilation failed", "--> function broken:2:"} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("FormatSQLFunctionDiagnostic() = %q, want %q", formatted, want)
		}
	}
}

func BenchmarkSQLJavaScriptFunctionBatch(b *testing.B) {
	if *sqlTestJavyPath == "" {
		b.Skip("pass -sql-js-compiler=/path/to/javy")
	}
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{JavyPath: *sqlTestJavyPath, JSCompileTimeout: 10 * time.Second, JSExecutionTimeout: 5 * time.Second})
	if err := registry.Register(SQLFunctionDefinition{Name: "plus_one", Arguments: []string{"value"}, ArgumentTypes: []string{"INTEGER"}, Language: "JS", Source: "return value + 1;"}); err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run("rows-"+strconv.Itoa(size), func(b *testing.B) {
			calls := make([]SQLFunctionCall, size)
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
		})
	}
}
