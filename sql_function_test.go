package hatriecache

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var sqlFunctionBenchmarkResult []interface{}

func TestCompileSQLFunctionDefinition(t *testing.T) {
	t.Parallel()

	definition, err := CompileSQLFunction(`CREATE FUNCTION eligible(age INTEGER, score INTEGER) LANGUAGE GO AS 'return age > 10 && score < 9'`)
	if err != nil {
		t.Fatalf("CompileSQLFunction() error = %v", err)
	}
	want := SQLFunctionDefinition{Name: "eligible", Arguments: []string{"age", "score"}, ArgumentTypes: []string{"INTEGER", "INTEGER"}, Language: "GO", Source: "return age > 10 && score < 9"}
	if !reflect.DeepEqual(definition, want) {
		t.Fatalf("CompileSQLFunction() = %#v, want %#v", definition, want)
	}
}

func TestSQLGoFunctionRegistryReportsTypedArgumentErrorWithFunctionSource(t *testing.T) {
	t.Parallel()

	definition := SQLFunctionDefinition{Name: "eligible", Arguments: []string{"age", "score"}, ArgumentTypes: []string{"INTEGER", "INTEGER"}, Language: "GO", Source: "return age > 10 && score < 9"}
	registry := NewSQLFunctionRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	_, err := registry.EvaluateSQLFunction("eligible", []SQLFunctionCall{{Arguments: []interface{}{"twelve", int64(7)}}})
	if err == nil {
		t.Fatal("EvaluateSQLFunction() error = nil, want argument type error")
	}
	formatted := FormatSQLFunctionDiagnostic(definition, err)
	for _, want := range []string{"argument \"age\" expects INTEGER, got TEXT", "--> function eligible:1:", "return age > 10"} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("FormatSQLFunctionDiagnostic() = %q, want %q", formatted, want)
		}
	}
}

func TestSQLGoFunctionRegistryEvaluatesOneBatch(t *testing.T) {
	t.Parallel()

	registry := NewSQLFunctionRegistry()
	if err := registry.Register(SQLFunctionDefinition{Name: "eligible", Arguments: []string{"age", "score"}, Language: "GO", Source: "return age > 10 && score < 9"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	got, err := registry.EvaluateSQLFunction("eligible", []SQLFunctionCall{{Arguments: []interface{}{int64(12), int64(7)}}, {Arguments: []interface{}{int64(4), int64(7)}}, {Arguments: []interface{}{int64(20), int64(10)}}})
	if err != nil {
		t.Fatalf("EvaluateSQLFunction() error = %v", err)
	}
	if want := []interface{}{true, false, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("EvaluateSQLFunction() = %#v, want %#v", got, want)
	}
}

func TestSQLGoFunctionReportsUnknownArgumentWithFunctionSource(t *testing.T) {
	t.Parallel()

	definition := SQLFunctionDefinition{Name: "eligible", Arguments: []string{"age"}, Language: "GO", Source: "return agge > 10"}
	err := NewSQLFunctionRegistry().Register(definition)
	if err == nil {
		t.Fatal("Register() error = nil, want source error")
	}
	formatted := FormatSQLFunctionDiagnostic(definition, err)
	for _, want := range []string{`unknown argument "agge"`, "--> function eligible:1:8", "return agge > 10"} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("FormatSQLFunctionDiagnostic() = %q, want %q", formatted, want)
		}
	}
}

func TestExecuteSQLQueryUsesGoFunctionInWhereAndSelect(t *testing.T) {
	t.Parallel()

	registry := NewSQLFunctionRegistry()
	if err := registry.Register(SQLFunctionDefinition{Name: "eligible", Arguments: []string{"age", "score"}, Language: "GO", Source: "return age > 10 && score < 9"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	resolver := sqlFunctionTestResolver{SQLSourceResolver: SQLSourceResolverFunc(nil), functions: registry}
	result, err := ExecuteSQLQuery(`
FROM VALUES ('Ivi', 12, 7), ('Lia', 4, 7), ('No', 20, 10) AS people(name, age, score)
WHERE eligible(age, score)
SELECT name, eligible(age, score) AS is_eligible`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := SQLQueryResult{Columns: []string{"name", "is_eligible"}, Rows: []SQLRow{{"name": "Ivi", "is_eligible": true}}}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("ExecuteSQLQuery() = %#v, want %#v", result, want)
	}
}

func TestExecuteSQLQueryReportsUnknownFunctionWithoutRegistry(t *testing.T) {
	t.Parallel()

	_, err := ExecuteSQLQuery(`FROM VALUES (12) AS people(age) SELECT eligible(age)`, SQLSourceResolverFunc(nil))
	if err == nil || !strings.Contains(err.Error(), `unknown SQL function "ELIGIBLE"`) {
		t.Fatalf("ExecuteSQLQuery() error = %v, want unknown function", err)
	}
}

func TestSQLGoFunctionSupportsArithmeticGroupingAndUnaryNot(t *testing.T) {
	t.Parallel()

	registry := NewSQLFunctionRegistry()
	definition := SQLFunctionDefinition{
		Name:          "eligible",
		Arguments:     []string{"age", "penalty", "disabled"},
		ArgumentTypes: []string{"INTEGER", "INTEGER", "BOOLEAN"},
		Language:      "GO",
		Source:        "return !disabled && (age + 2) * 3 >= 30 && penalty % 2 == 0",
	}
	if err := registry.Register(definition); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	got, err := registry.EvaluateSQLFunction("eligible", []SQLFunctionCall{
		{Arguments: []interface{}{int64(8), int64(2), false}},
		{Arguments: []interface{}{int64(8), int64(3), false}},
		{Arguments: []interface{}{int64(20), int64(2), true}},
	})
	if err != nil {
		t.Fatalf("EvaluateSQLFunction() error = %v", err)
	}
	if want := []interface{}{true, false, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("EvaluateSQLFunction() = %#v, want %#v", got, want)
	}
}

func TestSQLGoFunctionReportsArithmeticTypeAndDivideByZeroErrors(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		definition SQLFunctionDefinition
		arguments  []interface{}
		want       string
	}{
		{
			name:       "type",
			definition: SQLFunctionDefinition{Name: "add", Arguments: []string{"name"}, ArgumentTypes: []string{"TEXT"}, Language: "GO", Source: "return name + 1"},
			arguments:  []interface{}{"ivi"},
			want:       `operator "+" expects numeric operands, got TEXT and INTEGER`,
		},
		{
			name:       "zero",
			definition: SQLFunctionDefinition{Name: "divide", Arguments: []string{"score"}, ArgumentTypes: []string{"INTEGER"}, Language: "GO", Source: "return score / 0"},
			arguments:  []interface{}{int64(12)},
			want:       "division by zero",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := NewSQLFunctionRegistry()
			if err := registry.Register(test.definition); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			_, err := registry.EvaluateSQLFunction(test.definition.Name, []SQLFunctionCall{{Arguments: test.arguments}})
			if err == nil {
				t.Fatal("EvaluateSQLFunction() error = nil, want runtime diagnostic")
			}
			formatted := FormatSQLFunctionDiagnostic(test.definition, err)
			if !strings.Contains(formatted, test.want) {
				t.Fatalf("FormatSQLFunctionDiagnostic() = %q, want %q", formatted, test.want)
			}
		})
	}
}

type sqlFunctionTestResolver struct {
	SQLSourceResolver
	functions SQLFunctionResolver
}

func BenchmarkSQLGoFunctionBatch(b *testing.B) {
	registry := NewSQLFunctionRegistry()
	if err := registry.Register(SQLFunctionDefinition{Name: "eligible", Arguments: []string{"age", "score"}, ArgumentTypes: []string{"INTEGER", "INTEGER"}, Language: "GO", Source: "return age > 10 && score < 9"}); err != nil {
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

func (resolver sqlFunctionTestResolver) EvaluateSQLFunction(name string, calls []SQLFunctionCall) ([]interface{}, error) {
	return resolver.functions.EvaluateSQLFunction(name, calls)
}
