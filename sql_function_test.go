package hatriecache

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
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

func TestCompileSQLFunctionAcceptsJavaScriptAndReportsMissingCompiler(t *testing.T) {
	t.Parallel()
	definition, err := CompileSQLFunction(`CREATE FUNCTION double_it(value INTEGER) LANGUAGE JS AS 'return value * 2;'`)
	if err != nil {
		t.Fatalf("CompileSQLFunction() error = %v", err)
	}
	if definition.Language != "JS" {
		t.Fatalf("language = %q, want JS", definition.Language)
	}
	registry := NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{JavyPath: "/does/not/exist"})
	err = registry.Register(definition)
	if err == nil || !strings.Contains(err.Error(), "not an executable Javy binary") {
		t.Fatalf("Register() error = %v, want clear Javy compiler prerequisite", err)
	}
}

func TestSQLJavaScriptValidationRejectsMissingReturnAndUnsafeInteger(t *testing.T) {
	t.Parallel()
	definition := SQLFunctionDefinition{Name: "no_return", Language: "JS", Source: "const value = 1;"}
	err := NewSQLFunctionRegistry().Register(definition)
	if err == nil || !strings.Contains(err.Error(), "must contain a return statement") {
		t.Fatalf("Register() error = %v, want return diagnostic", err)
	}
	if _, err := sqlJSInteger(9007199254740992); err == nil || !strings.Contains(err.Error(), "exact number range") {
		t.Fatalf("sqlJSInteger() error = %v, want precision diagnostic", err)
	}
	reserved := SQLFunctionDefinition{Name: "reserved", Arguments: []string{"__hatrie_row"}, Language: "JS", Source: "return __hatrie_row;"}
	err = NewSQLFunctionRegistry().Register(reserved)
	if err == nil || !strings.Contains(err.Error(), "reserved __hatrie_ prefix") {
		t.Fatalf("Register() error = %v, want reserved-name diagnostic", err)
	}
}

func TestSQLJavaScriptValueConversionPreservesJSONShapes(t *testing.T) {
	t.Parallel()
	definition := SQLFunctionDefinition{Name: "shape", Arguments: []string{"value"}, ArgumentTypes: []string{"ANY"}, Language: "JS", Source: "return value;"}
	input, err := sqlJSBatchInput(definition, []SQLFunctionCall{{Arguments: []interface{}{map[string]interface{}{"name": "Ivi", "tags": []interface{}{"bass", float64(2)}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(input), `[[{"name":"Ivi","tags":["bass",2]}]]`; got != want {
		t.Fatalf("sqlJSBatchInput() = %s, want %s", got, want)
	}
	values, err := sqlJSBatchOutput([]byte(`[{"name":"Ivi","tags":["bass",2]}]`), 1)
	if err != nil {
		t.Fatal(err)
	}
	want := []interface{}{map[string]interface{}{"name": "Ivi", "tags": []interface{}{"bass", json.Number("2")}}}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("sqlJSBatchOutput() = %#v, want %#v", values, want)
	}
}

// TestSQLAcceptedKeywordInventory keeps every accepted SQL grammar word tied
// to an executable positive case. The subtest name is intentionally the exact
// word plus its context, so a parser expansion cannot be documented without
// adding a corresponding test here and to SQL_TEST_MATRIX.md.
func TestSQLAcceptedKeywordInventory(t *testing.T) {
	t.Parallel()
	resolver := SQLSourceResolverFunc(func(name string, key string) ([]SQLRow, error) {
		return []SQLRow{{"id": int64(1), "name": "Ivi", "key": "entry"}}, nil
	})
	type keywordCase struct {
		name   string
		mode   string
		source string
	}
	cases := []keywordCase{
		{"ALL", "query", "FROM VALUES (1) AS a(value) SELECT value UNION ALL FROM VALUES (2) AS b(value) SELECT value"},
		{"ANALYZE", "query", "EXPLAIN ANALYZE FROM VALUES (1) AS a(value) SELECT value"},
		{"AND", "query", "FROM VALUES (TRUE, TRUE) AS a(left_value, right_value) WHERE left_value AND right_value SELECT left_value"},
		{"AS", "query", "FROM VALUES (1) AS a(value) SELECT value AS result"},
		{"ASC", "query", "FROM VALUES (2), (1) AS a(value) SELECT value ORDER BY value ASC"},
		{"BY", "query", "FROM VALUES (1), (1) AS a(value) GROUP BY value SELECT value"},
		{"BREADTH", "query", "WITH RECURSIVE walk(value) AS (FROM VALUES (1) AS seed(value) SELECT value UNION ALL FROM walk AS previous WHERE value < 1 SELECT value) SEARCH BREADTH FIRST BY value SET search_order FROM walk SELECT search_order"},
		{"CACHE", "query", "FROM CACHE('people') AS p SELECT p.name"},
		{"CAST", "query", "FROM VALUES ('42') AS a(value) SELECT CAST(value AS NUMBER)"},
		{"CROSS", "query", "FROM VALUES (1) AS a(value) CROSS JOIN VALUES (2) AS b(value) SELECT a.value"},
		{"CYCLE", "query", "WITH RECURSIVE walk(value) AS (FROM VALUES (1) AS seed(value) SELECT value UNION ALL FROM walk AS previous WHERE value < 1 SELECT value) CYCLE value SET is_cycle FROM walk SELECT is_cycle"},
		{"DESC", "query", "FROM VALUES (1), (2) AS a(value) SELECT value ORDER BY value DESC"},
		{"DATE", "query", "FROM VALUES (DATE '2026-08-22') AS a(value) SELECT value"},
		{"DECIMAL", "query", "FROM VALUES (DECIMAL '12.50') AS a(value) SELECT value"},
		{"DISTINCT", "query", "FROM VALUES (1), (1) AS a(value) SELECT DISTINCT value"},
		{"EXCEPT", "query", "FROM VALUES (1), (2) AS a(value) SELECT value EXCEPT FROM VALUES (2) AS b(value) SELECT value"},
		{"EXPLAIN", "query", "EXPLAIN FROM VALUES (1) AS a(value) SELECT value"},
		{"FETCH", "query", "FROM VALUES (1), (2) AS a(value) SELECT value FETCH FIRST 1 ROWS ONLY"},
		{"FROM", "query", "FROM VALUES (1) AS a(value) SELECT value"},
		{"FULL", "query", "FROM VALUES (1) AS a(value) FULL JOIN VALUES (2) AS b(value) ON a.value = b.value SELECT a.value"},
		{"GROUP", "query", "FROM VALUES (1), (1) AS a(value) GROUP BY value SELECT value"},
		{"HAVING", "query", "FROM VALUES (1) AS a(value) GROUP BY value HAVING COUNT(*) = 1 SELECT value"},
		{"INNER", "query", "FROM VALUES (1) AS a(value) INNER JOIN VALUES (1) AS b(value) ON a.value = b.value SELECT a.value"},
		{"IN", "query", "FROM VALUES (1) AS a(value) WHERE value IN (1, 2) SELECT value"},
		{"JSON_FIELD_TYPES", "query", "FROM CACHE('people') AS p(id INTEGER, name TEXT, score NUMBER, price DECIMAL, active BOOLEAN, day DATE, occurred_at TIMESTAMP, extra JSON) SELECT p.id"},
		{"INTERSECT", "query", "FROM VALUES (1), (2) AS a(value) SELECT value INTERSECT FROM VALUES (2) AS b(value) SELECT value"},
		{"IS", "query", "FROM VALUES (NULL) AS a(value) WHERE value IS NULL SELECT value"},
		{"JOIN", "query", "FROM VALUES (1) AS a(value) JOIN VALUES (1) AS b(value) ON a.value = b.value SELECT a.value"},
		{"KEYS", "query", "FROM KEYS SELECT key"},
		{"LEFT", "query", "FROM VALUES (1) AS a(value) LEFT JOIN VALUES (2) AS b(value) ON a.value = b.value SELECT a.value"},
		{"LIKE", "query", "FROM VALUES ('Ivi') AS a(name) WHERE name LIKE 'I%' SELECT name"},
		{"LIMIT", "query", "FROM VALUES (1), (2) AS a(value) SELECT value LIMIT 1"},
		{"NOT", "query", "FROM VALUES (FALSE) AS a(value) WHERE NOT value SELECT value"},
		{"NULL", "query", "FROM VALUES (NULL) AS a(value) SELECT value"},
		{"NULLS", "query", "FROM VALUES (NULL), (1) AS a(value) SELECT value ORDER BY value NULLS LAST"},
		{"OFFSET", "query", "FROM VALUES (1), (2) AS a(value) SELECT value OFFSET 1"},
		{"ONLY", "query", "FROM VALUES (1), (2) AS a(value) SELECT value FETCH FIRST 1 ROWS ONLY"},
		{"ON", "query", "FROM VALUES (1) AS a(value) JOIN VALUES (1) AS b(value) ON a.value = b.value SELECT a.value"},
		{"OR", "query", "FROM VALUES (FALSE, TRUE) AS a(left_value, right_value) WHERE left_value OR right_value SELECT right_value"},
		{"ORDER", "query", "FROM VALUES (1) AS a(value) SELECT value ORDER BY value"},
		{"OVER", "query", "FROM VALUES (1) AS a(value) SELECT ROW_NUMBER() OVER (ORDER BY value) AS row_number"},
		{"OUTER", "query", "FROM VALUES (1) AS a(value) FULL OUTER JOIN VALUES (2) AS b(value) ON a.value = b.value SELECT a.value"},
		{"PARTITION", "query", "FROM VALUES ('a', 1) AS a(team, value) SELECT ROW_NUMBER() OVER (PARTITION BY team ORDER BY value) AS row_number"},
		{"RIGHT", "query", "FROM VALUES (1) AS a(value) RIGHT JOIN VALUES (1) AS b(value) ON a.value = b.value SELECT b.value"},
		{"RECURSIVE", "query", "WITH RECURSIVE sequence(value) AS (FROM VALUES (1) AS seed(value) SELECT value UNION ALL FROM sequence AS previous WHERE value < 2 SELECT value + 1 AS value) FROM sequence SELECT value"},
		{"BETWEEN", "query", "FROM VALUES (1) AS a(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
		{"CURRENT", "query", "FROM VALUES (1) AS a(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
		{"FOLLOWING", "query", "FROM VALUES (1) AS a(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)"},
		{"FIRST", "query", "WITH RECURSIVE walk(value) AS (FROM VALUES (1) AS seed(value) SELECT value UNION ALL FROM walk AS previous WHERE value < 1 SELECT value) SEARCH BREADTH FIRST BY value SET search_order FROM walk SELECT search_order"},
		{"LAST", "query", "FROM VALUES (NULL), (1) AS a(value) SELECT value ORDER BY value NULLS LAST"},
		{"PRECEDING", "query", "FROM VALUES (1) AS a(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"},
		{"ROW", "query", "FROM VALUES (1) AS a(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
		{"ROWS", "query", "FROM VALUES (1) AS a(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
		{"ROWS_WINDOW_FRAME", "query", "FROM VALUES (1) AS a(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
		{"SEARCH", "query", "WITH RECURSIVE walk(value) AS (FROM VALUES (1) AS seed(value) SELECT value UNION ALL FROM walk AS previous WHERE value < 1 SELECT value) SEARCH BREADTH FIRST BY value SET search_order FROM walk SELECT search_order"},
		{"UNBOUNDED", "query", "FROM VALUES (1) AS a(value) SELECT SUM(value) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"},
		{"SELECT", "query", "FROM VALUES (1) AS a(value) SELECT value"},
		{"TIMESTAMP", "query", "FROM VALUES (TIMESTAMP '2026-08-22T09:00:00Z') AS a(value) SELECT value"},
		{"UNION", "query", "FROM VALUES (1) AS a(value) SELECT value UNION FROM VALUES (2) AS b(value) SELECT value"},
		{"VALUES", "query", "FROM VALUES (1) AS a(value) SELECT value"},
		{"WHERE", "query", "FROM VALUES (1) AS a(value) WHERE value = 1 SELECT value"},
		{"WITH", "query", "WITH a(value) AS (VALUES (1)) FROM a SELECT value"},
		{"TRUE", "query", "FROM VALUES (TRUE) AS a(value) WHERE value SELECT value"},
		{"FALSE", "query", "FROM VALUES (FALSE) AS a(value) WHERE NOT value SELECT value"},
		{"INSERT", "command", "INSERT INTO cache (key, value) VALUES ('k', 'v')"},
		{"INTO", "command", "INSERT INTO cache (key, value) VALUES ('k', 'v')"},
		{"COUNTER_INSERT_FIELD", "command", "INSERT INTO cache (key, counter) VALUES ('k', 1)"},
		{"TTL_SECONDS_INSERT_FIELD", "command", "INSERT INTO cache (key, value, ttl_seconds) VALUES ('k', 'v', 1)"},
		{"UNIX_SECONDS_INSERT_FIELD", "command", "INSERT INTO cache (key, value, unix_seconds) VALUES ('k', 'v', 1)"},
		{"UPDATE", "command", "UPDATE cache SET value = 'v' WHERE key = 'k'"},
		{"SET", "command", "UPDATE cache SET value = 'v' WHERE key = 'k'"},
		{"VALUE_UPDATE_FIELD", "command", "UPDATE cache SET value = 'v' WHERE key = 'k'"},
		{"TTL_SECONDS_UPDATE_FIELD", "command", "UPDATE cache SET ttl_seconds = 1 WHERE key = 'k'"},
		{"UNIX_SECONDS_UPDATE_FIELD", "command", "UPDATE cache SET unix_seconds = 1 WHERE key = 'k'"},
		{"DELETE", "command", "DELETE FROM cache WHERE key = 'k'"},
		{"CALL", "command", "CALL GET('k')"},
		{"VALUE_SELECTOR", "command", "SELECT value FROM cache WHERE key = 'k'"},
		{"EXISTS_SELECTOR", "command", "SELECT exists FROM cache WHERE key = 'k'"},
		{"TTL_SELECTOR", "command", "SELECT ttl FROM cache WHERE key = 'k'"},
		{"DUMP_SELECTOR", "command", "SELECT dump FROM cache WHERE key = 'k'"},
		{"KEY_NAMED_FIELD", "command", "CALL SETSTR(key => 'k', value => 'v')"},
		{"VALUE_NAMED_FIELD", "command", "CALL SETSTR(key => 'k', value => 'v')"},
		{"SUBKEY_NAMED_FIELD", "command", "CALL RT.PUT(key => 'k', subkey => 'field', value => 'v')"},
		{"VALUES_NAMED_FIELD", "command", "CALL ADDSET(key => 'k', values => JSON '[\"v\"]')"},
		{"PAIRS_NAMED_FIELD", "command", "CALL PUTMAP(key => 'k', pairs => JSON '{\"field\":\"v\"}')"},
		{"PRIORITY_NAMED_FIELD", "command", "CALL PUSHPQ(key => 'k', priority => 1, value => 'v')"},
		{"TTL_SECONDS_NAMED_FIELD", "command", "CALL SETSTRX(key => 'k', value => 'v', ttl_seconds => 1)"},
		{"UNIX_SECONDS_NAMED_FIELD", "command", "CALL EXPIREAT(key => 'k', unix_seconds => 1)"},
		{"JSON", "command", "CALL ADDSET(key => 'k', values => JSON '[\"v\"]')"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var err error
			switch test.mode {
			case "query":
				_, err = ExecuteSQLQuery(test.source, resolver)
			case "command":
				_, err = CompileSQL(test.source)
			default:
				t.Fatalf("unknown test mode %q", test.mode)
			}
			if err != nil {
				t.Fatalf("%s keyword source %q: %v", test.name, test.source, err)
			}
		})
	}
	for _, word := range []string{"CREATE", "FUNCTION", "LANGUAGE", "AS"} {
		t.Run(word, func(t *testing.T) {
			_, err := CompileSQLFunction("CREATE FUNCTION syntax_" + strings.ToLower(word) + "() LANGUAGE GO AS 'return true'")
			if err != nil {
				t.Fatalf("%s syntax keyword: %v", word, err)
			}
		})
	}
	for _, typeName := range []string{"ANY", "INTEGER", "NUMBER", "TEXT", "BOOLEAN"} {
		t.Run("TYPE_"+typeName, func(t *testing.T) {
			_, err := CompileSQLFunction("CREATE FUNCTION typed_" + strings.ToLower(typeName) + "(value " + typeName + ") LANGUAGE GO AS 'return value == value'")
			if err != nil {
				t.Fatalf("%s type: %v", typeName, err)
			}
		})
	}
	for _, language := range []struct {
		name, source string
	}{
		{"GO", "return true"},
		{"LUA", "return true"},
		{"WASM", "AGFzbQEAAAA="},
		{"JS", "return true;"},
	} {
		t.Run("LANGUAGE_"+language.name, func(t *testing.T) {
			_, err := CompileSQLFunction("CREATE FUNCTION language_" + strings.ToLower(language.name) + "() LANGUAGE " + language.name + " AS '" + language.source + "'")
			if err != nil {
				t.Fatalf("%s language: %v", language.name, err)
			}
		})
	}
}

func TestSQLKeywordInventoryTracksEveryDirectParserLiteral(t *testing.T) {
	t.Parallel()
	// This is the declared inventory behind TestSQLAcceptedKeywordInventory.
	// When a parser gains a new literal word, this test fails and requires both
	// a named execution case above and a SQL_TEST_MATRIX.md update.
	covered := map[string]struct{}{
		"ALL": {}, "ANALYZE": {}, "AND": {}, "AS": {}, "ASC": {}, "BETWEEN": {}, "BREADTH": {}, "BY": {}, "CACHE": {}, "CREATE": {}, "CROSS": {}, "CURRENT": {}, "CYCLE": {}, "DATE": {}, "DESC": {}, "DISTINCT": {}, "EXCEPT": {}, "EXPLAIN": {}, "FETCH": {}, "FIRST": {}, "FOLLOWING": {}, "FROM": {}, "FULL": {}, "FUNCTION": {}, "GROUP": {}, "HAVING": {}, "IN": {}, "INNER": {}, "INTERSECT": {}, "INTO": {}, "IS": {}, "JOIN": {}, "KEY": {}, "KEYS": {}, "LANGUAGE": {}, "LAST": {}, "LEFT": {}, "LIKE": {}, "LIMIT": {}, "NOT": {}, "NULL": {}, "NULLS": {}, "OFFSET": {}, "ON": {}, "ONLY": {}, "OR": {}, "ORDER": {}, "OUTER": {}, "OVER": {}, "PARTITION": {}, "PRECEDING": {}, "RECURSIVE": {}, "RIGHT": {}, "ROW": {}, "ROWS": {}, "SEARCH": {}, "SELECT": {}, "SET": {}, "TIMESTAMP": {}, "UNBOUNDED": {}, "UNION": {}, "VALUES": {}, "WHERE": {}, "WITH": {},
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}
	prefix := regexp.MustCompile(`(?:keyword|expectKeyword)\("([A-Za-z_]+)"\)`)
	for _, sourceName := range []string{"sql.go", "sql_query.go", "sql_function.go"} {
		source, err := os.ReadFile(filepath.Join(filepath.Dir(filename), sourceName))
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range prefix.FindAllSubmatch(source, -1) {
			word := strings.ToUpper(string(match[1]))
			if _, ok := covered[word]; !ok {
				t.Fatalf("parser accepts %q but it has no declared keyword-inventory coverage; add a named TestSQLAcceptedKeywordInventory case", word)
			}
		}
	}
}

func TestSQLKeywordInventoryReportsContextualDiagnostics(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, source, want string
	}{
		{"inner_requires_join", "FROM VALUES (1) AS a(value) INNER VALUES (1) AS b(value) SELECT a.value", "expected JOIN"},
		{"group_requires_by", "FROM VALUES (1) AS a(value) GROUP value SELECT value", "expected BY"},
		{"order_requires_by", "FROM VALUES (1) AS a(value) SELECT value ORDER value", "expected BY"},
		{"is_requires_null", "FROM VALUES (NULL) AS a(value) WHERE value IS TRUE SELECT value", "expected NULL"},
		{"intersect_all_is_rejected", "FROM VALUES (1) AS a(value) SELECT value INTERSECT ALL FROM VALUES (1) AS b(value) SELECT value", "INTERSECT ALL is not supported"},
		{"fetch_conflicts_with_limit", "FROM VALUES (1) AS a(value) SELECT value LIMIT 1 FETCH FIRST 1 ROWS ONLY", "FETCH cannot be combined with LIMIT"},
		{"self_reference_requires_recursive", "WITH sequence(value) AS (FROM sequence SELECT value) FROM sequence SELECT value", "requires WITH RECURSIVE"},
		{"recursive_requires_union_term", "WITH RECURSIVE sequence(value) AS (FROM sequence SELECT value) FROM sequence SELECT value", "requires exactly one UNION or UNION ALL recursive term"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExecuteSQLQuery(test.source, SQLSourceResolverFunc(nil))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ExecuteSQLQuery(%q) error = %v, want %q", test.source, err, test.want)
			}
		})
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
