package hatriecache

import (
	"fmt"
	"strings"
	"sync"
)

// SQLFunctionDefinition is a named, scalar Go-like expression used by read-only
// SQL queries. Source must be one return expression; control flow is excluded
// so execution remains bounded by the query batch.
type SQLFunctionDefinition struct {
	Name          string   `json:"name"`
	Arguments     []string `json:"arguments"`
	ArgumentTypes []string `json:"argument_types"`
	Language      string   `json:"language"`
	Source        string   `json:"source"`
}

// SQLFunctionCall contains one invocation's positional values.
type SQLFunctionCall struct{ Arguments []interface{} }

// SQLFunctionError preserves the UDF source location for clear diagnostics.
type SQLFunctionError struct {
	Definition   SQLFunctionDefinition
	Message      string
	Line, Column int
}

func (err *SQLFunctionError) Error() string {
	return "SQL function " + err.Definition.Name + ": " + err.Message
}

func FormatSQLFunctionDiagnostic(definition SQLFunctionDefinition, err error) string {
	functionError, ok := err.(*SQLFunctionError)
	if !ok {
		return err.Error()
	}
	if functionError.Definition.Name != "" {
		definition = functionError.Definition
	}
	line, column := functionError.Line, functionError.Column
	if line < 1 {
		line = 1
	}
	if column < 1 {
		column = 1
	}
	sourceLines := strings.Split(definition.Source, "\n")
	if line > len(sourceLines) {
		line = 1
	}
	source := sourceLines[line-1]
	return fmt.Sprintf("error: %s\n --> function %s:%d:%d\n  |\n%d | %s\n  | %s^", functionError.Message, definition.Name, line, column, line, source, strings.Repeat(" ", column-1))
}

// SQLFunctionResolver supplies vectorized custom SQL function results.
type SQLFunctionResolver interface {
	EvaluateSQLFunction(name string, calls []SQLFunctionCall) ([]interface{}, error)
}

// SQLFunctionRegistry keeps compiled Go-like functions. It is safe for
// concurrent query execution.
type SQLFunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]*sqlGoFunction
}

func NewSQLFunctionRegistry() *SQLFunctionRegistry {
	return &SQLFunctionRegistry{functions: make(map[string]*sqlGoFunction)}
}

func (registry *SQLFunctionRegistry) Register(definition SQLFunctionDefinition) error {
	if registry == nil {
		return fmt.Errorf("SQL function registry is required")
	}
	if err := normalizeSQLFunctionDefinition(&definition); err != nil {
		return err
	}
	compiled, err := newSQLGoFunction(definition)
	if err != nil {
		return err
	}
	key := strings.ToUpper(definition.Name)
	registry.mu.Lock()
	previous := registry.functions[key]
	registry.functions[key] = compiled
	registry.mu.Unlock()
	_ = previous
	return nil
}

func (registry *SQLFunctionRegistry) EvaluateSQLFunction(name string, calls []SQLFunctionCall) ([]interface{}, error) {
	if registry == nil {
		return nil, fmt.Errorf("SQL function registry is required")
	}
	registry.mu.RLock()
	function := registry.functions[strings.ToUpper(strings.TrimSpace(name))]
	registry.mu.RUnlock()
	if function == nil {
		return nil, fmt.Errorf("unknown SQL function %q", name)
	}
	return function.Evaluate(calls)
}

func (registry *SQLFunctionRegistry) Close() {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	functions := registry.functions
	registry.functions = make(map[string]*sqlGoFunction)
	registry.mu.Unlock()
	_ = functions
}

// CompileSQLFunction parses CREATE FUNCTION NAME(args...) LANGUAGE GO AS
// 'return expression'. It does not register the function.
func CompileSQLFunction(source string) (SQLFunctionDefinition, error) {
	tokens, err := lexSQL(source)
	if err != nil {
		return SQLFunctionDefinition{}, err
	}
	parser := sqlParser{tokens: tokens}
	if err := parser.expectKeyword("CREATE"); err != nil {
		return SQLFunctionDefinition{}, err
	}
	if err := parser.expectKeyword("FUNCTION"); err != nil {
		return SQLFunctionDefinition{}, err
	}
	name, err := parser.expectIdentifier("a function name", nil)
	if err != nil {
		return SQLFunctionDefinition{}, err
	}
	arguments, types, err := parser.parseSQLFunctionArguments()
	if err != nil {
		return SQLFunctionDefinition{}, err
	}
	if err := parser.expectKeyword("LANGUAGE"); err != nil {
		return SQLFunctionDefinition{}, err
	}
	language, err := parser.expectIdentifier("GO", []string{"GO"})
	if err != nil {
		return SQLFunctionDefinition{}, err
	}
	if err := parser.expectKeyword("AS"); err != nil {
		return SQLFunctionDefinition{}, err
	}
	body := parser.current()
	if body.kind != sqlTokenString {
		return SQLFunctionDefinition{}, parser.expected(body, "a Go-like source string", nil)
	}
	parser.next()
	if parser.current().kind == sqlTokenSemicolon {
		parser.next()
	}
	if parser.current().kind != sqlTokenEOF {
		return SQLFunctionDefinition{}, parser.expected(parser.current(), "end of input", nil)
	}
	definition := SQLFunctionDefinition{Name: name.text, Language: language.text, Source: body.text, Arguments: arguments, ArgumentTypes: types}
	if err := normalizeSQLFunctionDefinition(&definition); err != nil {
		return SQLFunctionDefinition{}, err
	}
	return definition, nil
}

func (parser *sqlParser) parseSQLFunctionArguments() ([]string, []string, error) {
	if err := parser.expectKind(sqlTokenLeftParen, "("); err != nil {
		return nil, nil, err
	}
	var names, types []string
	if parser.current().kind == sqlTokenRightParen {
		parser.next()
		return names, types, nil
	}
	for {
		name, err := parser.expectIdentifier("a function argument name", nil)
		if err != nil {
			return nil, nil, err
		}
		typeName := "ANY"
		if parser.current().kind == sqlTokenIdentifier && !strings.EqualFold(parser.current().text, "LANGUAGE") {
			typeName = strings.ToUpper(parser.current().text)
			parser.next()
		}
		names = append(names, name.text)
		types = append(types, typeName)
		if parser.current().kind != sqlTokenComma {
			break
		}
		parser.next()
	}
	if err := parser.expectKind(sqlTokenRightParen, ")"); err != nil {
		return nil, nil, err
	}
	return names, types, nil
}

func normalizeSQLFunctionDefinition(definition *SQLFunctionDefinition) error {
	if definition == nil {
		return fmt.Errorf("SQL function definition is required")
	}
	if definition.Name == "" {
		return fmt.Errorf("SQL function name is required")
	}
	definition.Name = strings.TrimSpace(definition.Name)
	definition.Language = strings.ToUpper(strings.TrimSpace(definition.Language))
	if definition.Language != "GO" {
		return fmt.Errorf("SQL function %q language must be GO", definition.Name)
	}
	if len(definition.ArgumentTypes) == 0 {
		definition.ArgumentTypes = make([]string, len(definition.Arguments))
		for i := range definition.ArgumentTypes {
			definition.ArgumentTypes[i] = "ANY"
		}
	}
	if len(definition.ArgumentTypes) != len(definition.Arguments) {
		return fmt.Errorf("SQL function %q has %d arguments but %d argument types", definition.Name, len(definition.Arguments), len(definition.ArgumentTypes))
	}
	for i := range definition.ArgumentTypes {
		definition.ArgumentTypes[i] = strings.ToUpper(strings.TrimSpace(definition.ArgumentTypes[i]))
		switch definition.ArgumentTypes[i] {
		case "ANY", "INTEGER", "NUMBER", "TEXT", "BOOLEAN":
		default:
			return fmt.Errorf("SQL function argument %q has unsupported type %q", definition.Arguments[i], definition.ArgumentTypes[i])
		}
	}
	return validateSQLGoDefinition(*definition)
}

func validateSQLGoDefinition(definition SQLFunctionDefinition) error {
	if !isSQLIdentifierStart(definition.Name[0]) {
		return fmt.Errorf("invalid SQL function name %q", definition.Name)
	}
	seen := make(map[string]struct{}, len(definition.Arguments))
	for _, argument := range definition.Arguments {
		if argument == "" || !isSQLIdentifierStart(argument[0]) {
			return fmt.Errorf("invalid SQL function argument %q", argument)
		}
		key := strings.ToUpper(argument)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate SQL function argument %q", argument)
		}
		seen[key] = struct{}{}
	}
	source := strings.TrimSpace(definition.Source)
	if !strings.HasPrefix(source, "return ") || strings.ContainsAny(source, "\r\n;") {
		return fmt.Errorf("Go SQL function source must be one return expression")
	}
	return nil
}

func sqlFunctionTypeError(definition SQLFunctionDefinition, argumentIndex int, value interface{}) error {
	expected := definition.ArgumentTypes[argumentIndex]
	actual := sqlFunctionValueType(value)
	if value == nil || expected == "ANY" || expected == actual || expected == "NUMBER" && (actual == "INTEGER" || actual == "NUMBER") {
		return nil
	}
	column := strings.Index(definition.Source, definition.Arguments[argumentIndex]) + 1
	return &SQLFunctionError{Definition: definition, Message: fmt.Sprintf("argument %q expects %s, got %s", definition.Arguments[argumentIndex], expected, actual), Line: 1, Column: column}
}
func sqlFunctionValueType(value interface{}) string {
	switch value.(type) {
	case bool:
		return "BOOLEAN"
	case int, int64:
		return "INTEGER"
	case float32, float64:
		return "NUMBER"
	case string:
		return "TEXT"
	case nil:
		return "NULL"
	}
	return strings.ToUpper(fmt.Sprintf("%T", value))
}
