package hatCache

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"hatrie_cache/hat/hatSql"
)

type SQLFunctionDefinition = hatSql.FunctionDefinition
type SQLFunctionCall = hatSql.FunctionCall
type SQLFunctionError = hatSql.FunctionError
type SQLFunctionResolver = hatSql.FunctionResolver

func FormatSQLFunctionDiagnostic(definition SQLFunctionDefinition, err error) string {
	return hatSql.FormatFunctionDiagnostic(definition, err)
}

// sqlFunctionRuntime evaluates one UDF implementation. Every implementation
// receives a complete query batch so it may amortize host/runtime crossings.
type sqlFunctionRuntime interface {
	Evaluate([]SQLFunctionCall) ([]interface{}, error)
}

// SQLFunctionRegistry keeps compiled UDFs. It is safe for concurrent query
// execution.
type SQLFunctionRegistry struct {
	core    *hatSql.Registry
	options SQLFunctionRegistryOptions
}

// SQLFunctionRegistryOptions configures optional, sandboxed UDF runtimes.
// JavyPath is the absolute path to the Javy JavaScript-to-Wasm compiler. When
// blank, LANGUAGE JS searches for a `javy` executable on PATH at registration.
// JavaScript is compiled once when registered and is executed only inside
// Wazero afterwards.
type SQLFunctionRegistryOptions struct {
	// PersistencePath stores normalized function definitions. OpenSQLFunctionRegistry
	// reloads and recompiles this file so registered functions survive a restart.
	// An empty path keeps the registry in-memory only.
	PersistencePath    string
	JavyPath           string
	JSCompileTimeout   time.Duration
	JSExecutionTimeout time.Duration
}

func NewSQLFunctionRegistry() *SQLFunctionRegistry {
	return NewSQLFunctionRegistryWithOptions(SQLFunctionRegistryOptions{})
}

func NewSQLFunctionRegistryWithOptions(options SQLFunctionRegistryOptions) *SQLFunctionRegistry {
	if options.JSCompileTimeout <= 0 {
		options.JSCompileTimeout = 30 * time.Second
	}
	if options.JSExecutionTimeout <= 0 {
		options.JSExecutionTimeout = time.Second
	}
	registry := &SQLFunctionRegistry{options: options}
	registry.core = hatSql.NewRegistry(func(definition hatSql.FunctionDefinition) (hatSql.FunctionDefinition, hatSql.FunctionRuntime, error) {
		compiled, err := registry.compile(definition)
		if err != nil {
			return hatSql.FunctionDefinition{}, nil, err
		}
		return compiled.definition, compiled.runtime, nil
	})
	return registry
}

// OpenSQLFunctionRegistry creates a registry and restores any definitions from
// PersistencePath. Definitions are revalidated and recompiled before the
// registry is returned, so a broken persisted function prevents a server from
// silently starting without its expected SQL behavior.
func OpenSQLFunctionRegistry(options SQLFunctionRegistryOptions) (*SQLFunctionRegistry, error) {
	registry := NewSQLFunctionRegistryWithOptions(options)
	if err := registry.Load(); err != nil {
		registry.Close()
		return nil, err
	}
	return registry, nil
}

func (registry *SQLFunctionRegistry) Register(definition SQLFunctionDefinition) error {
	if registry == nil || registry.core == nil {
		return fmt.Errorf("SQL function registry is required")
	}
	return registry.core.Register(definition, registry.persistDefinitions)
}

type sqlCompiledFunction struct {
	definition SQLFunctionDefinition
	runtime    sqlFunctionRuntime
}

func (registry *SQLFunctionRegistry) compile(definition SQLFunctionDefinition) (sqlCompiledFunction, error) {
	if err := normalizeSQLFunctionDefinition(&definition); err != nil {
		return sqlCompiledFunction{}, err
	}
	var (
		compiled sqlFunctionRuntime
		err      error
	)
	switch definition.Language {
	case "GO":
		compiled, err = newSQLGoFunction(definition)
	case "LUA":
		compiled, err = newSQLLuaFunction(definition)
	case "WASM":
		compiled, err = newSQLWASMFunction(definition)
	case "JS":
		compiled, err = newSQLJSFunction(definition, registry.options)
	default:
		return sqlCompiledFunction{}, fmt.Errorf("SQL function %q language %q is not available", definition.Name, definition.Language)
	}
	if err != nil {
		return sqlCompiledFunction{}, err
	}
	return sqlCompiledFunction{definition: definition, runtime: compiled}, nil
}

// Load replaces this registry with every persisted definition. It is useful for
// controlled in-process reloads; OpenSQLFunctionRegistry calls it at startup.
func (registry *SQLFunctionRegistry) Load() error {
	if registry == nil {
		return fmt.Errorf("SQL function registry is required")
	}
	path := strings.TrimSpace(registry.options.PersistencePath)
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read SQL function definitions: %w", err)
	}
	var definitions []SQLFunctionDefinition
	if err := json.Unmarshal(data, &definitions); err != nil {
		return fmt.Errorf("parse SQL function definitions: %w", err)
	}
	return registry.core.Replace(definitions)
}

func (registry *SQLFunctionRegistry) persistDefinitions(definitions []SQLFunctionDefinition) error {
	path := strings.TrimSpace(registry.options.PersistencePath)
	if path == "" {
		return nil
	}
	if err := writeJSONFileAtomic(path, definitions); err != nil {
		return fmt.Errorf("persist SQL function definitions: %w", err)
	}
	return nil
}

func (registry *SQLFunctionRegistry) EvaluateSQLFunction(name string, calls []SQLFunctionCall) ([]interface{}, error) {
	if registry == nil || registry.core == nil {
		return nil, fmt.Errorf("SQL function registry is required")
	}
	return registry.core.EvaluateSQLFunction(name, calls)
}

func (registry *SQLFunctionRegistry) Close() {
	if registry == nil {
		return
	}
	registry.core.Close()
}

// CompileSQLFunction parses CREATE FUNCTION NAME(args...) LANGUAGE GO|LUA|WASM|JS AS
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
	language, err := parser.expectIdentifier("GO, LUA, WASM, or JS", []string{"GO", "LUA", "WASM", "JS"})
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
	if definition.Language != "GO" && definition.Language != "LUA" && definition.Language != "WASM" && definition.Language != "JS" {
		return fmt.Errorf("SQL function %q language must be GO, LUA, WASM, or JS", definition.Name)
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
	switch definition.Language {
	case "GO":
		return validateSQLGoDefinition(*definition)
	case "LUA":
		return validateSQLLuaDefinition(*definition)
	case "WASM":
		return validateSQLWASMDefinition(*definition)
	case "JS":
		return validateSQLJSDefinition(*definition)
	}
	return nil
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

func validateSQLLuaDefinition(definition SQLFunctionDefinition) error {
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
	if !strings.HasPrefix(source, "return ") || strings.ContainsAny(source, "\r\n") {
		return fmt.Errorf("Lua SQL function source must be one return expression")
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
