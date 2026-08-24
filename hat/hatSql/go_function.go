package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

// sqlGoFunction compiles a deliberately small, pure Go-like expression. It is
// not Go source execution: there are no imports, loops, allocations, calls, or
// access to the host process.
type sqlGoFunction struct {
	definition SQLFunctionDefinition
	program    sqlGoProgram
}

// sqlGoProgram is a compact post-order instruction stream inspired by typed
// interpreter IRs such as Go-Joker. It replaces recursive AST walking in the
// per-row hot path while retaining the deliberately bounded GO UDF language.
type sqlGoProgram struct {
	instructions []sqlGoInstruction
	stackSize    int
}

type sqlGoInstruction struct {
	op     string
	value  interface{}
	column int
}

// NewGoFunction compiles one deliberately bounded Go-like scalar UDF.
func NewGoFunction(definition FunctionDefinition) (FunctionRuntime, error) {
	expression, err := parseSQLGoFunctionExpression(definition)
	if err != nil {
		return nil, err
	}
	arguments := make(map[string]int, len(definition.Arguments))
	for index, name := range definition.Arguments {
		arguments[strings.ToUpper(name)] = index
	}
	if err := bindSQLGoFunctionFields(&expression, definition, arguments); err != nil {
		return nil, err
	}
	program, err := compileSQLGoProgram(expression, definition)
	if err != nil {
		return nil, err
	}
	return &sqlGoFunction{definition: definition, program: program}, nil
}

func parseSQLGoFunctionExpression(definition SQLFunctionDefinition) (sqlExpr, error) {
	expression := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(definition.Source), "return"))
	// Use Go's familiar boolean spelling while sharing the SQL expression parser.
	expression = strings.ReplaceAll(expression, "&&", " AND ")
	expression = strings.ReplaceAll(expression, "||", " OR ")
	expression = strings.ReplaceAll(expression, "==", "=")
	tokens, err := lexSQL(expression)
	if err != nil {
		return sqlExpr{}, sqlGoFunctionParseError(definition, err, len("return ")+1)
	}
	parser := sqlQueryParser{tokens: tokens}
	parsed, err := parser.parseCondition()
	if err != nil {
		return sqlExpr{}, sqlGoFunctionParseError(definition, err, len("return ")+1)
	}
	if parser.current().kind != sqlTokenEOF {
		return sqlExpr{}, &SQLFunctionError{Definition: definition, Message: "expected the end of the return expression", Line: 1, Column: parser.current().column}
	}
	return parsed, nil
}

func sqlGoFunctionParseError(definition SQLFunctionDefinition, err error, offset int) error {
	var diagnostic *SQLDiagnostic
	if errors.As(err, &diagnostic) {
		message := diagnostic.Message
		if diagnostic.Suggestion != "" {
			message += "; did you mean `" + diagnostic.Suggestion + "`?"
		}
		return &SQLFunctionError{Definition: definition, Message: message, Line: diagnostic.Line, Column: diagnostic.Column + offset - 1}
	}
	return &SQLFunctionError{Definition: definition, Message: err.Error(), Line: 1, Column: offset}
}

func bindSQLGoFunctionFields(expression *sqlExpr, definition SQLFunctionDefinition, arguments map[string]int) error {
	if expression == nil {
		return nil
	}
	if expression.kind == "field" {
		if expression.qualifier != "" {
			return sqlGoFunctionSourceError(definition, expression.name, "qualified field access is not allowed")
		}
		index, ok := arguments[strings.ToUpper(expression.name)]
		if !ok {
			return sqlGoFunctionSourceError(definition, expression.name, fmt.Sprintf("unknown argument %q", expression.name))
		}
		expression.value = index
		return nil
	}
	if expression.kind == "func" {
		return sqlGoFunctionSourceError(definition, expression.name, "function calls are not allowed in a GO UDF")
	}
	if err := bindSQLGoFunctionFields(expression.left, definition, arguments); err != nil {
		return err
	}
	return bindSQLGoFunctionFields(expression.right, definition, arguments)
}

func sqlGoFunctionSourceError(definition SQLFunctionDefinition, token, message string) error {
	return &SQLFunctionError{Definition: definition, Message: message, Line: 1, Column: strings.Index(definition.Source, token) + 1}
}

func (function *sqlGoFunction) Evaluate(calls []SQLFunctionCall) ([]interface{}, error) {
	values := make([]interface{}, len(calls))
	stack := make([]interface{}, 0, function.program.stackSize)
	for row, call := range calls {
		if len(call.Arguments) != len(function.definition.Arguments) {
			return nil, &SQLFunctionError{Definition: function.definition, Message: fmt.Sprintf("expects %d arguments, got %d", len(function.definition.Arguments), len(call.Arguments)), Line: 1, Column: 1}
		}
		for index, value := range call.Arguments {
			if err := sqlFunctionTypeError(function.definition, index, value); err != nil {
				return nil, err
			}
		}
		value, err := function.program.Evaluate(call.Arguments, function.definition, stack)
		if err != nil {
			return nil, err
		}
		values[row] = value
	}
	return values, nil
}

func compileSQLGoProgram(expression sqlExpr, definition SQLFunctionDefinition) (sqlGoProgram, error) {
	program := sqlGoProgram{instructions: make([]sqlGoInstruction, 0, 8)}
	depth, maxDepth, err := appendSQLGoProgram(&program, expression, definition, 0, 0)
	if err != nil {
		return sqlGoProgram{}, err
	}
	if depth != 1 {
		return sqlGoProgram{}, fmt.Errorf("GO UDF compiler produced invalid stack depth %d", depth)
	}
	program.stackSize = maxDepth
	return program, nil
}

func appendSQLGoProgram(program *sqlGoProgram, expression sqlExpr, definition SQLFunctionDefinition, depth, maxDepth int) (int, int, error) {
	column := strings.Index(definition.Source, expression.op) + 1
	switch expression.kind {
	case "literal":
		program.instructions = append(program.instructions, sqlGoInstruction{op: "literal", value: expression.value, column: column})
		depth++
	case "field":
		program.instructions = append(program.instructions, sqlGoInstruction{op: "argument", value: expression.value, column: strings.Index(definition.Source, expression.name) + 1})
		depth++
	case "unary":
		var err error
		depth, maxDepth, err = appendSQLGoProgram(program, *expression.left, definition, depth, maxDepth)
		if err != nil {
			return depth, maxDepth, err
		}
		program.instructions = append(program.instructions, sqlGoInstruction{op: "unary " + expression.op, column: column})
	case "binary":
		var err error
		depth, maxDepth, err = appendSQLGoProgram(program, *expression.left, definition, depth, maxDepth)
		if err != nil {
			return depth, maxDepth, err
		}
		if expression.op != "IS NULL" && expression.op != "IS NOT NULL" {
			depth, maxDepth, err = appendSQLGoProgram(program, *expression.right, definition, depth, maxDepth)
			if err != nil {
				return depth, maxDepth, err
			}
			depth--
		}
		program.instructions = append(program.instructions, sqlGoInstruction{op: "binary " + expression.op, column: column})
	default:
		return depth, maxDepth, fmt.Errorf("unsupported GO UDF expression %q", expression.kind)
	}
	if depth > maxDepth {
		maxDepth = depth
	}
	return depth, maxDepth, nil
}

func (program sqlGoProgram) Evaluate(arguments []interface{}, definition SQLFunctionDefinition, stack []interface{}) (interface{}, error) {
	stack = stack[:0]
	for _, instruction := range program.instructions {
		switch instruction.op {
		case "literal":
			stack = append(stack, instruction.value)
		case "argument":
			stack = append(stack, arguments[instruction.value.(int)])
		default:
			if strings.HasPrefix(instruction.op, "unary ") {
				if len(stack) == 0 {
					return nil, fmt.Errorf("GO UDF stack underflow")
				}
				value := stack[len(stack)-1]
				op := strings.TrimPrefix(instruction.op, "unary ")
				if op == "!" {
					stack[len(stack)-1] = !sqlTruthy(value)
					continue
				}
				if op == "-" {
					if _, ok := sqlNumber(value); !ok {
						return nil, sqlGoRuntimeErrorAt(definition, instruction.column, fmt.Sprintf("operator %q expects a numeric operand, got %s", op, sqlFunctionValueType(value)))
					}
					stack[len(stack)-1] = sqlArithmeticValue("*", int64(-1), value)
					continue
				}
				return nil, fmt.Errorf("unsupported GO UDF unary operator %q", op)
			}
			if !strings.HasPrefix(instruction.op, "binary ") {
				return nil, fmt.Errorf("unsupported GO UDF instruction %q", instruction.op)
			}
			if len(stack) == 0 {
				return nil, fmt.Errorf("GO UDF stack underflow")
			}
			op := strings.TrimPrefix(instruction.op, "binary ")
			if op == "IS NULL" {
				stack[len(stack)-1] = stack[len(stack)-1] == nil
				continue
			}
			if op == "IS NOT NULL" {
				stack[len(stack)-1] = stack[len(stack)-1] != nil
				continue
			}
			if len(stack) < 2 {
				return nil, fmt.Errorf("GO UDF stack underflow")
			}
			left, right := stack[len(stack)-2], stack[len(stack)-1]
			stack = stack[:len(stack)-2]
			var value interface{}
			var err error
			if strings.Contains("+-*/%", op) && len(op) == 1 {
				value, err = sqlGoArithmeticValueAt(op, left, right, definition, instruction.column)
			} else {
				value = sqlBinaryValue(op, left, right)
			}
			if err != nil {
				return nil, err
			}
			stack = append(stack, value)
		}
	}
	if len(stack) != 1 {
		return nil, fmt.Errorf("GO UDF program ended with stack depth %d", len(stack))
	}
	return stack[0], nil
}

func sqlGoArithmeticValueAt(op string, left, right interface{}, definition SQLFunctionDefinition, column int) (interface{}, error) {
	_, leftOK := sqlNumber(left)
	_, rightOK := sqlNumber(right)
	if !leftOK || !rightOK {
		return nil, sqlGoRuntimeErrorAt(definition, column, fmt.Sprintf("operator %q expects numeric operands, got %s and %s", op, sqlFunctionValueType(left), sqlFunctionValueType(right)))
	}
	if op == "/" || op == "%" {
		if number, _ := sqlNumber(right); number == 0 {
			return nil, sqlGoRuntimeErrorAt(definition, column, "division by zero")
		}
	}
	return sqlArithmeticValue(op, left, right), nil
}

func sqlGoRuntimeErrorAt(definition SQLFunctionDefinition, column int, message string) error {
	return &SQLFunctionError{Definition: definition, Message: message, Line: 1, Column: column}
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
