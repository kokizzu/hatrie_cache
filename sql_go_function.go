package hatriecache

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
	expression sqlExpr
}

func newSQLGoFunction(definition SQLFunctionDefinition) (*sqlGoFunction, error) {
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
	return &sqlGoFunction{definition: definition, expression: expression}, nil
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
	for row, call := range calls {
		if len(call.Arguments) != len(function.definition.Arguments) {
			return nil, &SQLFunctionError{Definition: function.definition, Message: fmt.Sprintf("expects %d arguments, got %d", len(function.definition.Arguments), len(call.Arguments)), Line: 1, Column: 1}
		}
		for index, value := range call.Arguments {
			if err := sqlFunctionTypeError(function.definition, index, value); err != nil {
				return nil, err
			}
		}
		value, err := evalSQLGoFunctionExpr(function.expression, call.Arguments, function.definition)
		if err != nil {
			return nil, err
		}
		values[row] = value
	}
	return values, nil
}

func evalSQLGoFunctionExpr(expression sqlExpr, arguments []interface{}, definition SQLFunctionDefinition) (interface{}, error) {
	switch expression.kind {
	case "literal":
		return expression.value, nil
	case "field":
		return arguments[expression.value.(int)], nil
	case "unary":
		value, err := evalSQLGoFunctionExpr(*expression.left, arguments, definition)
		if err != nil {
			return nil, err
		}
		if expression.op == "!" {
			return !sqlTruthy(value), nil
		}
		if expression.op == "-" {
			if _, ok := sqlNumber(value); !ok {
				return nil, sqlGoRuntimeError(definition, expression.op, fmt.Sprintf("operator %q expects a numeric operand, got %s", expression.op, sqlFunctionValueType(value)))
			}
			return sqlArithmeticValue("*", int64(-1), value), nil
		}
		return nil, fmt.Errorf("unsupported GO UDF unary operator %q", expression.op)
	case "binary":
		left, err := evalSQLGoFunctionExpr(*expression.left, arguments, definition)
		if err != nil {
			return nil, err
		}
		if expression.op == "IS NULL" {
			return left == nil, nil
		}
		if expression.op == "IS NOT NULL" {
			return left != nil, nil
		}
		right, err := evalSQLGoFunctionExpr(*expression.right, arguments, definition)
		if err != nil {
			return nil, err
		}
		if strings.Contains("+-*/%", expression.op) && len(expression.op) == 1 {
			return sqlGoArithmeticValue(expression.op, left, right, definition)
		}
		return sqlBinaryValue(expression.op, left, right), nil
	}
	return nil, fmt.Errorf("unsupported GO UDF expression")
}

func sqlGoArithmeticValue(op string, left, right interface{}, definition SQLFunctionDefinition) (interface{}, error) {
	_, leftOK := sqlNumber(left)
	_, rightOK := sqlNumber(right)
	if !leftOK || !rightOK {
		return nil, sqlGoRuntimeError(definition, op, fmt.Sprintf("operator %q expects numeric operands, got %s and %s", op, sqlFunctionValueType(left), sqlFunctionValueType(right)))
	}
	if op == "/" || op == "%" {
		if number, _ := sqlNumber(right); number == 0 {
			return nil, sqlGoRuntimeError(definition, op, "division by zero")
		}
	}
	return sqlArithmeticValue(op, left, right), nil
}

func sqlGoRuntimeError(definition SQLFunctionDefinition, token, message string) error {
	return &SQLFunctionError{Definition: definition, Message: message, Line: 1, Column: strings.Index(definition.Source, token) + 1}
}
