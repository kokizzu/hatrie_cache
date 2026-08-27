package hatSql

import (
	"fmt"
	"regexp"
)

func evalSQLRegexPredicate(left, right interface{}, op string, token sqlToken) interface{} {
	if left == nil || right == nil {
		return nil
	}
	text, textOK := left.(string)
	pattern, patternOK := right.(string)
	if !textOK || !patternOK {
		return sqlEvalError{err: fmt.Errorf("REGEXP expects TEXT operands"), token: token}
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return sqlEvalError{err: fmt.Errorf("invalid regular expression: %w", err), token: token}
	}
	matched := compiled.MatchString(text)
	if op == "NOT REGEXP" {
		return !matched
	}
	return matched
}

func evalSQLRegexFunction(expr sqlExpr, group []sqlExecRow, row sqlExecRow) interface{} {
	if expr.name == "REGEXP_LIKE" && len(expr.args) != 2 {
		return sqlEvalError{err: fmt.Errorf("REGEXP_LIKE expects exactly two arguments"), token: expr.token}
	}
	if expr.name == "REGEXP_EXTRACT" && len(expr.args) != 2 && len(expr.args) != 3 {
		return sqlEvalError{err: fmt.Errorf("REGEXP_EXTRACT expects two or three arguments"), token: expr.token}
	}
	textValue := evalSQLExpr(expr.args[0], group, row)
	if err := sqlExpressionError(textValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	patternValue := evalSQLExpr(expr.args[1], group, row)
	if err := sqlExpressionError(patternValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	if textValue == nil || patternValue == nil {
		return nil
	}
	text, textOK := textValue.(string)
	pattern, patternOK := patternValue.(string)
	if !textOK || !patternOK {
		return sqlEvalError{err: fmt.Errorf("%s expects TEXT input and pattern", expr.name), token: expr.token}
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return sqlEvalError{err: fmt.Errorf("invalid regular expression: %w", err), token: expr.token}
	}
	if expr.name == "REGEXP_LIKE" {
		return compiled.MatchString(text)
	}
	capture := 0
	if len(expr.args) == 3 {
		captureValue := evalSQLExpr(expr.args[2], group, row)
		if err := sqlExpressionError(captureValue); err != nil {
			return sqlEvaluationFailure(err)
		}
		parsed, ok := sqlInteger(captureValue)
		if !ok || parsed < 0 {
			return sqlEvalError{err: fmt.Errorf("REGEXP_EXTRACT capture group must be a non-negative INTEGER"), token: expr.token}
		}
		capture = int(parsed)
	}
	match := compiled.FindStringSubmatch(text)
	if match == nil {
		return nil
	}
	if capture >= len(match) {
		return sqlEvalError{err: fmt.Errorf("REGEXP_EXTRACT capture group %d is unavailable", capture), token: expr.token}
	}
	return match[capture]
}
