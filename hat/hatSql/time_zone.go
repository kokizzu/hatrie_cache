package hatSql

import (
	"fmt"
	"strings"
	"time"
)

func evalSQLTimeZoneExpr(expr sqlExpr, group []sqlExecRow, row sqlExecRow) interface{} {
	value := evalSQLExpr(*expr.left, group, row)
	if err := sqlExpressionError(value); err != nil {
		return sqlEvaluationFailure(err)
	}
	zoneValue := evalSQLExpr(*expr.right, group, row)
	if err := sqlExpressionError(zoneValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	location, err := sqlTimeZoneLocation(zoneValue)
	if err != nil {
		return sqlEvalError{err: err, token: expr.token}
	}
	timestamp, err := sqlTimestampValue(value, location)
	if err != nil {
		return sqlEvalError{err: err, token: expr.token}
	}
	return timestamp.In(location)
}

func evalSQLTimeFunction(expr sqlExpr, group []sqlExecRow, row sqlExecRow) interface{} {
	if expr.name == "VALID_AT" {
		return evalSQLValidAtFunction(expr, group, row)
	}
	arguments := make([]interface{}, len(expr.args))
	for index, argument := range expr.args {
		value := evalSQLExpr(argument, group, row)
		if err := sqlExpressionError(value); err != nil {
			return sqlEvaluationFailure(err)
		}
		arguments[index] = value
	}
	invalid := func(message string) interface{} {
		return sqlEvalError{err: fmt.Errorf("%s", message), token: expr.token}
	}
	switch expr.name {
	case "PARSE_TIMESTAMP":
		if len(arguments) != 2 {
			return invalid("PARSE_TIMESTAMP expects exactly two arguments")
		}
		location, err := sqlTimeZoneLocation(arguments[1])
		if err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		timestamp, err := sqlTimestampValue(arguments[0], location)
		if err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		return timestamp
	case "TIMESTAMP_ADD":
		if len(arguments) != 2 {
			return invalid("TIMESTAMP_ADD expects exactly two arguments")
		}
		timestamp, err := sqlTimestampValue(arguments[0], time.UTC)
		if err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		duration, err := sqlDurationValue(arguments[1])
		if err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		return timestamp.Add(duration)
	case "TIMESTAMP_DIFF":
		if len(arguments) != 2 {
			return invalid("TIMESTAMP_DIFF expects exactly two arguments")
		}
		left, err := sqlTimestampValue(arguments[0], time.UTC)
		if err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		right, err := sqlTimestampValue(arguments[1], time.UTC)
		if err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		return sqlDuration(left.Sub(right).String())
	}
	return invalid("unknown time function " + expr.name)
}

func evalSQLValidAtFunction(expr sqlExpr, group []sqlExecRow, row sqlExecRow) interface{} {
	invalid := func(message string) interface{} {
		return sqlEvalError{err: fmt.Errorf("%s", message), token: expr.token}
	}
	if len(expr.args) != 3 {
		return invalid("VALID_AT expects exactly three arguments")
	}
	atValue := evalSQLExpr(expr.args[0], group, row)
	if err := sqlExpressionError(atValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	if atValue == nil {
		return nil
	}
	fromValue := evalSQLExpr(expr.args[1], group, row)
	if err := sqlExpressionError(fromValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	toValue := evalSQLExpr(expr.args[2], group, row)
	if err := sqlExpressionError(toValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	return sqlEvaluateValidAtValues(atValue, fromValue, toValue, expr.token)
}

func evalSQLValidAtBatch(expr sqlExpr, rows []sqlExecRow, functions SQLFunctionResolver) ([]interface{}, error) {
	if len(expr.args) != 3 {
		return nil, fmt.Errorf("VALID_AT expects exactly three arguments")
	}
	if !sqlValidAtBatchSimpleArgument(expr.args[0]) || !sqlValidAtBatchSimpleArgument(expr.args[1]) || !sqlValidAtBatchSimpleArgument(expr.args[2]) {
		return evalSQLValidAtBatchGeneric(expr, rows, functions)
	}
	result := make([]interface{}, len(rows))
	if expr.args[0].kind == "literal" {
		if expr.args[0].value == nil {
			for index, row := range rows {
				fromValue := sqlValidAtBatchArgument(expr.args[1], row)
				toValue := sqlValidAtBatchArgument(expr.args[2], row)
				result[index] = sqlEvaluateValidAtValues(nil, fromValue, toValue, expr.token)
			}
			return result, nil
		}
		at, err := sqlTimestampValue(expr.args[0].value, time.UTC)
		if err != nil {
			failure := sqlEvalError{err: err, token: expr.token}
			for index := range result {
				result[index] = failure
			}
			return result, nil
		}
		for index, row := range rows {
			fromValue := sqlValidAtBatchArgument(expr.args[1], row)
			toValue := sqlValidAtBatchArgument(expr.args[2], row)
			result[index] = sqlEvaluateValidAtTime(at, fromValue, toValue, expr.token)
		}
		return result, nil
	}
	for index, row := range rows {
		atValue := sqlValidAtBatchArgument(expr.args[0], row)
		fromValue := sqlValidAtBatchArgument(expr.args[1], row)
		toValue := sqlValidAtBatchArgument(expr.args[2], row)
		result[index] = sqlEvaluateValidAtValues(atValue, fromValue, toValue, expr.token)
	}
	return result, nil
}

func evalSQLValidAtBatchGeneric(expr sqlExpr, rows []sqlExecRow, functions SQLFunctionResolver) ([]interface{}, error) {
	at, err := evalSQLExprBatch(expr.args[0], rows, functions)
	if err != nil {
		return nil, err
	}
	validFrom, err := evalSQLExprBatch(expr.args[1], rows, functions)
	if err != nil {
		return nil, err
	}
	validTo, err := evalSQLExprBatch(expr.args[2], rows, functions)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(rows))
	for index := range rows {
		result[index] = sqlEvaluateValidAtValues(at[index], validFrom[index], validTo[index], expr.token)
	}
	return result, nil
}

func sqlValidAtBatchSimpleArgument(expr sqlExpr) bool {
	return expr.kind == "literal" || expr.kind == "field"
}

func sqlValidAtBatchArgument(expr sqlExpr, row sqlExecRow) interface{} {
	if expr.kind == "literal" {
		return expr.value
	}
	return sqlField(row, expr.qualifier, expr.name)
}

func sqlEvaluateValidAtValues(atValue, fromValue, toValue interface{}, token sqlToken) interface{} {
	if err := sqlExpressionError(atValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	if err := sqlExpressionError(fromValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	if err := sqlExpressionError(toValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	if atValue == nil {
		return nil
	}
	at, err := sqlTimestampValue(atValue, time.UTC)
	if err != nil {
		return sqlEvalError{err: err, token: token}
	}
	return sqlEvaluateValidAtTime(at, fromValue, toValue, token)
}

func sqlEvaluateValidAtTime(at time.Time, fromValue, toValue interface{}, token sqlToken) interface{} {
	if fromValue != nil {
		validFrom, err := sqlTimestampValue(fromValue, time.UTC)
		if err != nil {
			return sqlEvalError{err: err, token: token}
		}
		if at.Before(validFrom) {
			return false
		}
	}
	if toValue != nil {
		validTo, err := sqlTimestampValue(toValue, time.UTC)
		if err != nil {
			return sqlEvalError{err: err, token: token}
		}
		if !at.Before(validTo) {
			return false
		}
	}
	return true
}

func sqlTimeZoneLocation(value interface{}) (*time.Location, error) {
	name, ok := value.(string)
	if !ok || strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("time zone must be a non-empty IANA time zone name")
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("unknown time zone %q", name)
	}
	return location, nil
}

func sqlTimestampValue(value interface{}, location *time.Location) (time.Time, error) {
	if timestamp, ok := value.(time.Time); ok {
		return timestamp, nil
	}
	text, ok := value.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("timestamp value must be TIMESTAMP or TEXT")
	}
	if timestamp, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return timestamp.In(location), nil
	}
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05", "2006-01-02"} {
		if timestamp, err := time.ParseInLocation(layout, text, location); err == nil {
			return timestamp, nil
		}
	}
	return time.Time{}, fmt.Errorf("timestamp must be RFC3339 or YYYY-MM-DD HH:MM:SS text")
}

func sqlDurationValue(value interface{}) (time.Duration, error) {
	switch duration := value.(type) {
	case sqlDuration:
		return time.ParseDuration(string(duration))
	case string:
		parsed, err := time.ParseDuration(duration)
		if err == nil {
			return parsed, nil
		}
	}
	return 0, fmt.Errorf("duration value must be DURATION or a valid duration TEXT")
}
