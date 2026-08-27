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
