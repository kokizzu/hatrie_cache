package hatSql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type sqlJSONPathSegment struct {
	key     string
	index   int
	isIndex bool
}

// NormalizeJSONPath validates a restricted SQL/JSON path and returns its canonical form.
func NormalizeJSONPath(path string) (string, error) {
	segments, err := parseSQLJSONPath(path)
	if err != nil {
		return "", err
	}
	return formatSQLJSONPath(segments), nil
}

// JSONPathValue resolves a SQL/JSON path from decoded JSON, an SQLRow, JSON text, or JSON bytes.
// The boolean reports whether the path exists, including an existing JSON null.
func JSONPathValue(value interface{}, path string) (interface{}, bool, error) {
	segments, err := parseSQLJSONPath(path)
	if err != nil {
		return nil, false, err
	}
	return sqlJSONPathValue(value, segments)
}

func sqlJSONPathWithField(field, path string) (string, bool) {
	if !sqlJSONPathIdentifier(field) {
		return "", false
	}
	segments, err := parseSQLJSONPath(path)
	if err != nil {
		return "", false
	}
	segments = append([]sqlJSONPathSegment{{key: field}}, segments...)
	return formatSQLJSONPath(segments), true
}

func parseSQLJSONPath(path string) ([]sqlJSONPathSegment, error) {
	if path == "" || path[0] != '$' {
		return nil, fmt.Errorf("JSON path must start with $")
	}
	segments := make([]sqlJSONPathSegment, 0, 2)
	for index := 1; index < len(path); {
		switch path[index] {
		case '.':
			index++
			start := index
			if index >= len(path) || !sqlJSONPathIdentifierStart(path[index]) {
				return nil, fmt.Errorf("JSON path member after . must be an identifier")
			}
			index++
			for index < len(path) && sqlJSONPathIdentifierPart(path[index]) {
				index++
			}
			segments = append(segments, sqlJSONPathSegment{key: path[start:index]})
		case '[':
			index++
			if index >= len(path) {
				return nil, fmt.Errorf("JSON path has an unterminated [")
			}
			if path[index] == '\'' || path[index] == '"' {
				quote := path[index]
				index++
				var builder strings.Builder
				closed := false
				for index < len(path) {
					character := path[index]
					index++
					if character == '\\' {
						if index >= len(path) {
							return nil, fmt.Errorf("JSON path has an unfinished escape")
						}
						builder.WriteByte(path[index])
						index++
						continue
					}
					if character == quote {
						closed = true
						break
					}
					builder.WriteByte(character)
				}
				if !closed || index >= len(path) || path[index] != ']' {
					return nil, fmt.Errorf("JSON path has an unterminated quoted member")
				}
				index++
				segments = append(segments, sqlJSONPathSegment{key: builder.String()})
				continue
			}
			start := index
			for index < len(path) && path[index] >= '0' && path[index] <= '9' {
				index++
			}
			if start == index || index >= len(path) || path[index] != ']' {
				return nil, fmt.Errorf("JSON path array indexes must be non-negative integers")
			}
			value, err := strconv.Atoi(path[start:index])
			if err != nil {
				return nil, fmt.Errorf("JSON path array index is too large")
			}
			index++
			segments = append(segments, sqlJSONPathSegment{index: value, isIndex: true})
		default:
			return nil, fmt.Errorf("JSON path has unexpected character %q", path[index])
		}
	}
	return segments, nil
}

func formatSQLJSONPath(segments []sqlJSONPathSegment) string {
	var builder strings.Builder
	builder.WriteByte('$')
	for _, segment := range segments {
		if segment.isIndex {
			builder.WriteByte('[')
			builder.WriteString(strconv.Itoa(segment.index))
			builder.WriteByte(']')
			continue
		}
		if sqlJSONPathIdentifier(segment.key) {
			builder.WriteByte('.')
			builder.WriteString(segment.key)
			continue
		}
		builder.WriteString("['")
		builder.WriteString(strings.ReplaceAll(strings.ReplaceAll(segment.key, "\\", "\\\\"), "'", "\\'"))
		builder.WriteString("']")
	}
	return builder.String()
}

func sqlJSONPathIdentifier(value string) bool {
	if value == "" || !sqlJSONPathIdentifierStart(value[0]) {
		return false
	}
	for index := 1; index < len(value); index++ {
		if !sqlJSONPathIdentifierPart(value[index]) {
			return false
		}
	}
	return true
}

func sqlJSONPathIdentifierStart(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character == '_'
}

func sqlJSONPathIdentifierPart(character byte) bool {
	return sqlJSONPathIdentifierStart(character) || character >= '0' && character <= '9'
}

func sqlJSONPathValue(value interface{}, segments []sqlJSONPathSegment) (interface{}, bool, error) {
	current, err := sqlJSONPathInput(value)
	if err != nil {
		return nil, false, err
	}
	for _, segment := range segments {
		if segment.isIndex {
			array, ok := current.([]interface{})
			if !ok || segment.index >= len(array) {
				return nil, false, nil
			}
			current = array[segment.index]
			continue
		}
		object, ok := sqlJSONObject(current)
		if !ok {
			return nil, false, nil
		}
		value, exists := object[segment.key]
		if !exists {
			return nil, false, nil
		}
		current = value
	}
	return current, true, nil
}

func sqlJSONPathInput(value interface{}) (interface{}, error) {
	switch value := value.(type) {
	case string:
		var decoded interface{}
		if err := json.Unmarshal([]byte(value), &decoded); err != nil {
			return nil, fmt.Errorf("JSON path input must be JSON: %w", err)
		}
		return decoded, nil
	case []byte:
		var decoded interface{}
		if err := json.Unmarshal(value, &decoded); err != nil {
			return nil, fmt.Errorf("JSON path input must be JSON: %w", err)
		}
		return decoded, nil
	case json.RawMessage:
		var decoded interface{}
		if err := json.Unmarshal(value, &decoded); err != nil {
			return nil, fmt.Errorf("JSON path input must be JSON: %w", err)
		}
		return decoded, nil
	default:
		return value, nil
	}
}

func sqlJSONObject(value interface{}) (map[string]interface{}, bool) {
	switch object := value.(type) {
	case map[string]interface{}:
		return object, true
	case SQLRow:
		return map[string]interface{}(object), true
	default:
		return nil, false
	}
}

func evalSQLJSONPathFunction(expr sqlExpr, group []sqlExecRow, row sqlExecRow) interface{} {
	if len(expr.args) != 2 {
		return sqlEvalError{err: fmt.Errorf("%s expects exactly two arguments", expr.name), token: expr.token}
	}
	input := evalSQLExpr(expr.args[0], group, row)
	if err := sqlExpressionError(input); err != nil {
		return sqlEvaluationFailure(err)
	}
	pathValue := evalSQLExpr(expr.args[1], group, row)
	if err := sqlExpressionError(pathValue); err != nil {
		return sqlEvaluationFailure(err)
	}
	path, ok := pathValue.(string)
	if !ok {
		return sqlEvalError{err: fmt.Errorf("%s expects a TEXT JSON path", expr.name), token: expr.token}
	}
	value, exists, err := JSONPathValue(input, path)
	if err != nil {
		return sqlEvalError{err: err, token: expr.token}
	}
	switch expr.name {
	case "JSON_EXISTS":
		return exists
	case "JSON_QUERY":
		if !exists {
			return nil
		}
		return value
	case "JSON_VALUE":
		if !exists {
			return nil
		}
		switch value.(type) {
		case map[string]interface{}, SQLRow, []interface{}:
			return sqlEvalError{err: fmt.Errorf("JSON_VALUE requires a scalar path result; use JSON_QUERY"), token: expr.token}
		}
		return value
	default:
		return sqlEvalError{err: fmt.Errorf("unknown JSON path function %q", expr.name), token: expr.token}
	}
}
