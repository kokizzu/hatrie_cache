package hatSql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type sqlJSONPathSegment struct {
	key     string
	index   int
	isIndex bool
}

type sqlJSONPathProgram struct {
	source        string
	segments      []sqlJSONPathSegment
	err           error
	canonicalOnce sync.Once
	canonical     string
}

type sqlJSONMemberLookup interface {
	sqlJSONMember(key string) (interface{}, bool)
}

type sqlJSONMaterializer interface {
	sqlJSONMaterialize() interface{}
}

func compileSQLJSONPath(path string) *sqlJSONPathProgram {
	segments, err := parseSQLJSONPath(path)
	return &sqlJSONPathProgram{source: path, segments: segments, err: err}
}

func (program *sqlJSONPathProgram) canonicalPath() string {
	if program == nil || program.err != nil {
		return ""
	}
	program.canonicalOnce.Do(func() {
		program.canonical = formatSQLJSONPath(program.segments)
	})
	return program.canonical
}

func prepareSQLJSONPathExpr(expr *sqlExpr) {
	if expr == nil || expr.kind != "func" || len(expr.args) != 2 {
		return
	}
	switch expr.name {
	case "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS":
	default:
		return
	}
	path, ok := expr.args[1].value.(string)
	if expr.args[1].kind != "literal" || !ok {
		return
	}
	expr.jsonPath = compileSQLJSONPath(path)
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
		member, exists, ok := sqlJSONMember(current, segment.key)
		if !ok {
			return nil, false, nil
		}
		if !exists {
			return nil, false, nil
		}
		current = member
	}
	return current, true, nil
}

func sqlJSONMember(value interface{}, key string) (interface{}, bool, bool) {
	if lookup, ok := value.(sqlJSONMemberLookup); ok {
		member, exists := lookup.sqlJSONMember(key)
		return member, exists, true
	}
	object, ok := sqlJSONObject(value)
	if !ok {
		return nil, false, false
	}
	member, exists := object[key]
	return member, exists, true
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
	if value, present, found := sqlColumnarJSONSubcolumnValue(expr, row); found {
		if expr.name == "JSON_EXISTS" {
			return present
		}
		if !present {
			return nil
		}
		return value
	}
	input := evalSQLExpr(expr.args[0], group, row)
	if columnarInput, ok := sqlColumnarJSONFieldInput(expr.args[0], row); ok {
		input = columnarInput
	}
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
	var err error
	var segments []sqlJSONPathSegment
	if expr.jsonPath != nil && expr.jsonPath.source == path {
		segments = expr.jsonPath.segments
		err = expr.jsonPath.err
	} else {
		segments, err = parseSQLJSONPath(path)
	}
	if err != nil {
		return sqlEvalError{err: err, token: expr.token}
	}
	value, exists, err := sqlJSONPathValue(input, segments)
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
		return sqlJSONMaterialize(value)
	case "JSON_VALUE":
		if !exists {
			return nil
		}
		switch value.(type) {
		case map[string]interface{}, SQLRow, []interface{}, sqlJSONMemberLookup:
			return sqlEvalError{err: fmt.Errorf("JSON_VALUE requires a scalar path result; use JSON_QUERY"), token: expr.token}
		}
		return value
	default:
		return sqlEvalError{err: fmt.Errorf("unknown JSON path function %q", expr.name), token: expr.token}
	}
}

func sqlJSONMaterialize(value interface{}) interface{} {
	if materializer, ok := value.(sqlJSONMaterializer); ok {
		return materializer.sqlJSONMaterialize()
	}
	return value
}
