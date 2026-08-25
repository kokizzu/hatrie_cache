package hatSql

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// ParameterType declares the SQL value type accepted by a prepared-query
// parameter. ParameterAny accepts any SQL value. The remaining values match
// CACHE source field-type names.
type ParameterType string

const (
	ParameterAny       ParameterType = "ANY"
	ParameterText      ParameterType = "TEXT"
	ParameterNumber    ParameterType = "NUMBER"
	ParameterInteger   ParameterType = "INTEGER"
	ParameterDecimal   ParameterType = "DECIMAL"
	ParameterBoolean   ParameterType = "BOOLEAN"
	ParameterDate      ParameterType = "DATE"
	ParameterTimestamp ParameterType = "TIMESTAMP"
	ParameterUUID      ParameterType = "UUID"
	ParameterDuration  ParameterType = "DURATION"
	ParameterBinary    ParameterType = "BINARY"
	ParameterJSON      ParameterType = "JSON"
)

// ParameterSpec declares one positional prepared-query parameter. Parameters
// are ordered by $1, $2, and so on. Nullable defaults to false so callers must
// explicitly permit NULL where it has application-level meaning.
type ParameterSpec struct {
	Type     ParameterType
	Nullable bool
}

// SQLPreparedQuery binds a validated parameter schema to an immutable cached
// SQL template. It is safe for concurrent execution.
type SQLPreparedQuery struct {
	source     string
	parameters []ParameterSpec
	cache      *SQLPreparedQueryCache
}

// PreparedQuery is the package-native name for SQLPreparedQuery.
type PreparedQuery = SQLPreparedQuery

// PrepareSQLQuery parses source once, verifies its positional parameter schema,
// and retains the supplied cache for subsequent executions. A nil cache uses
// the bounded package-default cache.
func PrepareSQLQuery(source string, parameters []ParameterSpec, cache *SQLPreparedQueryCache) (*SQLPreparedQuery, error) {
	if cache == nil {
		cache = defaultSQLPreparedQueryCache
	}
	if _, err := cache.template(source); err != nil {
		return nil, err
	}
	parameterCount, err := sqlPreparedParameterCount(source)
	if err != nil {
		return nil, err
	}
	if parameterCount != len(parameters) {
		return nil, fmt.Errorf("SQL query requires %d declared parameters, got %d", parameterCount, len(parameters))
	}
	for index, parameter := range parameters {
		if !parameter.Type.valid() {
			return nil, fmt.Errorf("parameter $%d has unsupported type %q", index+1, parameter.Type)
		}
	}
	return &SQLPreparedQuery{
		source:     source,
		parameters: append([]ParameterSpec(nil), parameters...),
		cache:      cache,
	}, nil
}

// PrepareQuery prepares a read-only query with typed positional parameters.
func PrepareQuery(source string, parameters []ParameterSpec, cache *PreparedQueryCache) (*PreparedQuery, error) {
	return PrepareSQLQuery(source, parameters, cache)
}

// Source returns the immutable source text used to prepare query.
func (query *SQLPreparedQuery) Source() string {
	if query == nil {
		return ""
	}
	return query.source
}

// Parameters returns an independent copy of query's declared parameter schema.
func (query *SQLPreparedQuery) Parameters() []ParameterSpec {
	if query == nil {
		return nil
	}
	return append([]ParameterSpec(nil), query.parameters...)
}

// Execute validates and converts values, then evaluates the prepared query.
func (query *SQLPreparedQuery) Execute(ctx context.Context, resolver SQLSourceResolver, values []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	bound, err := query.bind(values)
	if err != nil {
		return SQLQueryResult{}, sqlClassifyError(err)
	}
	options.PreparedCache = query.cache
	return ExecuteSQLQueryParameters(ctx, query.source, resolver, bound, options)
}

// ExecuteRows validates and converts values, then streams a compatible prepared
// query to visit without materializing the result set.
func (query *SQLPreparedQuery) ExecuteRows(ctx context.Context, resolver SQLSourceResolver, values []interface{}, options SQLQueryOptions, visit func([]string, SQLRow) error) error {
	bound, err := query.bind(values)
	if err != nil {
		return sqlClassifyError(err)
	}
	options.PreparedCache = query.cache
	return ExecuteSQLQueryRows(ctx, query.source, resolver, bound, options, visit)
}

func (query *SQLPreparedQuery) bind(values []interface{}) ([]interface{}, error) {
	if query == nil {
		return nil, fmt.Errorf("prepared SQL query is required")
	}
	if len(values) != len(query.parameters) {
		return nil, fmt.Errorf("prepared SQL query requires %d parameters, got %d", len(query.parameters), len(values))
	}
	bound := make([]interface{}, len(values))
	for index, value := range values {
		converted, err := query.parameters[index].convert(value)
		if err != nil {
			return nil, fmt.Errorf("parameter $%d: %w", index+1, err)
		}
		bound[index] = converted
	}
	return bound, nil
}

func (typeName ParameterType) valid() bool {
	switch typeName {
	case ParameterAny, ParameterText, ParameterNumber, ParameterInteger, ParameterDecimal, ParameterBoolean, ParameterDate, ParameterTimestamp, ParameterUUID, ParameterDuration, ParameterBinary, ParameterJSON:
		return true
	default:
		return false
	}
}

func (parameter ParameterSpec) convert(value interface{}) (interface{}, error) {
	if value == nil {
		if parameter.Nullable {
			return nil, nil
		}
		return nil, fmt.Errorf("expects %s, got NULL", parameter.Type)
	}
	if parameter.Type == ParameterAny {
		return value, nil
	}
	if parameter.Type == ParameterTimestamp {
		if timestamp, ok := value.(time.Time); ok {
			return timestamp, nil
		}
	}
	if parameter.Type == ParameterDate {
		if date, ok := value.(sqlDate); ok {
			return date, nil
		}
	}
	if parameter.Type == ParameterDecimal {
		if decimal, ok := value.(sqlDecimal); ok {
			return decimal, nil
		}
	}
	if parameter.Type == ParameterUUID {
		if uuid, ok := value.(sqlUUID); ok {
			return uuid, nil
		}
	}
	if parameter.Type == ParameterDuration {
		if duration, ok := value.(sqlDuration); ok {
			return duration, nil
		}
	}
	if parameter.Type == ParameterBinary {
		if binary, ok := value.([]byte); ok {
			return append([]byte(nil), binary...), nil
		}
	}
	converted, ok := sqlTypedJSONFieldValue(value, string(parameter.Type))
	if !ok {
		return nil, fmt.Errorf("expects %s, got %s", parameter.Type, sqlLiteralTypeName(value))
	}
	return converted, nil
}

func sqlPreparedParameterCount(source string) (int, error) {
	tokens, err := Lex(source)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, token := range tokens {
		if token.Kind() != TokenParameter {
			continue
		}
		index, err := strconv.Atoi(token.Text())
		if err != nil {
			return 0, fmt.Errorf("invalid parameter %q", token.Text())
		}
		if index > count {
			count = index
		}
	}
	return count, nil
}
