package hatSql

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	// ErrSQLIncrementalDistinctUnsupported identifies a query shape that cannot
	// be lowered to exact signed DISTINCT maintenance.
	ErrSQLIncrementalDistinctUnsupported = errors.New("hatSql: incremental distinct SQL shape is unsupported")
	// ErrSQLIncrementalDistinctNil reports a method call on a nil operator.
	ErrSQLIncrementalDistinctNil = errors.New("hatSql: incremental distinct SQL operator is nil")
)

// SQLIncrementalDistinct adapts a restricted SELECT DISTINCT projection to
// exact signed source-row updates. It preserves one visible row for each
// projected value while retaining duplicate multiplicity internally. The
// operator is single-writer; callers provide synchronization when applying it
// concurrently.
type SQLIncrementalDistinct struct {
	projection *SQLIncrementalProjection
	distinct   *IncrementalDistinct
}

// CompileIncrementalDistinct lowers a compiled SELECT DISTINCT query to exact
// signed maintenance. The supported shape matches CompileIncrementalProjection
// plus DISTINCT: one CACHE or VALUES source, explicit scalar SELECT
// expressions, and an optional scalar WHERE without custom functions.
func (query *CompiledSQLQuery) CompileIncrementalDistinct() (*SQLIncrementalDistinct, error) {
	if query == nil || query.template == nil {
		return nil, fmt.Errorf("%w: compiled SQL query is required", ErrSQLIncrementalDistinctUnsupported)
	}
	if !query.template.distinct {
		return nil, fmt.Errorf("%w: query must use SELECT DISTINCT", ErrSQLIncrementalDistinctUnsupported)
	}
	if err := validateSQLIncrementalProjectionQueryWithDistinct(query, true); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSQLIncrementalDistinctUnsupported, err)
	}
	return &SQLIncrementalDistinct{
		projection: newSQLIncrementalProjection(query),
		distinct:   NewIncrementalDistinct(),
	}, nil
}

// Apply atomically evaluates the source projection and applies its projected
// rows to exact DISTINCT state. Duplicate projected values are suppressed
// until their final positive multiplicity is retracted. Input rows are never
// mutated or retained by the projection layer.
func (operator *SQLIncrementalDistinct) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if operator == nil {
		return nil, ErrSQLIncrementalDistinctNil
	}
	if len(updates) == 0 {
		return nil, nil
	}
	projected, err := operator.projection.Apply(updates)
	if err != nil {
		return nil, err
	}
	if len(projected) == 0 {
		return nil, nil
	}
	for index := range projected {
		key, err := canonicalSQLIncrementalDistinctRow(projected[index].Row)
		if err != nil {
			return nil, fmt.Errorf("incremental distinct SQL update %d: %w", index, err)
		}
		projected[index].Key = key
	}
	return operator.distinct.Apply(projected)
}

// Snapshot returns one positive row for each currently visible projected
// value, sorted by canonical row key for deterministic replay.
func (operator *SQLIncrementalDistinct) Snapshot() []DifferentialRow {
	if operator == nil || operator.distinct == nil {
		return nil
	}
	return operator.distinct.Snapshot()
}

func canonicalSQLIncrementalDistinctRow(row Row) (string, error) {
	typed := make(map[string]sqlIncrementalDistinctCanonicalValue, len(row))
	for column, value := range row {
		typeName := "null"
		if valueType := reflect.TypeOf(value); valueType != nil {
			typeName = valueType.String()
		}
		typed[column] = sqlIncrementalDistinctCanonicalValue{Type: typeName, Value: value}
	}
	encoded, err := json.Marshal(typed)
	if err != nil {
		return "", fmt.Errorf("encode projected row as canonical JSON: %w", err)
	}
	return string(encoded), nil
}

type sqlIncrementalDistinctCanonicalValue struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}
