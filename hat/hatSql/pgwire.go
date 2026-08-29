package hatSql

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"hatrie_cache/hat/hatPgWire"
)

// PgWireQueryHandler adapts the Hatrie SQL engine to PostgreSQL's simple-query
// wire protocol. The resolver and options have the same behavior as direct SQL
// execution.
type PgWireQueryHandler struct {
	Resolver SQLSourceResolver
	Options  SQLQueryOptions
}

// NewPgWireQueryHandler creates a PostgreSQL-wire query handler backed by the
// Hatrie SQL engine.
func NewPgWireQueryHandler(resolver SQLSourceResolver, options SQLQueryOptions) PgWireQueryHandler {
	return PgWireQueryHandler{Resolver: resolver, Options: options}
}

// Query executes one SQL query and converts the result to PostgreSQL text rows.
func (handler PgWireQueryHandler) Query(ctx context.Context, query string) (hatPgWire.QueryResult, error) {
	result, err := ExecuteSQLQueryParameters(ctx, query, handler.Resolver, nil, handler.Options)
	if err != nil {
		return hatPgWire.QueryResult{}, err
	}
	fields := make([]hatPgWire.Field, len(result.Columns))
	for columnIndex, column := range result.Columns {
		fields[columnIndex] = hatPgWire.Field{Name: column, DataTypeOID: pgWireColumnType(result.Rows, column)}
	}
	rows := make([][]*string, len(result.Rows))
	for rowIndex, row := range result.Rows {
		values := make([]*string, len(result.Columns))
		for columnIndex, column := range result.Columns {
			if value, exists := row[column]; exists && value != nil {
				encoded := pgWireTextValue(value)
				values[columnIndex] = &encoded
			}
		}
		rows[rowIndex] = values
	}
	return hatPgWire.QueryResult{Fields: fields, Rows: rows}, nil
}

func pgWireColumnType(rows []Row, column string) uint32 {
	for _, row := range rows {
		if value, exists := row[column]; exists && value != nil {
			return pgWireValueType(value)
		}
	}
	return hatPgWire.OIDText
}

func pgWireValueType(value interface{}) uint32 {
	switch value.(type) {
	case bool:
		return hatPgWire.OIDBool
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return hatPgWire.OIDInt8
	case float32, float64:
		return hatPgWire.OIDFloat8
	case time.Time:
		return 1184 // timestamptz
	default:
		return hatPgWire.OIDText
	}
}

func pgWireTextValue(value interface{}) string {
	switch typed := value.(type) {
	case bool:
		if typed {
			return "t"
		}
		return "f"
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}
