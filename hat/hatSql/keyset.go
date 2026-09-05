package hatSql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

type sqlKeysetCursor struct {
	Fingerprint string         `json:"f"`
	After       KeysetPosition `json:"a"`
	Returned    int            `json:"r"`
}

// ExecuteSQLQueryKeysetPage returns one page after an ordered source
// position. It is an explicit alternative to ExecuteSQLQueryPage: the
// resolver must implement KeysetOrderedStreamSourceResolver, and the existing
// offset cursor remains the default for all callers.
func ExecuteSQLQueryKeysetPage(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, pageSize int, cursor string) (result SQLQueryResult, err error) {
	observation := newSQLQueryObservation(options)
	var operatorSteps []SQLExplainStep
	result.QueryID = observation.id
	defer func() { observation.finish(result, err, operatorSteps, source, parameters) }()
	release := lockSQLSnapshot(resolver)
	defer release()
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > maxSQLPageSize {
		return result, fmt.Errorf("SQL page_size exceeds the maximum %d", maxSQLPageSize)
	}
	control, cancel, controlErr := newSQLExecutionControl(ctx, options)
	if controlErr != nil {
		return result, controlErr
	}
	defer cancel()
	query, parseErr := parseSQLQueryWithCache(source, parameters, options.PreparedCache, options.PreparedSchemaVersion)
	if parseErr != nil {
		return result, parseErr
	}
	if err = options.IndexHint.validate(); err != nil {
		return result, err
	}
	query.indexHint = options.IndexHint
	if query.explain {
		return result, fmt.Errorf("EXPLAIN does not support keyset pagination")
	}
	if query.offset != 0 {
		return result, fmt.Errorf("SQL keyset pagination cannot combine with OFFSET")
	}
	if !sqlKeysetQueryStreamable(query, resolver) {
		return result, fmt.Errorf("SQL keyset pagination requires one direct ordered CACHE source and keyset stream support")
	}
	fingerprint, fingerprintErr := sqlCursorFingerprint(source, parameters)
	if fingerprintErr != nil {
		return result, fingerprintErr
	}
	after := KeysetPosition{}
	returned := 0
	if cursor != "" {
		value, cursorErr := decodeSQLKeysetCursor(cursor)
		if cursorErr != nil {
			return result, cursorErr
		}
		if value.Fingerprint != fingerprint {
			return result, fmt.Errorf("SQL keyset cursor does not match this query and parameters")
		}
		after = value.After
		returned = value.Returned
	}
	fetch := pageSize + 1
	if query.limit >= 0 {
		remaining := query.limit - returned
		if remaining <= 0 {
			result.Columns = sqlColumns(query.selects)
			return result, nil
		}
		if remaining < fetch {
			fetch = remaining
		}
	}
	query.limit = fetch
	var metrics *sqlExecutionMetrics
	if observation.observer != nil || observation.recorder != nil || options.IndexHint.Mode != "" {
		metrics = &sqlExecutionMetrics{indexHint: options.IndexHint}
	}
	var positions []KeysetPosition
	result, positions, err = executeSQLKeysetPage(ctx, query, resolver, control, after, metrics)
	if metrics != nil {
		operatorSteps = metrics.steps
	}
	result.QueryID = observation.id
	if err != nil {
		return result, sqlRuntimeDiagnostic(err)
	}
	if len(result.Rows) > pageSize {
		result.Rows = result.Rows[:pageSize]
		result.HasMore = true
		next := sqlKeysetCursor{Fingerprint: fingerprint, After: positions[pageSize-1], Returned: returned + pageSize}
		result.NextCursor, err = encodeSQLKeysetCursor(next)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// ExecuteQueryKeysetPage is the non-SQL-prefixed facade for keyset paging.
func ExecuteQueryKeysetPage(ctx context.Context, source string, resolver SourceResolver, parameters []interface{}, options QueryOptions, pageSize int, cursor string) (QueryResult, error) {
	return ExecuteSQLQueryKeysetPage(ctx, source, resolver, parameters, options, pageSize, cursor)
}

func sqlKeysetQueryStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || query.from == nil || query.explain || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || query.where.window != nil || len(query.orderBy) != 1 {
		return false
	}
	order := query.orderBy[0]
	if order.expr.kind != "field" || order.expr.qualifier != query.from.alias || order.expr.name == "" {
		return false
	}
	if _, ok := resolver.(KeysetOrderedStreamSourceResolver); !ok {
		return false
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || item.expr.window != nil || sqlExprHasAggregate(item.expr) {
			return false
		}
	}
	return true
}

func executeSQLKeysetPage(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, after KeysetPosition, metrics *sqlExecutionMetrics) (SQLQueryResult, []KeysetPosition, error) {
	streaming, ok := resolver.(KeysetOrderedStreamSourceResolver)
	if !ok {
		return SQLQueryResult{}, nil, fmt.Errorf("SQL keyset pagination requires keyset ordered source support")
	}
	columns := sqlColumns(query.selects)
	result := SQLQueryResult{Columns: columns, Rows: make([]SQLRow, 0, query.limit)}
	positions := make([]KeysetPosition, 0, query.limit)
	if query.limit == 0 {
		return result, positions, nil
	}
	functions, _ := resolver.(SQLFunctionResolver)
	inputRows, resultBytes := 0, 0
	started := time.Now()
	emit := func(sourceRow SQLRow, position KeysetPosition) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		if query.limit >= 0 && len(result.Rows) >= query.limit {
			return errSQLStreamLimitReached
		}
		row := SQLRow{}
		for index, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return err
			}
			row[columns[index]] = value
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		result.Rows = append(result.Rows, row)
		positions = append(positions, position)
		if query.limit >= 0 && len(result.Rows) >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	}
	available, err := streaming.StreamSQLOrderedSourceAfter(ctx, query.from.kind, query.from.key, query.orderBy[0].expr.name, query.orderBy[0].desc, query.orderBy[0].nullsFirst, query.orderBy[0].nullsLast, after, emit)
	if err != nil && err != errSQLStreamLimitReached {
		return SQLQueryResult{}, nil, sqlRuntimeDiagnostic(err)
	}
	if !available {
		return SQLQueryResult{}, nil, fmt.Errorf("SQL keyset pagination ordered source is unavailable: %w", errSQLOrderedSourceUnavailable)
	}
	if metrics != nil {
		metrics.record("KEYSET INDEX STREAM", sqlExplainSource(*query.from)+" ORDER BY "+sqlExplainOrders(query.orderBy), inputRows, len(result.Rows), started)
	}
	return result, positions, nil
}

func encodeSQLKeysetCursor(cursor sqlKeysetCursor) (string, error) {
	value, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func decodeSQLKeysetCursor(value string) (sqlKeysetCursor, error) {
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return sqlKeysetCursor{}, fmt.Errorf("invalid SQL keyset cursor")
	}
	var cursor sqlKeysetCursor
	if json.Unmarshal(encoded, &cursor) != nil || cursor.Fingerprint == "" || cursor.Returned < 0 || cursor.Returned > maxSQLQueryRows || (cursor.Returned == 0 && cursor.After.Valid == false) {
		return sqlKeysetCursor{}, fmt.Errorf("invalid SQL keyset cursor")
	}
	if cursor.Returned > 0 && !cursor.After.Valid {
		return sqlKeysetCursor{}, fmt.Errorf("invalid SQL keyset cursor")
	}
	return cursor, nil
}
